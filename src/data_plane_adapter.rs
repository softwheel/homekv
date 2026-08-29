use crate::data_plane::{Mutation as WireMutation, Request, RequestBody, Status};
use crate::data_plane_runtime::{HandlerResponse, RequestHandler};
use crate::storage::{Mutation, ShardEngineError, ShardStore};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteDisposition {
    Local,
    AdvisoryNotOwner,
}

pub trait RouteHintProvider: Send + Sync + 'static {
    fn disposition(&self, shard_id: u16) -> RouteDisposition;
}

#[derive(Debug, Default)]
pub struct LocalRouteHints;

impl RouteHintProvider for LocalRouteHints {
    fn disposition(&self, _shard_id: u16) -> RouteDisposition {
        RouteDisposition::Local
    }
}

#[derive(Clone)]
pub struct ShardRequestHandler<R = LocalRouteHints> {
    store: ShardStore,
    route_hints: Arc<R>,
}

impl ShardRequestHandler<LocalRouteHints> {
    pub fn local(store: ShardStore) -> Self {
        Self::new(store, Arc::new(LocalRouteHints))
    }
}

impl<R: RouteHintProvider> ShardRequestHandler<R> {
    pub fn new(store: ShardStore, route_hints: Arc<R>) -> Self {
        Self { store, route_hints }
    }

    async fn execute(&self, request: Request) -> HandlerResponse {
        if self.route_hints.disposition(request.shard_id) == RouteDisposition::AdvisoryNotOwner {
            return HandlerResponse::new(Status::StaleRouteOrNotOwner, Vec::new());
        }

        let result = match request.body {
            RequestBody::Get { key } => match self.store.try_get_on_shard(request.shard_id, &key).await {
                Ok(Some(value)) => return HandlerResponse::ok(value),
                Ok(None) => return HandlerResponse::new(Status::NotFound, Vec::new()),
                Err(error) => Err(error),
            },
            RequestBody::Set { key, value } => {
                self.store.try_put_on_shard(request.shard_id, key, value).await
            }
            RequestBody::Delete { key } => {
                self.store.try_delete_on_shard(request.shard_id, key).await
            }
            RequestBody::Batch { mutations } => {
                let mutations = mutations
                    .into_iter()
                    .map(|mutation| match mutation {
                        WireMutation::Set { key, value } => Mutation::Put { key, value },
                        WireMutation::Delete { key } => Mutation::Delete { key },
                    })
                    .collect();
                self.store
                    .try_apply_batch_on_shard(request.shard_id, mutations)
                    .await
            }
        };

        match result {
            Ok(()) => HandlerResponse::ok(Vec::new()),
            Err(error) => HandlerResponse::new(status_for_engine_error(&error), Vec::new()),
        }
    }
}

#[async_trait]
impl<R: RouteHintProvider> RequestHandler for ShardRequestHandler<R> {
    async fn handle(&self, request: Request) -> HandlerResponse {
        self.execute(request).await
    }
}

pub fn status_for_engine_error(error: &ShardEngineError) -> Status {
    match error {
        ShardEngineError::InvalidShard(_)
        | ShardEngineError::WrongShard { .. }
        | ShardEngineError::CrossShardBatch { .. } => Status::WrongShard,
        ShardEngineError::QueueFull => Status::Overloaded,
        ShardEngineError::Closed => Status::ClosedOrUnavailable,
        ShardEngineError::EmptyBatch => Status::MalformedRequest,
        ShardEngineError::OwnerStopped => Status::InternalError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_plane::{Mutation as WireMutation, RequestBody};
    use crate::storage::{shard_for_key, ShardId, LOGICAL_SHARD_COUNT};

    fn request(id: u64, shard_id: u16, body: RequestBody) -> Request {
        Request {
            request_id: id,
            shard_id,
            body,
        }
    }

    fn same_shard_keys() -> (Vec<u8>, Vec<u8>) {
        let first = b"compact-batch-a".to_vec();
        let target = shard_for_key(&first);
        for i in 0..100_000u32 {
            let candidate = format!("compact-batch-{i}").into_bytes();
            if candidate != first && shard_for_key(&candidate) == target {
                return (first, candidate);
            }
        }
        panic!("failed to find same-shard key");
    }

    #[tokio::test]
    async fn get_set_delete_follow_m1_semantics() {
        let store = ShardStore::spawn(8);
        let handler = ShardRequestHandler::local(store.clone());
        let key = b"compact-key".to_vec();
        let shard = shard_for_key(&key).as_u16();

        let missing = handler
            .handle(request(1, shard, RequestBody::Get { key: key.clone() }))
            .await;
        assert_eq!(missing.status, Status::NotFound);

        let set = handler
            .handle(request(
                2,
                shard,
                RequestBody::Set {
                    key: key.clone(),
                    value: b"value".to_vec(),
                },
            ))
            .await;
        assert_eq!(set.status, Status::Ok);

        let get = handler
            .handle(request(3, shard, RequestBody::Get { key: key.clone() }))
            .await;
        assert_eq!(get.status, Status::Ok);
        assert_eq!(get.body, b"value");

        let delete = handler
            .handle(request(4, shard, RequestBody::Delete { key: key.clone() }))
            .await;
        assert_eq!(delete.status, Status::Ok);
        assert_eq!(store.get(&key).await.unwrap(), None);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wrong_claimed_shard_is_rejected_before_mutation() {
        let store = ShardStore::spawn(8);
        let handler = ShardRequestHandler::local(store.clone());
        let key = b"wrong-route".to_vec();
        let actual = shard_for_key(&key).as_u16();
        let wrong = (actual + 1) % LOGICAL_SHARD_COUNT;

        let response = handler
            .handle(request(
                1,
                wrong,
                RequestBody::Set {
                    key: key.clone(),
                    value: b"nope".to_vec(),
                },
            ))
            .await;
        assert_eq!(response.status, Status::WrongShard);
        assert_eq!(store.get(&key).await.unwrap(), None);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_shard_batch_is_rejected_before_application() {
        let store = ShardStore::spawn(8);
        let handler = ShardRequestHandler::local(store.clone());
        let key_a = b"batch-a".to_vec();
        let shard = shard_for_key(&key_a).as_u16();
        let key_b = (0..100_000u32)
            .map(|i| format!("other-{i}").into_bytes())
            .find(|key| shard_for_key(key).as_u16() != shard)
            .unwrap();

        let response = handler
            .handle(request(
                1,
                shard,
                RequestBody::Batch {
                    mutations: vec![
                        WireMutation::Set {
                            key: key_a.clone(),
                            value: b"a".to_vec(),
                        },
                        WireMutation::Set {
                            key: key_b.clone(),
                            value: b"b".to_vec(),
                        },
                    ],
                },
            ))
            .await;
        assert_eq!(response.status, Status::WrongShard);
        assert_eq!(store.get(&key_a).await.unwrap(), None);
        assert_eq!(store.get(&key_b).await.unwrap(), None);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn same_shard_batch_applies_atomically_via_m1_primitive() {
        let store = ShardStore::spawn(8);
        let handler = ShardRequestHandler::local(store.clone());
        let (key_a, key_b) = same_shard_keys();
        let shard = shard_for_key(&key_a).as_u16();

        let response = handler
            .handle(request(
                1,
                shard,
                RequestBody::Batch {
                    mutations: vec![
                        WireMutation::Set {
                            key: key_a.clone(),
                            value: b"a".to_vec(),
                        },
                        WireMutation::Set {
                            key: key_b.clone(),
                            value: b"b".to_vec(),
                        },
                    ],
                },
            ))
            .await;
        assert_eq!(response.status, Status::Ok);
        assert_eq!(store.get(&key_a).await.unwrap(), Some(b"a".to_vec()));
        assert_eq!(store.get(&key_b).await.unwrap(), Some(b"b".to_vec()));
        store.shutdown().await.unwrap();
    }

    #[derive(Default)]
    struct NotOwner;

    impl RouteHintProvider for NotOwner {
        fn disposition(&self, _shard_id: u16) -> RouteDisposition {
            RouteDisposition::AdvisoryNotOwner
        }
    }

    #[tokio::test]
    async fn advisory_not_owner_maps_without_claiming_consensus_authority() {
        let store = ShardStore::spawn(8);
        let handler = ShardRequestHandler::new(store.clone(), Arc::new(NotOwner));
        let key = b"hint-only".to_vec();
        let shard = shard_for_key(&key).as_u16();
        let response = handler
            .handle(request(1, shard, RequestBody::Get { key }))
            .await;
        assert_eq!(response.status, Status::StaleRouteOrNotOwner);
        store.shutdown().await.unwrap();
    }

    #[test]
    fn engine_errors_have_stable_status_translation() {
        let shard0 = ShardId::new(0).unwrap();
        let shard1 = ShardId::new(1).unwrap();
        assert_eq!(status_for_engine_error(&ShardEngineError::QueueFull), Status::Overloaded);
        assert_eq!(status_for_engine_error(&ShardEngineError::Closed), Status::ClosedOrUnavailable);
        assert_eq!(status_for_engine_error(&ShardEngineError::OwnerStopped), Status::InternalError);
        assert_eq!(
            status_for_engine_error(&ShardEngineError::WrongShard {
                expected: shard0,
                actual: shard1,
            }),
            Status::WrongShard
        );
        assert_eq!(status_for_engine_error(&ShardEngineError::EmptyBatch), Status::MalformedRequest);
    }
}
