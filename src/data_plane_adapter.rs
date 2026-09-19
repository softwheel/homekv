use crate::data_plane::{Mutation as WireMutation, Request, RequestBody, Status};
use crate::data_plane_runtime::{HandlerResponse, RequestHandler};
use crate::routing::{CommittedRouteResolver, RouteDecision};
use crate::storage::{shard_for_key, Mutation, ShardEngineError, ShardStore};
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

#[derive(Debug, Default, Clone, Copy)]
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
    route_resolver: Option<CommittedRouteResolver>,
}

impl ShardRequestHandler<LocalRouteHints> {
    pub fn local(store: ShardStore) -> Self {
        Self::new(store, Arc::new(LocalRouteHints))
    }
}

impl<R: RouteHintProvider> ShardRequestHandler<R> {
    pub fn new(store: ShardStore, route_hints: Arc<R>) -> Self {
        Self {
            store,
            route_hints,
            route_resolver: None,
        }
    }

    /// Attach a committed catalog resolver (M4-T3). When present, every
    /// request is resolved against the committed placement before dispatch;
    /// the resolver supersedes the advisory [`RouteHintProvider`].
    pub fn with_route_resolver(mut self, resolver: CommittedRouteResolver) -> Self {
        self.route_resolver = Some(resolver);
        self
    }

    async fn execute(&self, request: Request) -> HandlerResponse {
        // Recompute shard identity from every raw key before consulting any
        // routing state (REQ-M4-ROUTE-001).
        if let Err(status) = validate_request_shard(&request) {
            return HandlerResponse::new(status, Vec::new());
        }

        if let Some(resolver) = &self.route_resolver {
            match resolver.resolve(request.shard_id).await {
                RouteDecision::Local { .. } => {}
                RouteDecision::Redirect(redirect) => {
                    return HandlerResponse::new(Status::StaleRouteOrNotOwner, redirect.encode());
                }
                RouteDecision::Unavailable => {
                    return HandlerResponse::new(Status::ClosedOrUnavailable, Vec::new());
                }
            }
        } else if self.route_hints.disposition(request.shard_id)
            == RouteDisposition::AdvisoryNotOwner
        {
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

/// Recompute the shard identity from every raw key in the request and check
/// it against the claimed `shard_id` (REQ-M4-ROUTE-001, REQ-M4-MAP-001).
/// Runs before any routing state is consulted.
fn validate_request_shard(request: &Request) -> Result<(), Status> {
    let keys: Vec<&[u8]> = match &request.body {
        RequestBody::Get { key } | RequestBody::Set { key, .. } | RequestBody::Delete { key } => {
            vec![key.as_slice()]
        }
        RequestBody::Batch { mutations } => mutations
            .iter()
            .map(|mutation| match mutation {
                WireMutation::Set { key, .. } => key.as_slice(),
                WireMutation::Delete { key } => key.as_slice(),
            })
            .collect(),
    };
    for key in keys {
        if shard_for_key(key).as_u16() != request.shard_id {
            return Err(Status::WrongShard);
        }
    }
    Ok(())
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
    use crate::placement::{CatalogCommand, CatalogResponse, EligibleNode, PlacementCatalog};
    use crate::raft::RaftNodeId;
    use crate::routing::{CommittedRouteResolver, RouteRedirect};
    use crate::storage::{shard_for_key, ShardId, LOGICAL_SHARD_COUNT};
    use tokio::sync::RwLock;

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

    // ---- M4-T3 committed routing tests ----

    const ROUTE_CLUSTER_ID: [u8; 16] = *b"homekv-m4-t3rtes";

    fn route_node(node_id: RaftNodeId, endpoint: &str) -> EligibleNode {
        EligibleNode {
            node_id,
            raft_endpoint: endpoint.to_string(),
            failure_domain: "test".to_string(),
        }
    }

    fn route_catalog(nodes: Vec<EligibleNode>) -> PlacementCatalog {
        let mut catalog = PlacementCatalog::default();
        assert_eq!(
            catalog
                .apply(CatalogCommand::Bootstrap {
                    cluster_id: ROUTE_CLUSTER_ID,
                    eligible_nodes: nodes,
                })
                .unwrap(),
            CatalogResponse::Initialized { placement_epoch: 1 }
        );
        catalog
    }

    fn three_node_catalog() -> PlacementCatalog {
        route_catalog(vec![
            route_node(1, "n1"),
            route_node(2, "n2"),
            route_node(3, "n3"),
        ])
    }

    fn routed_handler(
        node_id: RaftNodeId,
        catalog: PlacementCatalog,
    ) -> (ShardRequestHandler, ShardStore) {
        let store = ShardStore::spawn(8);
        let resolver = CommittedRouteResolver::new(node_id, Arc::new(RwLock::new(catalog)));
        let handler = ShardRequestHandler::local(store.clone()).with_route_resolver(resolver);
        (handler, store)
    }

    #[tokio::test]
    async fn wrong_shard_is_rejected_before_routing() {
        // Even a committed voter never consults routing state for bad input.
        let (handler, store) = routed_handler(1, three_node_catalog());
        let key = b"route-wrong-shard".to_vec();
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
    async fn cross_shard_batch_is_rejected_before_routing() {
        let (handler, store) = routed_handler(1, three_node_catalog());
        let key_a = b"route-batch-a".to_vec();
        let shard = shard_for_key(&key_a).as_u16();
        let key_b = (0..100_000u32)
            .map(|i| format!("route-other-{i}").into_bytes())
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
    async fn non_member_receives_redirect_and_applies_nothing() {
        let (handler, store) = routed_handler(99, three_node_catalog());
        let key = b"route-redirect".to_vec();
        let shard = shard_for_key(&key).as_u16();

        for (id, body) in [
            (
                1,
                RequestBody::Set {
                    key: key.clone(),
                    value: b"nope".to_vec(),
                },
            ),
            (2, RequestBody::Get { key: key.clone() }),
            (3, RequestBody::Delete { key: key.clone() }),
        ] {
            let response = handler.handle(request(id, shard, body)).await;
            assert_eq!(response.status, Status::StaleRouteOrNotOwner);
            let redirect = RouteRedirect::decode(&response.body)
                .expect("redirect body must carry a decodable route hint");
            assert_eq!(redirect.route_epoch, 1);
            assert!(redirect.leader_hint.is_some());
            assert!(redirect.endpoint_hint.is_some());
        }
        // Nothing was applied or acknowledged locally.
        assert_eq!(store.get(&key).await.unwrap(), None);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn member_serves_locally_through_resolver() {
        let (handler, store) = routed_handler(1, three_node_catalog());
        let key = b"route-local".to_vec();
        let shard = shard_for_key(&key).as_u16();

        let set = handler
            .handle(request(
                1,
                shard,
                RequestBody::Set {
                    key: key.clone(),
                    value: b"v".to_vec(),
                },
            ))
            .await;
        assert_eq!(set.status, Status::Ok);

        let get = handler
            .handle(request(2, shard, RequestBody::Get { key: key.clone() }))
            .await;
        assert_eq!(get.status, Status::Ok);
        assert_eq!(get.body, b"v");
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn uninitialized_catalog_is_unavailable() {
        let (handler, store) = routed_handler(1, PlacementCatalog::default());
        let key = b"route-unready".to_vec();
        let shard = shard_for_key(&key).as_u16();

        let response = handler
            .handle(request(1, shard, RequestBody::Get { key }))
            .await;
        assert_eq!(response.status, Status::ClosedOrUnavailable);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn redirect_then_retry_on_member_preserves_idempotent_write() {
        let catalog = three_node_catalog();
        let (outsider, _) = routed_handler(99, catalog.clone());
        let (member, store) = routed_handler(1, catalog);
        let key = b"route-retry".to_vec();
        let shard = shard_for_key(&key).as_u16();
        let set = || RequestBody::Set {
            key: key.clone(),
            value: b"v".to_vec(),
        };

        // Stale route: the non-member redirects without applying.
        let redirected = outsider.handle(request(1, shard, set())).await;
        assert_eq!(redirected.status, Status::StaleRouteOrNotOwner);
        assert!(RouteRedirect::decode(&redirected.body).is_some());

        // Retry against the committed owner: the write lands exactly once
        // per attempt and repeats are idempotent.
        for id in 2..=3 {
            let retried = member.handle(request(id, shard, set())).await;
            assert_eq!(retried.status, Status::Ok);
        }
        let get = member
            .handle(request(4, shard, RequestBody::Get { key: key.clone() }))
            .await;
        assert_eq!(get.status, Status::Ok);
        assert_eq!(get.body, b"v");
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_multi_shard_history_during_route_refresh() {
        // Verification §6: concurrent GET/SET across shards while the
        // committed view flips. Every response is either served by a
        // committed voter or a clean redirect; acknowledged writes are
        // exactly the applied writes, in real-time order per key.
        //
        // Flips are driven by a shared op counter (not wall time) so the
        // route refresh is guaranteed to interleave with in-flight requests.
        use std::sync::atomic::{AtomicU64, Ordering};

        let catalog_b = route_catalog(vec![
            route_node(1, "n1"),
            route_node(2, "n2"),
            route_node(4, "n4"),
        ]);
        let shared = Arc::new(RwLock::new(three_node_catalog()));
        let store = ShardStore::spawn(16);
        let handler = ShardRequestHandler::local(store.clone())
            .with_route_resolver(CommittedRouteResolver::new(3, shared.clone()));
        let ops = Arc::new(AtomicU64::new(0));
        const TOTAL_OPS: u64 = 8 * 25;
        const FLIP_EVERY: u64 = 10;

        // Flap node 3 between member (view A) and non-member (view B).
        let flipper = {
            let shared = shared.clone();
            let ops = ops.clone();
            tokio::spawn(async move {
                let mut flipped = 0u64;
                while ops.load(Ordering::SeqCst) < TOTAL_OPS {
                    let epoch = ops.load(Ordering::SeqCst) / FLIP_EVERY;
                    if epoch != flipped {
                        flipped = epoch;
                        *shared.write().await = if flipped % 2 == 1 {
                            catalog_b.clone()
                        } else {
                            three_node_catalog()
                        };
                    }
                    tokio::time::sleep(std::time::Duration::from_micros(50)).await;
                }
            })
        };

        let workers: Vec<_> = (0..8u32)
            .map(|task| {
                let handler = handler.clone();
                let ops = ops.clone();
                tokio::spawn(async move {
                    let mut applied = Vec::new();
                    let mut rejected = Vec::new();
                    for i in 0..25u32 {
                        let key = format!("hist-{task}-{i}").into_bytes();
                        let value = format!("v-{task}-{i}").into_bytes();
                        let shard = shard_for_key(&key).as_u16();
                        ops.fetch_add(1, Ordering::SeqCst);
                        let set = handler
                            .handle(request(
                                (task as u64) * 1000 + i as u64,
                                shard,
                                RequestBody::Set {
                                    key: key.clone(),
                                    value: value.clone(),
                                },
                            ))
                            .await;
                        match set.status {
                            Status::Ok => {
                                // Read-your-write on the same handler/store.
                                let get = handler
                                    .handle(request(
                                        (task as u64) * 1000 + 500 + i as u64,
                                        shard,
                                        RequestBody::Get { key: key.clone() },
                                    ))
                                    .await;
                                assert!(
                                    get.status == Status::Ok && get.body == value
                                        || get.status == Status::StaleRouteOrNotOwner,
                                    "unexpected GET after acknowledged SET: {:?}",
                                    get.status
                                );
                                applied.push((key, value));
                            }
                            Status::StaleRouteOrNotOwner => {
                                assert!(RouteRedirect::decode(&set.body).is_some());
                                rejected.push(key);
                            }
                            other => panic!("unexpected SET status: {other:?}"),
                        }
                    }
                    (applied, rejected)
                })
            })
            .collect();

        let mut applied_all = Vec::new();
        let mut rejected_all = Vec::new();
        for worker in workers {
            let (applied, rejected) = worker.await.unwrap();
            applied_all.extend(applied);
            rejected_all.extend(rejected);
        }
        flipper.await.unwrap();

        // Settle on view A (node 3 is a member) and check the final state:
        // every acknowledged write is present exactly, every rejected write
        // was never applied.
        *shared.write().await = three_node_catalog();
        for (key, value) in &applied_all {
            assert_eq!(
                store.get(key).await.unwrap().as_deref(),
                Some(value.as_slice()),
                "acknowledged write missing for key {key:?}"
            );
        }
        for key in &rejected_all {
            assert_eq!(
                store.get(key).await.unwrap(),
                None,
                "rejected write was applied for key {key:?}"
            );
        }
        assert!(!applied_all.is_empty(), "expected some writes to land");
        assert!(
            !rejected_all.is_empty(),
            "expected some redirects during flaps"
        );
        store.shutdown().await.unwrap();
    }
}
