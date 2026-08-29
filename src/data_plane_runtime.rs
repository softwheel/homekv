use crate::data_plane::{
    decode_prefix, decode_request, encode_response, CodecError, CodecLimits, FrameKind, Response,
    Status, FRAME_PREFIX_LEN,
};
use async_trait::async_trait;
use std::collections::HashSet;
use std::fmt;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Semaphore};

pub const DEFAULT_MAX_IN_FLIGHT: usize = 256;
pub const DEFAULT_RESPONSE_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub max_in_flight: usize,
    pub response_queue_capacity: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            response_queue_capacity: DEFAULT_RESPONSE_QUEUE_CAPACITY,
        }
    }
}

impl RuntimeLimits {
    pub fn validate(self) -> Result<Self, RuntimeError> {
        if self.max_in_flight == 0
            || self.response_queue_capacity == 0
            || self.response_queue_capacity > self.max_in_flight
        {
            return Err(RuntimeError::InvalidLimits {
                max_in_flight: self.max_in_flight,
                response_queue_capacity: self.response_queue_capacity,
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerResponse {
    pub status: Status,
    pub body: Vec<u8>,
}

impl HandlerResponse {
    pub fn new(status: Status, body: Vec<u8>) -> Self {
        Self { status, body }
    }

    pub fn ok(body: Vec<u8>) -> Self {
        Self::new(Status::Ok, body)
    }
}

#[async_trait]
pub trait RequestHandler: Send + Sync + 'static {
    async fn handle(&self, request: crate::data_plane::Request) -> HandlerResponse;
}

#[derive(Debug)]
pub enum RuntimeError {
    Io(io::Error),
    Codec(CodecError),
    InvalidLimits {
        max_in_flight: usize,
        response_queue_capacity: usize,
    },
    UnexpectedFrameKind(FrameKind),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Codec(error) => write!(f, "codec error: {error}"),
            Self::InvalidLimits {
                max_in_flight,
                response_queue_capacity,
            } => write!(
                f,
                "invalid runtime limits: max_in_flight={max_in_flight}, response_queue_capacity={response_queue_capacity}"
            ),
            Self::UnexpectedFrameKind(kind) => {
                write!(f, "unexpected compact frame kind: {kind:?}")
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<io::Error> for RuntimeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for RuntimeError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

pub async fn serve_listener<H: RequestHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
) -> Result<(), RuntimeError> {
    let runtime_limits = runtime_limits.validate()?;
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let _ = serve_connection(stream, handler, codec_limits, runtime_limits).await;
        });
    }
}

pub async fn serve_connection<H: RequestHandler>(
    stream: TcpStream,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
) -> Result<(), RuntimeError> {
    serve_stream(stream, handler, codec_limits, runtime_limits).await
}

pub async fn serve_stream<S, H>(
    stream: S,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: RequestHandler,
{
    let runtime_limits = runtime_limits.validate()?;
    let semaphore = Arc::new(Semaphore::new(runtime_limits.max_in_flight));
    let active_request_ids = Arc::new(Mutex::new(HashSet::<u64>::with_capacity(
        runtime_limits.max_in_flight,
    )));
    let (response_tx, response_rx) =
        mpsc::channel::<Vec<u8>>(runtime_limits.response_queue_capacity);
    let (mut reader, writer) = tokio::io::split(stream);
    let writer_task = tokio::spawn(write_responses(writer, response_rx));

    let read_result = loop {
        // Acquire before advancing application reads. At saturation, no additional
        // request frame is staged in userspace beyond the kernel/transport buffer.
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break Ok(()),
        };

        let frame = match read_request_frame(&mut reader, codec_limits).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                drop(permit);
                break Ok(());
            }
            Err(error) => {
                drop(permit);
                break Err(error);
            }
        };

        let request = match decode_request(&frame, codec_limits) {
            Ok(request) => request,
            Err(error) => {
                drop(permit);
                if let Some(request_id) = correlated_request_id(&frame) {
                    let response = Response {
                        request_id,
                        status: Status::MalformedRequest,
                        body: Vec::new(),
                    };
                    match encode_response(&response, codec_limits) {
                        Ok(encoded) => {
                            if response_tx.send(encoded).await.is_err() {
                                break Ok(());
                            }
                            continue;
                        }
                        Err(encode_error) => break Err(RuntimeError::Codec(encode_error)),
                    }
                }
                break Err(RuntimeError::Codec(error));
            }
        };

        let request_id = request.request_id;
        {
            let mut active = active_request_ids.lock().await;
            if !active.insert(request_id) {
                drop(active);
                drop(permit);
                let duplicate = Response {
                    request_id,
                    status: Status::DuplicateInflightRequestId,
                    body: Vec::new(),
                };
                let encoded = encode_response(&duplicate, codec_limits)?;
                if response_tx.send(encoded).await.is_err() {
                    break Ok(());
                }
                continue;
            }
        }

        let handler = handler.clone();
        let response_tx = response_tx.clone();
        let active_request_ids = active_request_ids.clone();
        tokio::spawn(async move {
            let handler_response = handler.handle(request).await;
            let response = Response {
                request_id,
                status: handler_response.status,
                body: handler_response.body,
            };
            let encoded = encode_response(&response, codec_limits).or_else(|_| {
                encode_response(
                    &Response {
                        request_id,
                        status: Status::InternalError,
                        body: Vec::new(),
                    },
                    codec_limits,
                )
            });

            if let Ok(encoded) = encoded {
                let _ = response_tx.send(encoded).await;
            }

            active_request_ids.lock().await.remove(&request_id);
            drop(permit);
        });
    };

    drop(response_tx);
    let _ = writer_task.await;
    read_result
}

async fn read_request_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: CodecLimits,
) -> Result<Option<Vec<u8>>, RuntimeError> {
    let mut prefix = [0u8; FRAME_PREFIX_LEN];
    let first = reader.read(&mut prefix[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..]).await?;
    let decoded = decode_prefix(&prefix, limits)?;
    if decoded.kind != FrameKind::Request {
        return Err(RuntimeError::UnexpectedFrameKind(decoded.kind));
    }

    let payload_len = decoded.payload_len as usize;
    let total = FRAME_PREFIX_LEN
        .checked_add(payload_len)
        .ok_or(RuntimeError::Codec(CodecError::LengthOverflow))?;
    let mut frame = Vec::with_capacity(total);
    frame.extend_from_slice(&prefix);
    frame.resize(total, 0);
    reader.read_exact(&mut frame[FRAME_PREFIX_LEN..]).await?;
    Ok(Some(frame))
}

async fn write_responses<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut responses: mpsc::Receiver<Vec<u8>>,
) -> io::Result<()> {
    while let Some(frame) = responses.recv().await {
        writer.write_all(&frame).await?;
    }
    writer.shutdown().await
}

fn correlated_request_id(frame: &[u8]) -> Option<u64> {
    let start = FRAME_PREFIX_LEN;
    let end = start.checked_add(8)?;
    let bytes: [u8; 8] = frame.get(start..end)?.try_into().ok()?;
    let request_id = u64::from_be_bytes(bytes);
    (request_id != 0).then_some(request_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_plane::{
        decode_response, encode_request, Request, RequestBody, Status, FRAME_PREFIX_LEN,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{mpsc, Semaphore};
    use tokio::time::{timeout, Duration};

    fn test_codec_limits() -> CodecLimits {
        CodecLimits {
            max_frame: 4096,
            max_key: 128,
            max_value: 2048,
            max_batch_mutations: 16,
            max_batch_payload: 3072,
        }
    }

    fn request(id: u64) -> Request {
        Request {
            request_id: id,
            shard_id: 0,
            body: RequestBody::Get {
                key: format!("key-{id}").into_bytes(),
            },
        }
    }

    async fn write_request<W: AsyncWrite + Unpin>(writer: &mut W, id: u64) {
        let frame = encode_request(&request(id), test_codec_limits()).unwrap();
        writer.write_all(&frame).await.unwrap();
    }

    async fn read_response<R: AsyncRead + Unpin>(reader: &mut R) -> Response {
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        reader.read_exact(&mut prefix).await.unwrap();
        let decoded = decode_prefix(&prefix, test_codec_limits()).unwrap();
        assert_eq!(decoded.kind, FrameKind::Response);
        let mut frame = prefix.to_vec();
        frame.resize(FRAME_PREFIX_LEN + decoded.payload_len as usize, 0);
        reader
            .read_exact(&mut frame[FRAME_PREFIX_LEN..])
            .await
            .unwrap();
        decode_response(&frame, test_codec_limits()).unwrap()
    }

    struct EchoHandler;

    #[async_trait]
    impl RequestHandler for EchoHandler {
        async fn handle(&self, request: Request) -> HandlerResponse {
            HandlerResponse::ok(request.request_id.to_be_bytes().to_vec())
        }
    }

    #[tokio::test]
    async fn rejects_invalid_runtime_limits() {
        let (server, _) = duplex(64);
        let error = serve_stream(
            server,
            Arc::new(EchoHandler),
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 1,
                response_queue_capacity: 2,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, RuntimeError::InvalidLimits { .. }));
    }

    #[tokio::test]
    async fn tcp_listener_accepts_compact_request_and_correlates_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_listener(
            listener,
            Arc::new(EchoHandler),
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 2,
                response_queue_capacity: 2,
            },
        ));

        let mut client = TcpStream::connect(address).await.unwrap();
        write_request(&mut client, 7).await;
        let response = read_response(&mut client).await;
        assert_eq!(response.request_id, 7);
        assert_eq!(response.status, Status::Ok);
        assert_eq!(response.body, 7u64.to_be_bytes());
        server.abort();
    }

    struct BlockingHandler {
        started: mpsc::Sender<u64>,
        release: Arc<Semaphore>,
    }

    #[async_trait]
    impl RequestHandler for BlockingHandler {
        async fn handle(&self, request: Request) -> HandlerResponse {
            self.started.send(request.request_id).await.unwrap();
            let permit = self.release.acquire().await.unwrap();
            permit.forget();
            HandlerResponse::ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn saturation_stops_admission_at_configured_inflight_bound() {
        let (server, mut client) = duplex(16 * 1024);
        let (started_tx, mut started_rx) = mpsc::channel(8);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            started: started_tx,
            release: release.clone(),
        });
        let server_task = tokio::spawn(serve_stream(
            server,
            handler,
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 2,
                response_queue_capacity: 2,
            },
        ));

        write_request(&mut client, 1).await;
        write_request(&mut client, 2).await;
        write_request(&mut client, 3).await;

        assert_eq!(started_rx.recv().await, Some(1));
        assert_eq!(started_rx.recv().await, Some(2));
        assert!(timeout(Duration::from_millis(30), started_rx.recv())
            .await
            .is_err());

        release.add_permits(2);
        assert_eq!(started_rx.recv().await, Some(3));
        release.add_permits(1);
        drop(client);
        let _ = timeout(Duration::from_secs(1), server_task).await;
    }

    #[tokio::test]
    async fn duplicate_active_request_id_is_rejected_without_second_execution() {
        let (server, mut client) = duplex(16 * 1024);
        let (started_tx, mut started_rx) = mpsc::channel(8);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            started: started_tx,
            release: release.clone(),
        });
        let server_task = tokio::spawn(serve_stream(
            server,
            handler,
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 2,
                response_queue_capacity: 2,
            },
        ));

        write_request(&mut client, 11).await;
        assert_eq!(started_rx.recv().await, Some(11));
        write_request(&mut client, 11).await;

        let duplicate = read_response(&mut client).await;
        assert_eq!(duplicate.request_id, 11);
        assert_eq!(duplicate.status, Status::DuplicateInflightRequestId);
        assert!(timeout(Duration::from_millis(30), started_rx.recv())
            .await
            .is_err());

        release.add_permits(1);
        let completed = read_response(&mut client).await;
        assert_eq!(completed.request_id, 11);
        assert_eq!(completed.status, Status::Ok);
        drop(client);
        let _ = timeout(Duration::from_secs(1), server_task).await;
    }

    struct SideEffectHandler {
        started: mpsc::Sender<u64>,
        release: Arc<Semaphore>,
        completed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RequestHandler for SideEffectHandler {
        async fn handle(&self, request: Request) -> HandlerResponse {
            self.started.send(request.request_id).await.unwrap();
            let permit = self.release.acquire().await.unwrap();
            permit.forget();
            self.completed.fetch_add(1, Ordering::SeqCst);
            HandlerResponse::ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn admitted_work_continues_after_disconnect() {
        let (server, mut client) = duplex(16 * 1024);
        let (started_tx, mut started_rx) = mpsc::channel(4);
        let release = Arc::new(Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(SideEffectHandler {
            started: started_tx,
            release: release.clone(),
            completed: completed.clone(),
        });
        let _server_task = tokio::spawn(serve_stream(
            server,
            handler,
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 1,
                response_queue_capacity: 1,
            },
        ));

        write_request(&mut client, 21).await;
        assert_eq!(started_rx.recv().await, Some(21));
        drop(client);
        release.add_permits(1);

        timeout(Duration::from_secs(1), async {
            while completed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disconnect_before_admission_does_not_execute_partial_next_frame() {
        let (server, mut client) = duplex(16 * 1024);
        let (started_tx, mut started_rx) = mpsc::channel(4);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            started: started_tx,
            release: release.clone(),
        });
        let server_task = tokio::spawn(serve_stream(
            server,
            handler,
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 1,
                response_queue_capacity: 1,
            },
        ));

        write_request(&mut client, 31).await;
        assert_eq!(started_rx.recv().await, Some(31));
        let second = encode_request(&request(32), test_codec_limits()).unwrap();
        client.write_all(&second[..5]).await.unwrap();
        drop(client);
        release.add_permits(1);

        let result = timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(RuntimeError::Io(_))));
        assert!(started_rx.try_recv().is_err());
    }

    struct LargeResponseHandler {
        started: mpsc::Sender<u64>,
    }

    #[async_trait]
    impl RequestHandler for LargeResponseHandler {
        async fn handle(&self, request: Request) -> HandlerResponse {
            self.started.send(request.request_id).await.unwrap();
            HandlerResponse::ok(vec![0x5a; 512])
        }
    }

    #[tokio::test]
    async fn slow_client_applies_bounded_response_backpressure() {
        // Tiny duplex capacity makes the single writer block quickly when the client
        // deliberately does not read responses. The bounded response queue then
        // retains permits and prevents unbounded request execution.
        let (server, mut client) = duplex(64);
        let (started_tx, mut started_rx) = mpsc::channel(16);
        let server_task = tokio::spawn(serve_stream(
            server,
            Arc::new(LargeResponseHandler { started: started_tx }),
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 2,
                response_queue_capacity: 1,
            },
        ));

        for id in 41..=45 {
            write_request(&mut client, id).await;
        }

        // Writer owns one response, the queue can hold one more, and at most two
        // handlers may then be blocked trying to enqueue. The fifth request cannot
        // be admitted until the client drains responses.
        for expected in 41..=44 {
            assert_eq!(started_rx.recv().await, Some(expected));
        }
        assert!(timeout(Duration::from_millis(30), started_rx.recv())
            .await
            .is_err());

        for _ in 0..3 {
            let _ = read_response(&mut client).await;
        }
        assert_eq!(started_rx.recv().await, Some(45));
        drop(client);
        let _ = timeout(Duration::from_secs(1), server_task).await;
    }
}
