use crate::data_plane::{
    decode_prefix, decode_request, encode_response, CodecError, CodecLimits, FrameKind, Response,
    Status, FRAME_PREFIX_LEN,
};
use async_trait::async_trait;
use std::collections::HashSet;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub frames_accepted: u64,
    pub frames_rejected: u64,
    pub requests_accepted: u64,
    pub requests_rejected: u64,
    pub protocol_errors: u64,
    pub malformed_errors: u64,
    pub unsupported_version_errors: u64,
    pub overload_responses: u64,
    pub closed_responses: u64,
    pub active_connections: usize,
    pub peak_connections: usize,
    pub in_flight_requests: usize,
    pub peak_in_flight_requests: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub completed_requests: u64,
    pub handler_latency_ns_total: u64,
}

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    frames_accepted: AtomicU64,
    frames_rejected: AtomicU64,
    requests_accepted: AtomicU64,
    requests_rejected: AtomicU64,
    protocol_errors: AtomicU64,
    malformed_errors: AtomicU64,
    unsupported_version_errors: AtomicU64,
    overload_responses: AtomicU64,
    closed_responses: AtomicU64,
    active_connections: AtomicUsize,
    peak_connections: AtomicUsize,
    in_flight_requests: AtomicUsize,
    peak_in_flight_requests: AtomicUsize,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    completed_requests: AtomicU64,
    handler_latency_ns_total: AtomicU64,
}

impl RuntimeMetrics {
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            frames_accepted: self.frames_accepted.load(Ordering::Relaxed),
            frames_rejected: self.frames_rejected.load(Ordering::Relaxed),
            requests_accepted: self.requests_accepted.load(Ordering::Relaxed),
            requests_rejected: self.requests_rejected.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            malformed_errors: self.malformed_errors.load(Ordering::Relaxed),
            unsupported_version_errors: self.unsupported_version_errors.load(Ordering::Relaxed),
            overload_responses: self.overload_responses.load(Ordering::Relaxed),
            closed_responses: self.closed_responses.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            peak_connections: self.peak_connections.load(Ordering::Relaxed),
            in_flight_requests: self.in_flight_requests.load(Ordering::Relaxed),
            peak_in_flight_requests: self.peak_in_flight_requests.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            completed_requests: self.completed_requests.load(Ordering::Relaxed),
            handler_latency_ns_total: self.handler_latency_ns_total.load(Ordering::Relaxed),
        }
    }

    fn connection_opened(&self) {
        let current = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_connections.fetch_max(current, Ordering::Relaxed);
    }

    fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn request_started(&self) {
        self.requests_accepted.fetch_add(1, Ordering::Relaxed);
        let current = self.in_flight_requests.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_in_flight_requests
            .fetch_max(current, Ordering::Relaxed);
    }

    fn request_completed(&self, elapsed_ns: u64) {
        self.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
        self.handler_latency_ns_total
            .fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    fn protocol_error(&self, error: &CodecError) {
        self.protocol_errors.fetch_add(1, Ordering::Relaxed);
        match error {
            CodecError::UnsupportedVersion(_) => {
                self.unsupported_version_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.malformed_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_response_status(&self, status: Status) {
        match status {
            Status::Overloaded => {
                self.overload_responses.fetch_add(1, Ordering::Relaxed);
            }
            Status::ClosedOrUnavailable => {
                self.closed_responses.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

struct ConnectionGuard {
    metrics: Arc<RuntimeMetrics>,
}

impl ConnectionGuard {
    fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        metrics.connection_opened();
        Self { metrics }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.connection_closed();
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
    serve_listener_with_metrics(
        listener,
        handler,
        codec_limits,
        runtime_limits,
        Arc::new(RuntimeMetrics::default()),
    )
    .await
}

pub async fn serve_listener_with_metrics<H: RequestHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(), RuntimeError> {
    let runtime_limits = runtime_limits.validate()?;
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let handler = handler.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _ = serve_connection_with_metrics(
                stream,
                handler,
                codec_limits,
                runtime_limits,
                metrics,
            )
            .await;
        });
    }
}

pub async fn serve_connection<H: RequestHandler>(
    stream: TcpStream,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
) -> Result<(), RuntimeError> {
    serve_connection_with_metrics(
        stream,
        handler,
        codec_limits,
        runtime_limits,
        Arc::new(RuntimeMetrics::default()),
    )
    .await
}

pub async fn serve_connection_with_metrics<H: RequestHandler>(
    stream: TcpStream,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(), RuntimeError> {
    serve_stream_with_metrics(stream, handler, codec_limits, runtime_limits, metrics).await
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
    serve_stream_with_metrics(
        stream,
        handler,
        codec_limits,
        runtime_limits,
        Arc::new(RuntimeMetrics::default()),
    )
    .await
}

pub async fn serve_stream_with_metrics<S, H>(
    stream: S,
    handler: Arc<H>,
    codec_limits: CodecLimits,
    runtime_limits: RuntimeLimits,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: RequestHandler,
{
    let runtime_limits = runtime_limits.validate()?;
    let _connection_guard = ConnectionGuard::new(metrics.clone());
    let semaphore = Arc::new(Semaphore::new(runtime_limits.max_in_flight));
    let active_request_ids = Arc::new(Mutex::new(HashSet::<u64>::with_capacity(
        runtime_limits.max_in_flight,
    )));
    let (response_tx, response_rx) =
        mpsc::channel::<Vec<u8>>(runtime_limits.response_queue_capacity);
    let (mut reader, writer) = tokio::io::split(stream);
    let writer_metrics = metrics.clone();
    let writer_task = tokio::spawn(write_responses(writer, response_rx, writer_metrics));

    let read_result = loop {
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break Ok(()),
        };

        let frame = match read_request_frame(&mut reader, codec_limits, &metrics).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                drop(permit);
                break Ok(());
            }
            Err(error) => {
                drop(permit);
                metrics.frames_rejected.fetch_add(1, Ordering::Relaxed);
                if let RuntimeError::Codec(codec_error) = &error {
                    metrics.protocol_error(codec_error);
                } else if matches!(error, RuntimeError::UnexpectedFrameKind(_)) {
                    metrics.protocol_errors.fetch_add(1, Ordering::Relaxed);
                    metrics.malformed_errors.fetch_add(1, Ordering::Relaxed);
                }
                break Err(error);
            }
        };

        let request = match decode_request(&frame, codec_limits) {
            Ok(request) => {
                metrics.frames_accepted.fetch_add(1, Ordering::Relaxed);
                request
            }
            Err(error) => {
                drop(permit);
                metrics.frames_rejected.fetch_add(1, Ordering::Relaxed);
                metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
                metrics.protocol_error(&error);
                if let Some(request_id) = correlated_request_id(&frame) {
                    let response = Response {
                        request_id,
                        status: match error {
                            CodecError::UnsupportedVersion(_) => Status::UnsupportedVersion,
                            _ => Status::MalformedRequest,
                        },
                        body: Vec::new(),
                    };
                    metrics.record_response_status(response.status);
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
                metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
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

        metrics.request_started();
        let handler = handler.clone();
        let response_tx = response_tx.clone();
        let active_request_ids = active_request_ids.clone();
        let request_metrics = metrics.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let handler_response = handler.handle(request).await;
            request_metrics.record_response_status(handler_response.status);
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
            let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            request_metrics.request_completed(elapsed_ns);
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
    metrics: &RuntimeMetrics,
) -> Result<Option<Vec<u8>>, RuntimeError> {
    let mut prefix = [0u8; FRAME_PREFIX_LEN];
    let first = reader.read(&mut prefix[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    metrics.bytes_read.fetch_add(first as u64, Ordering::Relaxed);
    read_exact_counted(reader, &mut prefix[1..], metrics).await?;
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
    read_exact_counted(reader, &mut frame[FRAME_PREFIX_LEN..], metrics).await?;
    Ok(Some(frame))
}

async fn read_exact_counted<R: AsyncRead + Unpin>(
    reader: &mut R,
    mut buffer: &mut [u8],
    metrics: &RuntimeMetrics,
) -> io::Result<()> {
    while !buffer.is_empty() {
        let read = reader.read(buffer).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compact frame ended before declared length",
            ));
        }
        metrics.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        let (_, remaining) = buffer.split_at_mut(read);
        buffer = remaining;
    }
    Ok(())
}

async fn write_responses<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut responses: mpsc::Receiver<Vec<u8>>,
    metrics: Arc<RuntimeMetrics>,
) -> io::Result<()> {
    while let Some(frame) = responses.recv().await {
        write_all_counted(&mut writer, &frame, &metrics).await?;
    }
    writer.shutdown().await
}

async fn write_all_counted<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut buffer: &[u8],
    metrics: &RuntimeMetrics,
) -> io::Result<()> {
    while !buffer.is_empty() {
        let written = writer.write(buffer).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write compact response",
            ));
        }
        metrics
            .bytes_written
            .fetch_add(written as u64, Ordering::Relaxed);
        buffer = &buffer[written..];
    }
    Ok(())
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
        decode_response, encode_request, Request, RequestBody, Status, FRAME_PREFIX_LEN, VERSION,
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

    fn runtime_limits() -> RuntimeLimits {
        RuntimeLimits {
            max_in_flight: 2,
            response_queue_capacity: 2,
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
            runtime_limits(),
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
            runtime_limits(),
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
            runtime_limits(),
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

    struct StatusHandler;

    #[async_trait]
    impl RequestHandler for StatusHandler {
        async fn handle(&self, request: Request) -> HandlerResponse {
            match request.request_id {
                71 => HandlerResponse::new(Status::Overloaded, Vec::new()),
                72 => HandlerResponse::new(Status::ClosedOrUnavailable, Vec::new()),
                _ => HandlerResponse::ok(Vec::new()),
            }
        }
    }

    #[tokio::test]
    async fn metrics_cover_success_status_bytes_and_connection_lifecycle() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let (server, mut client) = duplex(16 * 1024);
        let server_task = tokio::spawn(serve_stream_with_metrics(
            server,
            Arc::new(StatusHandler),
            test_codec_limits(),
            runtime_limits(),
            metrics.clone(),
        ));

        for id in [70, 71, 72] {
            write_request(&mut client, id).await;
            let response = read_response(&mut client).await;
            assert_eq!(response.request_id, id);
        }
        drop(client);
        timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.frames_accepted, 3);
        assert_eq!(snapshot.frames_rejected, 0);
        assert_eq!(snapshot.requests_accepted, 3);
        assert_eq!(snapshot.requests_rejected, 0);
        assert_eq!(snapshot.overload_responses, 1);
        assert_eq!(snapshot.closed_responses, 1);
        assert_eq!(snapshot.active_connections, 0);
        assert_eq!(snapshot.peak_connections, 1);
        assert_eq!(snapshot.in_flight_requests, 0);
        assert!(snapshot.peak_in_flight_requests >= 1);
        assert_eq!(snapshot.completed_requests, 3);
        assert!(snapshot.bytes_read > 0);
        assert!(snapshot.bytes_written > 0);
    }

    #[tokio::test]
    async fn metrics_track_duplicate_request_rejection_and_peak_inflight() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let (server, mut client) = duplex(16 * 1024);
        let (started_tx, mut started_rx) = mpsc::channel(8);
        let release = Arc::new(Semaphore::new(0));
        let handler = Arc::new(BlockingHandler {
            started: started_tx,
            release: release.clone(),
        });
        let server_task = tokio::spawn(serve_stream_with_metrics(
            server,
            handler,
            test_codec_limits(),
            RuntimeLimits {
                max_in_flight: 3,
                response_queue_capacity: 3,
            },
            metrics.clone(),
        ));

        write_request(&mut client, 80).await;
        write_request(&mut client, 81).await;
        assert_eq!(started_rx.recv().await, Some(80));
        assert_eq!(started_rx.recv().await, Some(81));
        write_request(&mut client, 80).await;
        let duplicate = read_response(&mut client).await;
        assert_eq!(duplicate.request_id, 80);
        assert_eq!(duplicate.status, Status::DuplicateInflightRequestId);

        release.add_permits(2);
        for _ in 0..2 {
            let _ = read_response(&mut client).await;
        }
        drop(client);
        let _ = timeout(Duration::from_secs(1), server_task).await;

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.frames_accepted, 3);
        assert_eq!(snapshot.requests_accepted, 2);
        assert_eq!(snapshot.requests_rejected, 1);
        assert_eq!(snapshot.peak_in_flight_requests, 2);
        assert_eq!(snapshot.in_flight_requests, 0);
    }

    #[tokio::test]
    async fn metrics_track_correlated_malformed_request() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let (server, mut client) = duplex(4096);
        let server_task = tokio::spawn(serve_stream_with_metrics(
            server,
            Arc::new(EchoHandler),
            test_codec_limits(),
            runtime_limits(),
            metrics.clone(),
        ));

        let mut frame = encode_request(&request(90), test_codec_limits()).unwrap();
        frame[FRAME_PREFIX_LEN + 10] = 0xff;
        client.write_all(&frame).await.unwrap();
        let response = read_response(&mut client).await;
        assert_eq!(response.request_id, 90);
        assert_eq!(response.status, Status::MalformedRequest);
        drop(client);
        let _ = timeout(Duration::from_secs(1), server_task).await;

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.frames_accepted, 0);
        assert_eq!(snapshot.frames_rejected, 1);
        assert_eq!(snapshot.requests_rejected, 1);
        assert_eq!(snapshot.protocol_errors, 1);
        assert_eq!(snapshot.malformed_errors, 1);
        assert_eq!(snapshot.unsupported_version_errors, 0);
    }

    #[tokio::test]
    async fn metrics_track_unsupported_version_and_partial_disconnect_bytes() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let (server, mut client) = duplex(4096);
        let server_task = tokio::spawn(serve_stream_with_metrics(
            server,
            Arc::new(EchoHandler),
            test_codec_limits(),
            runtime_limits(),
            metrics.clone(),
        ));

        let mut frame = encode_request(&request(91), test_codec_limits()).unwrap();
        frame[2] = VERSION.wrapping_add(1);
        client.write_all(&frame[..FRAME_PREFIX_LEN]).await.unwrap();
        drop(client);
        let result = timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(RuntimeError::Codec(CodecError::UnsupportedVersion(_)))));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.frames_rejected, 1);
        assert_eq!(snapshot.protocol_errors, 1);
        assert_eq!(snapshot.unsupported_version_errors, 1);
        assert_eq!(snapshot.malformed_errors, 0);
        assert_eq!(snapshot.active_connections, 0);
        assert_eq!(snapshot.bytes_read, FRAME_PREFIX_LEN as u64);
    }
}
