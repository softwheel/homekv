use std::fmt;

pub const MAGIC: u16 = 0x484b;
pub const VERSION: u8 = 1;
pub const FRAME_PREFIX_LEN: usize = 12;
pub const MAX_SHARD_ID: u16 = 1023;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Response = 2,
}

impl TryFrom<u8> for FrameKind {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            other => Err(CodecError::InvalidFrameKind(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    Get = 1,
    Set = 2,
    Delete = 3,
    Batch = 4,
}

impl TryFrom<u8> for Operation {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Get),
            2 => Ok(Self::Set),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Batch),
            other => Err(CodecError::InvalidOperation(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MutationKind {
    Set = 1,
    Delete = 2,
}

impl TryFrom<u8> for MutationKind {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Set),
            2 => Ok(Self::Delete),
            other => Err(CodecError::InvalidMutationKind(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    WrongShard = 2,
    StaleRouteOrNotOwner = 3,
    Overloaded = 4,
    ClosedOrUnavailable = 5,
    MalformedRequest = 6,
    UnsupportedVersion = 7,
    InternalError = 8,
    DuplicateInflightRequestId = 9,
}

impl TryFrom<u16> for Status {
    type Error = CodecError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::NotFound),
            2 => Ok(Self::WrongShard),
            3 => Ok(Self::StaleRouteOrNotOwner),
            4 => Ok(Self::Overloaded),
            5 => Ok(Self::ClosedOrUnavailable),
            6 => Ok(Self::MalformedRequest),
            7 => Ok(Self::UnsupportedVersion),
            8 => Ok(Self::InternalError),
            9 => Ok(Self::DuplicateInflightRequestId),
            other => Err(CodecError::InvalidStatus(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecLimits {
    pub max_frame: usize,
    pub max_key: usize,
    pub max_value: usize,
    pub max_batch_mutations: usize,
    pub max_batch_payload: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame: 8 * 1024 * 1024,
            max_key: 64 * 1024,
            max_value: 4 * 1024 * 1024,
            max_batch_mutations: 1024,
            max_batch_payload: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FramePrefix {
    pub kind: FrameKind,
    pub payload_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestBody {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Batch { mutations: Vec<Mutation> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub request_id: u64,
    pub shard_id: u16,
    pub body: RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub request_id: u64,
    pub status: Status,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    TrailingBytes,
    BadMagic(u16),
    UnsupportedVersion(u8),
    InvalidFrameKind(u8),
    ReservedBits,
    FrameTooLarge { actual: usize, max: usize },
    LengthOverflow,
    ZeroRequestId,
    InvalidShardId(u16),
    InvalidOperation(u8),
    InvalidMutationKind(u8),
    InvalidStatus(u16),
    InvalidOperationFlags(u8),
    InvalidResponseFlags(u16),
    InvalidMutationReserved(u8),
    KeyTooLarge { actual: usize, max: usize },
    ValueTooLarge { actual: usize, max: usize },
    BatchTooLarge { actual: usize, max: usize },
    BatchPayloadTooLarge { actual: usize, max: usize },
    DeleteHasValue,
    WrongFrameKind { expected: FrameKind, actual: FrameKind },
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for CodecError {}

pub fn decode_prefix(input: &[u8], limits: CodecLimits) -> Result<FramePrefix, CodecError> {
    if input.len() < FRAME_PREFIX_LEN {
        return Err(CodecError::Truncated);
    }
    let magic = u16::from_be_bytes([input[0], input[1]]);
    if magic != MAGIC {
        return Err(CodecError::BadMagic(magic));
    }
    if input[2] != VERSION {
        return Err(CodecError::UnsupportedVersion(input[2]));
    }
    let kind = FrameKind::try_from(input[3])?;
    let flags = u16::from_be_bytes([input[4], input[5]]);
    let payload_len = u32::from_be_bytes([input[6], input[7], input[8], input[9]]);
    let reserved = u16::from_be_bytes([input[10], input[11]]);
    if flags != 0 || reserved != 0 {
        return Err(CodecError::ReservedBits);
    }
    let total = FRAME_PREFIX_LEN
        .checked_add(payload_len as usize)
        .ok_or(CodecError::LengthOverflow)?;
    if total > limits.max_frame {
        return Err(CodecError::FrameTooLarge {
            actual: total,
            max: limits.max_frame,
        });
    }
    Ok(FramePrefix { kind, payload_len })
}

pub fn encode_request(request: &Request, limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    validate_request_header(request.request_id, request.shard_id)?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&request.request_id.to_be_bytes());
    payload.extend_from_slice(&request.shard_id.to_be_bytes());
    payload.push(operation_of(&request.body) as u8);
    payload.push(0);

    match &request.body {
        RequestBody::Get { key } | RequestBody::Delete { key } => {
            validate_key(key, limits)?;
            put_len_u32(&mut payload, key.len())?;
            payload.extend_from_slice(key);
        }
        RequestBody::Set { key, value } => {
            validate_key(key, limits)?;
            validate_value(value, limits)?;
            put_len_u32(&mut payload, key.len())?;
            put_len_u32(&mut payload, value.len())?;
            payload.extend_from_slice(key);
            payload.extend_from_slice(value);
        }
        RequestBody::Batch { mutations } => {
            if mutations.len() > limits.max_batch_mutations || mutations.len() > u16::MAX as usize {
                return Err(CodecError::BatchTooLarge {
                    actual: mutations.len(),
                    max: limits.max_batch_mutations.min(u16::MAX as usize),
                });
            }
            payload.extend_from_slice(&(mutations.len() as u16).to_be_bytes());
            let mut aggregate = 0usize;
            for mutation in mutations {
                match mutation {
                    Mutation::Set { key, value } => {
                        validate_key(key, limits)?;
                        validate_value(value, limits)?;
                        aggregate = checked_aggregate(aggregate, key.len(), value.len(), limits)?;
                        payload.push(MutationKind::Set as u8);
                        payload.push(0);
                        put_len_u32(&mut payload, key.len())?;
                        put_len_u32(&mut payload, value.len())?;
                        payload.extend_from_slice(key);
                        payload.extend_from_slice(value);
                    }
                    Mutation::Delete { key } => {
                        validate_key(key, limits)?;
                        aggregate = checked_aggregate(aggregate, key.len(), 0, limits)?;
                        payload.push(MutationKind::Delete as u8);
                        payload.push(0);
                        put_len_u32(&mut payload, key.len())?;
                        payload.extend_from_slice(&0u32.to_be_bytes());
                        payload.extend_from_slice(key);
                    }
                }
            }
        }
    }
    encode_frame(FrameKind::Request, &payload, limits)
}

pub fn decode_request(frame: &[u8], limits: CodecLimits) -> Result<Request, CodecError> {
    let payload = checked_payload(frame, FrameKind::Request, limits)?;
    let mut cursor = Cursor::new(payload);
    let request_id = cursor.u64()?;
    let shard_id = cursor.u16()?;
    validate_request_header(request_id, shard_id)?;
    let op = Operation::try_from(cursor.u8()?)?;
    let op_flags = cursor.u8()?;
    if op_flags != 0 {
        return Err(CodecError::InvalidOperationFlags(op_flags));
    }

    let body = match op {
        Operation::Get => RequestBody::Get {
            key: read_key(&mut cursor, limits)?,
        },
        Operation::Delete => RequestBody::Delete {
            key: read_key(&mut cursor, limits)?,
        },
        Operation::Set => {
            let key_len = cursor.u32()? as usize;
            let value_len = cursor.u32()? as usize;
            validate_lengths(key_len, value_len, limits)?;
            let key = cursor.bytes(key_len)?.to_vec();
            let value = cursor.bytes(value_len)?.to_vec();
            RequestBody::Set { key, value }
        }
        Operation::Batch => {
            let count = cursor.u16()? as usize;
            if count > limits.max_batch_mutations {
                return Err(CodecError::BatchTooLarge {
                    actual: count,
                    max: limits.max_batch_mutations,
                });
            }
            let mut mutations = Vec::with_capacity(count);
            let mut aggregate = 0usize;
            for _ in 0..count {
                let kind = MutationKind::try_from(cursor.u8()?)?;
                let reserved = cursor.u8()?;
                if reserved != 0 {
                    return Err(CodecError::InvalidMutationReserved(reserved));
                }
                let key_len = cursor.u32()? as usize;
                let value_len = cursor.u32()? as usize;
                validate_lengths(key_len, value_len, limits)?;
                aggregate = checked_aggregate(aggregate, key_len, value_len, limits)?;
                let key = cursor.bytes(key_len)?.to_vec();
                match kind {
                    MutationKind::Set => {
                        let value = cursor.bytes(value_len)?.to_vec();
                        mutations.push(Mutation::Set { key, value });
                    }
                    MutationKind::Delete => {
                        if value_len != 0 {
                            return Err(CodecError::DeleteHasValue);
                        }
                        mutations.push(Mutation::Delete { key });
                    }
                }
            }
            RequestBody::Batch { mutations }
        }
    };
    cursor.finish()?;
    Ok(Request { request_id, shard_id, body })
}

pub fn encode_response(response: &Response, limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    if response.request_id == 0 {
        return Err(CodecError::ZeroRequestId);
    }
    let mut payload = Vec::with_capacity(12usize.saturating_add(response.body.len()));
    payload.extend_from_slice(&response.request_id.to_be_bytes());
    payload.extend_from_slice(&(response.status as u16).to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&response.body);
    encode_frame(FrameKind::Response, &payload, limits)
}

pub fn decode_response(frame: &[u8], limits: CodecLimits) -> Result<Response, CodecError> {
    let payload = checked_payload(frame, FrameKind::Response, limits)?;
    let mut cursor = Cursor::new(payload);
    let request_id = cursor.u64()?;
    if request_id == 0 {
        return Err(CodecError::ZeroRequestId);
    }
    let status = Status::try_from(cursor.u16()?)?;
    let flags = cursor.u16()?;
    if flags != 0 {
        return Err(CodecError::InvalidResponseFlags(flags));
    }
    let body = cursor.remaining().to_vec();
    Ok(Response { request_id, status, body })
}

fn encode_frame(kind: FrameKind, payload: &[u8], limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    let total = FRAME_PREFIX_LEN
        .checked_add(payload.len())
        .ok_or(CodecError::LengthOverflow)?;
    if total > limits.max_frame || payload.len() > u32::MAX as usize {
        return Err(CodecError::FrameTooLarge { actual: total, max: limits.max_frame });
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.push(VERSION);
    out.push(kind as u8);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn checked_payload<'a>(frame: &'a [u8], expected: FrameKind, limits: CodecLimits) -> Result<&'a [u8], CodecError> {
    let prefix = decode_prefix(frame, limits)?;
    if prefix.kind != expected {
        return Err(CodecError::WrongFrameKind { expected, actual: prefix.kind });
    }
    let expected_len = FRAME_PREFIX_LEN
        .checked_add(prefix.payload_len as usize)
        .ok_or(CodecError::LengthOverflow)?;
    if frame.len() < expected_len {
        return Err(CodecError::Truncated);
    }
    if frame.len() != expected_len {
        return Err(CodecError::TrailingBytes);
    }
    Ok(&frame[FRAME_PREFIX_LEN..])
}

fn validate_request_header(request_id: u64, shard_id: u16) -> Result<(), CodecError> {
    if request_id == 0 {
        return Err(CodecError::ZeroRequestId);
    }
    if shard_id > MAX_SHARD_ID {
        return Err(CodecError::InvalidShardId(shard_id));
    }
    Ok(())
}

fn validate_key(key: &[u8], limits: CodecLimits) -> Result<(), CodecError> {
    if key.len() > limits.max_key {
        return Err(CodecError::KeyTooLarge { actual: key.len(), max: limits.max_key });
    }
    Ok(())
}

fn validate_value(value: &[u8], limits: CodecLimits) -> Result<(), CodecError> {
    if value.len() > limits.max_value {
        return Err(CodecError::ValueTooLarge { actual: value.len(), max: limits.max_value });
    }
    Ok(())
}

fn validate_lengths(key_len: usize, value_len: usize, limits: CodecLimits) -> Result<(), CodecError> {
    if key_len > limits.max_key {
        return Err(CodecError::KeyTooLarge { actual: key_len, max: limits.max_key });
    }
    if value_len > limits.max_value {
        return Err(CodecError::ValueTooLarge { actual: value_len, max: limits.max_value });
    }
    Ok(())
}

fn checked_aggregate(current: usize, key: usize, value: usize, limits: CodecLimits) -> Result<usize, CodecError> {
    let next = current
        .checked_add(key)
        .and_then(|n| n.checked_add(value))
        .ok_or(CodecError::LengthOverflow)?;
    if next > limits.max_batch_payload {
        return Err(CodecError::BatchPayloadTooLarge { actual: next, max: limits.max_batch_payload });
    }
    Ok(next)
}

fn read_key(cursor: &mut Cursor<'_>, limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    let len = cursor.u32()? as usize;
    if len > limits.max_key {
        return Err(CodecError::KeyTooLarge { actual: len, max: limits.max_key });
    }
    Ok(cursor.bytes(len)?.to_vec())
}

fn put_len_u32(out: &mut Vec<u8>, len: usize) -> Result<(), CodecError> {
    let len = u32::try_from(len).map_err(|_| CodecError::LengthOverflow)?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn operation_of(body: &RequestBody) -> Operation {
    match body {
        RequestBody::Get { .. } => Operation::Get,
        RequestBody::Set { .. } => Operation::Set,
        RequestBody::Delete { .. } => Operation::Delete,
        RequestBody::Batch { .. } => Operation::Batch,
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.pos.checked_add(len).ok_or(CodecError::LengthOverflow)?;
        if end > self.input.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.input[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        let b = self.bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        let b = self.bytes(8)?;
        Ok(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    fn remaining(&self) -> &'a [u8] {
        &self.input[self.pos..]
    }

    fn finish(&self) -> Result<(), CodecError> {
        if self.pos == self.input.len() { Ok(()) } else { Err(CodecError::TrailingBytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_limits() -> CodecLimits {
        CodecLimits { max_frame: 256, max_key: 8, max_value: 16, max_batch_mutations: 3, max_batch_payload: 20 }
    }

    #[test]
    fn get_golden_vector_is_stable() {
        let request = Request { request_id: 0x0102030405060708, shard_id: 7, body: RequestBody::Get { key: vec![0xaa, 0xbb] } };
        let encoded = encode_request(&request, tiny_limits()).unwrap();
        let expected = vec![
            0x48,0x4b,0x01,0x01,0x00,0x00,0x00,0x00,0x00,0x12,0x00,0x00,
            0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x00,0x07,0x01,0x00,
            0x00,0x00,0x00,0x02,0xaa,0xbb,
        ];
        assert_eq!(encoded, expected);
        assert_eq!(decode_request(&encoded, tiny_limits()).unwrap(), request);
    }

    #[test]
    fn request_round_trips_all_operations_with_binary_data() {
        let cases = [
            Request { request_id: 1, shard_id: 0, body: RequestBody::Get { key: vec![0, 0xff] } },
            Request { request_id: 2, shard_id: 1023, body: RequestBody::Set { key: vec![1,2], value: vec![0,3,0xff] } },
            Request { request_id: 3, shard_id: 4, body: RequestBody::Delete { key: vec![9] } },
            Request { request_id: 4, shard_id: 4, body: RequestBody::Batch { mutations: vec![Mutation::Set { key: vec![1], value: vec![2,3] }, Mutation::Delete { key: vec![4] }] } },
        ];
        for request in cases {
            let wire = encode_request(&request, tiny_limits()).unwrap();
            assert_eq!(decode_request(&wire, tiny_limits()).unwrap(), request);
        }
    }

    #[test]
    fn response_round_trip_and_status_numbers_are_stable() {
        let response = Response { request_id: 9, status: Status::WrongShard, body: vec![1,2,3] };
        let wire = encode_response(&response, tiny_limits()).unwrap();
        assert_eq!(wire[3], FrameKind::Response as u8);
        assert_eq!(&wire[20..22], &(Status::WrongShard as u16).to_be_bytes());
        assert_eq!(decode_response(&wire, tiny_limits()).unwrap(), response);
    }

    #[test]
    fn prefix_rejects_bad_magic_version_reserved_and_oversize_before_payload_access() {
        let mut prefix = vec![0x48,0x4b,1,1,0,0,0,0,0,1,0,0];
        prefix[0] = 0;
        assert!(matches!(decode_prefix(&prefix, tiny_limits()), Err(CodecError::BadMagic(_))));
        prefix[0] = 0x48; prefix[2] = 2;
        assert_eq!(decode_prefix(&prefix, tiny_limits()), Err(CodecError::UnsupportedVersion(2)));
        prefix[2] = 1; prefix[4] = 1;
        assert_eq!(decode_prefix(&prefix, tiny_limits()), Err(CodecError::ReservedBits));
        prefix[4] = 0; prefix[6..10].copy_from_slice(&1000u32.to_be_bytes());
        assert!(matches!(decode_prefix(&prefix, tiny_limits()), Err(CodecError::FrameTooLarge { .. })));
    }

    #[test]
    fn truncated_and_trailing_frames_are_rejected() {
        let request = Request { request_id: 1, shard_id: 1, body: RequestBody::Get { key: vec![1] } };
        let wire = encode_request(&request, tiny_limits()).unwrap();
        for len in 0..wire.len() {
            assert!(decode_request(&wire[..len], tiny_limits()).is_err(), "prefix len {len}");
        }
        let mut extra = wire.clone(); extra.push(0);
        assert_eq!(decode_request(&extra, tiny_limits()), Err(CodecError::TrailingBytes));
    }

    #[test]
    fn request_header_and_enum_validation_is_strict() {
        let zero = Request { request_id: 0, shard_id: 1, body: RequestBody::Get { key: vec![1] } };
        assert_eq!(encode_request(&zero, tiny_limits()), Err(CodecError::ZeroRequestId));
        let bad_shard = Request { request_id: 1, shard_id: 1024, body: RequestBody::Get { key: vec![1] } };
        assert_eq!(encode_request(&bad_shard, tiny_limits()), Err(CodecError::InvalidShardId(1024)));

        let valid = Request { request_id: 1, shard_id: 1, body: RequestBody::Get { key: vec![1] } };
        let mut wire = encode_request(&valid, tiny_limits()).unwrap();
        wire[22] = 99;
        assert_eq!(decode_request(&wire, tiny_limits()), Err(CodecError::InvalidOperation(99)));
        wire[22] = 1; wire[23] = 1;
        assert_eq!(decode_request(&wire, tiny_limits()), Err(CodecError::InvalidOperationFlags(1)));
    }

    #[test]
    fn key_value_and_batch_bounds_are_enforced() {
        let long_key = Request { request_id: 1, shard_id: 1, body: RequestBody::Get { key: vec![0;9] } };
        assert!(matches!(encode_request(&long_key, tiny_limits()), Err(CodecError::KeyTooLarge { .. })));
        let long_value = Request { request_id: 1, shard_id: 1, body: RequestBody::Set { key: vec![1], value: vec![0;17] } };
        assert!(matches!(encode_request(&long_value, tiny_limits()), Err(CodecError::ValueTooLarge { .. })));
        let too_many = Request { request_id: 1, shard_id: 1, body: RequestBody::Batch { mutations: vec![Mutation::Delete { key: vec![1] }; 4] } };
        assert!(matches!(encode_request(&too_many, tiny_limits()), Err(CodecError::BatchTooLarge { .. })));
        let too_big = Request { request_id: 1, shard_id: 1, body: RequestBody::Batch { mutations: vec![Mutation::Set { key: vec![1;8], value: vec![2;13] }] } };
        assert!(matches!(encode_request(&too_big, tiny_limits()), Err(CodecError::BatchPayloadTooLarge { .. })));
    }

    #[test]
    fn malformed_batch_delete_value_and_reserved_byte_are_rejected() {
        let request = Request { request_id: 5, shard_id: 2, body: RequestBody::Batch { mutations: vec![Mutation::Delete { key: vec![7] }] } };
        let mut wire = encode_request(&request, tiny_limits()).unwrap();
        // prefix 12 + common request 12 + count 2; mutation starts at 26.
        wire[27] = 1;
        assert_eq!(decode_request(&wire, tiny_limits()), Err(CodecError::InvalidMutationReserved(1)));
        wire[27] = 0;
        wire[32..36].copy_from_slice(&1u32.to_be_bytes());
        assert_eq!(decode_request(&wire, tiny_limits()), Err(CodecError::DeleteHasValue));
    }

    #[test]
    fn arbitrary_short_or_corrupted_inputs_never_panic() {
        let limits = tiny_limits();
        for len in 0..128usize {
            let mut input = vec![0u8; len];
            for (i, byte) in input.iter_mut().enumerate() {
                *byte = (i as u8).wrapping_mul(37).wrapping_add(len as u8);
            }
            let _ = decode_request(&input, limits);
            let _ = decode_response(&input, limits);
            let _ = decode_prefix(&input, limits);
        }
    }
}
