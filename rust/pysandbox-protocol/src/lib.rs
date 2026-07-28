use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

pub const PROTOCOL_VERSION: u16 = 2;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 50 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Frame {
    pub version: u16,
    pub kind: FrameKind,
    pub worker_id: u64,
    pub request_id: u64,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(kind: FrameKind, worker_id: u64, request_id: u64, payload: Vec<u8>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            kind,
            worker_id,
            request_id,
            payload,
        }
    }

    fn validate_version(&self) -> Result<(), ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(self.version));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    Execute,
    ExecuteResult,
    GuestCall,
    GuestResponse,
    WorkerCall,
    WorkerResponse,
    VfsRequest,
    VfsResponse,
    InvalidateVfs,
    Output,
    UpdateLimits,
    Cancel,
    CloseWorker,
    ControlResult,
    Shutdown,
    HealthCheck,
    HealthStatus,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteRequest {
    pub program: String,
    pub limits: ExecutionLimits,
    pub rpc_methods: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutionLimits {
    pub max_memory_bytes: u64,
    pub max_output_bytes: u64,
    pub max_guest_rpc_bytes: u64,
    pub fuel: u64,
    pub timeout_ms: Option<u64>,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 128 * 1024 * 1024,
            max_output_bytes: 256 * 1024,
            max_guest_rpc_bytes: 10 * 1024 * 1024,
            fuel: u64::MAX,
            timeout_ms: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputSource {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutputPayload {
    pub source: OutputSource,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecuteResult {
    pub error: Option<String>,
    pub reason: TerminationReason,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    Completed,
    GuestError,
    Timeout,
    Cancelled,
    FuelExhausted,
    OutputLimit,
    MemoryLimit,
    RuntimeError,
    InfrastructureError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum ExecutionControl {
    SetFuel {
        execution_id: u64,
        fuel: u64,
    },
    AddFuel {
        execution_id: u64,
        amount: u64,
        cap: Option<u64>,
    },
    SetLimits {
        execution_id: u64,
        max_memory_bytes: Option<u64>,
        max_output_bytes: Option<u64>,
        max_guest_rpc_bytes: Option<u64>,
        timeout_ms: Option<u64>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CancelRequest {
    pub execution_id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlResult {
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RpcCall {
    pub method: String,
    #[serde(with = "serde_bytes")]
    pub arguments: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RpcResult {
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerRpcCall {
    pub path: Vec<String>,
    pub fuel: Option<FuelOperation>,
    #[serde(with = "serde_bytes")]
    pub arguments: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum FuelOperation {
    Set { fuel: u64 },
    Add { amount: u64, cap: Option<u64> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum VfsRequest {
    Stat { path: String },
    Read { path: String },
    List { path: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsNodeKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VfsMetadata {
    pub kind: VfsNodeKind,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VfsDirectoryEntry {
    pub name: String,
    pub metadata: VfsMetadata,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum VfsValue {
    Metadata(VfsMetadata),
    Bytes(#[serde(with = "serde_bytes")] Vec<u8>),
    Entries(Vec<VfsDirectoryEntry>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VfsResponse {
    pub value: Option<VfsValue>,
    pub error: Option<VfsError>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VfsError {
    pub code: VfsErrorCode,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsErrorCode {
    NotFound,
    NotDirectory,
    IsDirectory,
    PermissionDenied,
    Invalid,
    Io,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvalidateVfs {
    pub path: Option<String>,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("frame length {actual} exceeds the configured limit of {maximum} bytes")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("could not decode CBOR frame: {0}")]
    CborDecode(String),
    #[error("could not encode CBOR frame: {0}")]
    CborEncode(String),
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
}

pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, ProtocolError> {
    cbor2::to_vec(frame).map_err(|error| ProtocolError::CborEncode(error.to_string()))
}

pub fn decode_frame(encoded: &[u8], max_frame_bytes: usize) -> Result<Frame, ProtocolError> {
    check_frame_length(encoded.len(), max_frame_bytes)?;
    cbor2::validate_slice(encoded).map_err(|error| ProtocolError::CborDecode(error.to_string()))?;
    let frame: Frame =
        cbor2::from_slice(encoded).map_err(|error| ProtocolError::CborDecode(error.to_string()))?;
    frame.validate_version()?;
    Ok(frame)
}

pub fn encode_payload<T>(payload: &T) -> Result<Vec<u8>, ProtocolError>
where
    T: Serialize + ?Sized,
{
    cbor2::to_vec(payload).map_err(|error| ProtocolError::CborEncode(error.to_string()))
}

pub fn decode_payload<T>(payload: &[u8]) -> Result<T, ProtocolError>
where
    T: serde::de::DeserializeOwned,
{
    cbor2::validate_slice(payload).map_err(|error| ProtocolError::CborDecode(error.to_string()))?;
    cbor2::from_slice(payload).map_err(|error| ProtocolError::CborDecode(error.to_string()))
}

pub async fn read_frame<R>(reader: &mut R, max_frame_bytes: usize) -> Result<Frame, ProtocolError>
where
    R: AsyncRead + Unpin + Send + ?Sized,
{
    let frame: Frame = cbor2::async_io::tokio::read_value_with_limit(reader, max_frame_bytes)
        .await
        .map_err(|error| ProtocolError::CborDecode(error.to_string()))?;
    frame.validate_version()?;
    Ok(frame)
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin + Send + ?Sized,
{
    cbor2::async_io::tokio::write_value(writer, frame)
        .await
        .map_err(|error| ProtocolError::CborEncode(error.to_string()))
}

fn check_frame_length(frame_length: usize, max_frame_bytes: usize) -> Result<(), ProtocolError> {
    if frame_length > max_frame_bytes {
        return Err(ProtocolError::FrameTooLarge {
            actual: frame_length,
            maximum: max_frame_bytes,
        });
    }
    Ok(())
}
