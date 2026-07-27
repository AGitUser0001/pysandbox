use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Frame {
    pub version: u16,
    pub kind: FrameKind,
    pub worker_id: u64,
    pub request_id: u64,
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
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FrameKind {
    Execute,
    ExecuteResult,
    GuestCall,
    GuestResponse,
    WorkerCall,
    WorkerResponse,
    Output,
    UpdateLimits,
    Cancel,
    Shutdown,
    HealthCheck,
    HealthStatus,
    Error,
}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame, serde_cbor::Error> {
    serde_cbor::from_slice(bytes)
}

pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, serde_cbor::Error> {
    serde_cbor::to_vec(frame)
}
