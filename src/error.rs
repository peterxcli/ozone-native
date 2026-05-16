use crate::proto::hadoop::{hdds::datanode, ozone};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc status: {0}")]
    RpcStatus(#[from] tonic::Status),
    #[error("prost encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),
    #[error("prost decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),
    #[error("OM returned {status:?}: {message}")]
    Om {
        status: ozone::Status,
        message: String,
    },
    #[error("datanode returned {result:?}: {message}")]
    Datanode {
        result: datanode::Result,
        message: String,
    },
    #[error("ratis request failed: {0}")]
    Ratis(String),
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;
