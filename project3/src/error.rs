use std::{backtrace::Backtrace, fmt::Formatter, ops::Deref};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErrorCode {
    #[error("internel error: {0}")]
    InternalError(String),
    #[error(transparent)]
    NetworkError(#[from] std::io::Error),
    #[error("delete not exists key: {0}")]
    RmError(String),
}

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Error)]
#[error("{inner}")]
pub struct KvError {
    #[source]
    inner: Box<ErrorCode>,
    backtrace: Box<Backtrace>,
}

impl Deref for KvError {
    type Target = ErrorCode;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<ErrorCode> for KvError {
    fn from(value: ErrorCode) -> Self {
        KvError {
            inner: Box::new(value),
            backtrace: Box::new(Backtrace::capture()),
        }
    }
}

impl core::fmt::Debug for KvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}",
            self.inner,
            // Use inner error's backtrace by default, otherwise use the generated one in `From`.
            std::error::request_ref::<Backtrace>(&self.inner).unwrap_or(&*self.backtrace)
        )
    }
}

impl From<&str> for KvError {
    fn from(value: &str) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<std::env::VarError> for KvError {
    fn from(value: std::env::VarError) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<serde_json::error::Error> for KvError {
    fn from(value: serde_json::error::Error) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<std::str::Utf8Error> for KvError {
    fn from(value: std::str::Utf8Error) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<std::io::Error> for KvError {
    fn from(value: std::io::Error) -> Self {
        ErrorCode::NetworkError(value).into()
    }
}
