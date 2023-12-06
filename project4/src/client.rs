use std::net::{Shutdown, TcpStream, ToSocketAddrs};

use crate::common::KvsRequest;
use crate::common::KvsResponse;
use crate::common::ServiceProxy;
use crate::{error::ErrorCode, Result};

pub struct KvClient {
    pub stream: TcpStream,
}

// todo: KvClient和proxy简化成一个类
impl ServiceProxy<KvsRequest, KvsResponse> for KvClient {}

impl KvClient {
    pub fn new<Addr: ToSocketAddrs>(addr: Addr) -> Result<KvClient> {
        Ok(KvClient {
            stream: TcpStream::connect(addr)?,
        })
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown(Shutdown::Both)?;
        Ok(())
    }

    // 模版代码，装包解包，其实是KvServerProxy，可以通过宏自动生成
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Self::request(&mut self.stream, &KvsRequest::Set { key, value });
        match request {
            Ok(KvsResponse::Set(Ok(res))) => Ok(res),
            Ok(KvsResponse::Set(Err(fn_err))) => Err(ErrorCode::InternalError(fn_err).into()),
            Ok(msg) => panic!("invalid return type! {:#?}", msg),
            Err(rpc_err) => Err(ErrorCode::InternalError(rpc_err.to_string()).into()),
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Self::request(&mut self.stream, &KvsRequest::Get { key });
        match request {
            Ok(KvsResponse::Get(Ok(res))) => Ok(res),
            Ok(KvsResponse::Get(Err(fn_err))) => Err(ErrorCode::InternalError(fn_err).into()),
            Ok(msg) => panic!("invalid return type! {:#?}", msg),
            Err(rpc_err) => Err(ErrorCode::InternalError(rpc_err.to_string()).into()),
        }
    }

    pub fn rm(&mut self, key: String) -> Result<()> {
        let request = Self::request(&mut self.stream, &KvsRequest::Rm { key });
        match request {
            Ok(KvsResponse::Rm(Ok(res))) => Ok(res),
            Ok(KvsResponse::Rm(Err(fn_err))) => Err(ErrorCode::InternalError(fn_err).into()),
            Ok(msg) => panic!("invalid return type! {:#?}", msg),
            Err(rpc_err) => Err(ErrorCode::InternalError(rpc_err.to_string()).into()),
        }
    }
}
