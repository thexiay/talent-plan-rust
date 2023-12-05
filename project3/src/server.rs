
use std::net::{ToSocketAddrs, Shutdown, TcpStream, TcpListener};

use log::{info, error};

use crate::{Result, KvsEngine, common::{KvsRequest, handle_receive, Command, handle_send, KvsResponse, Service}};

pub struct KvServer<E> {
    engine: E,
}

impl<E: KvsEngine> Service<KvsRequest, KvsResponse> for KvServer<E> {
    fn handle(&mut self, req: KvsRequest) -> KvsResponse {
        match req {
            KvsRequest::Get { key } => {
                self.engine
                    .get(key)
                    .map_or_else(|x| KvsResponse::Get(Err(x.to_string())), 
                        |x| KvsResponse::Get(Ok(x)))      
            }
            KvsRequest::Set { key, value } => {
                self.engine
                    .set(key, value)
                    .map_or_else(|x| KvsResponse::Set(Err(x.to_string())),
                        |_| KvsResponse::Set(Ok(())))
            }
            KvsRequest::Rm { key } => {
                self.engine
                    .remove(key)
                    .map_or_else(
                        |x| KvsResponse::Rm(Err(x.to_string())), 
                        |_| KvsResponse::Rm(Ok(())))
            }
        }
    }
}
/// A Server provide network rpc service for kv database
impl<E: KvsEngine> KvServer<E> {
    pub fn new(engine: E) -> Self {
        KvServer{ 
            engine,
        }
    }

    pub fn serve_with_engine<Addr: ToSocketAddrs>(
        engine: E, 
        addr: Addr
    ) -> Result<()> {
        let mut server = KvServer::new(engine);
        server.serve(addr)
    }

    pub fn serve<Addr: ToSocketAddrs>(&mut self, addr: Addr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    if let Err(e) = self.handle_connection(&mut stream) {
                        error!("Error on serve client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn handle_connection(&mut self, stream: &mut TcpStream) -> Result<()>
    {
        info!("Connection connected! for {}", stream.peer_addr()?);
        while self.response(stream)? {}
        stream.shutdown(Shutdown::Both)?;
        Ok(())
    }
}