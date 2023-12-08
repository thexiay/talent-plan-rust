use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};

use log::{error, info};

use crate::{
    common::{KvsRequest, KvsResponse, Service},
    KvsEngine, Result, thread_pool::ThreadPool,
};

pub struct KvServer<E, P> {
    engine: E,
    thread_pool: P,
}

impl<T:KvsEngine> Service<KvsRequest, KvsResponse> for T {
    fn handle(&mut self, req: KvsRequest) -> KvsResponse {
        match req {
            KvsRequest::Get { key } => self.get(key).map_or_else(
                |x| KvsResponse::Get(Err(x.to_string())),
                |x| KvsResponse::Get(Ok(x)),
            ),
            KvsRequest::Set { key, value } => self.set(key, value).map_or_else(
                |x| KvsResponse::Set(Err(x.to_string())),
                |_| KvsResponse::Set(Ok(())),
            ),
            KvsRequest::Rm { key } => self.remove(key).map_or_else(
                |x| KvsResponse::Rm(Err(x.to_string())),
                |_| KvsResponse::Rm(Ok(())),
            ),
        }
    }
}

/// A Server provide network rpc service for kv database
impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    pub fn new(engine: E, thread_pool: P) -> Self {
        KvServer { engine, thread_pool}
    }

    pub fn serve_with_engine<Addr: ToSocketAddrs>(engine: E, thread_pool: P, addr: Addr) -> Result<()> {
        let server = KvServer::new(engine, thread_pool);
        server.serve(addr)
    }

    pub fn serve<Addr: ToSocketAddrs>(self, addr: Addr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        // accept connections and process them serially
        for stream in listener.incoming() {
            let mut engine = self.engine.clone();
            self.thread_pool.spawn(move || {
                match stream {
                    Ok(mut stream) => {
                        if let Err(e) = handle_connection(&mut engine, &mut stream) {
                            error!("Error on serve client: {}", e);
                        }
                    }
                    Err(e) => error!("Connection failed: {}", e),
                }
            })
            
        }
        Ok(())
    }
}

fn handle_connection<E: KvsEngine>(engine: &mut E, stream: &mut TcpStream) -> Result<()> {
    let peer = stream.peer_addr()?;
    info!("Connection for {} connected!", peer);
    while engine.response(stream)? {}
    stream.shutdown(Shutdown::Both)?;
    info!("Connection for {} close!", peer);
    Ok(())
}
