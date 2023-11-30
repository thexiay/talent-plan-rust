
use std::net::{ToSocketAddrs, Shutdown, TcpStream, TcpListener};

use log::{info, error};

use crate::{Result, KvsEngine, common::{KvsRequest, handle_receive, Command, GetResponse, handle_send, RmResponse, SetResponse}};

pub struct KvServer<E> {
    engine: E
}


impl<E> KvServer<E> {
    pub fn new(engine: E) -> Self {
        KvServer{ engine }
    }
}

/// A Server provide network rpc service for kv database
impl<E: KvsEngine> KvServer<E> {
    pub fn serve_with_engine<Addr: ToSocketAddrs>(engine: E, addr: Addr) -> Result<()> {
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
        while let Some(KvsRequest::Cmd(cmd)) = handle_receive(stream)? {
            match cmd {
                Command::Get { key } => {
                    let res = self.engine
                        .get(key)
                        .map_or_else(|x| GetResponse::Err(x.to_string()), 
                            |x| GetResponse::Ok(x));
                    handle_send(stream, &res)?        
                }
                Command::Set { key, value } => {
                    let res = self.engine
                        .set(key, value)
                        .map_or_else(|x| SetResponse::Err(x.to_string()),
                            |_| SetResponse::Ok(()));
                    handle_send(stream, &res)?
                }
                Command::Rm { key } => {
                    let res = self.engine
                        .remove(key)
                        .map_or_else(
                            |x| RmResponse::Err(x.to_string()), 
                            |_| RmResponse::Ok(()));
                    handle_send(stream, &res)?
                }
            }
        }
        stream.shutdown(Shutdown::Both)?;
        Ok(())
    }
}