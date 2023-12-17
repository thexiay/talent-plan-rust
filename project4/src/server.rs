use std::{
    marker::PhantomData,
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{spawn, JoinHandle},
};

use crossbeam_channel::bounded;
use log::{debug, error, info, warn};

use crate::{
    common::{KvsRequest, KvsResponse, Service},
    error::ErrorCode,
    thread_pool::ThreadPool,
    KvClient, KvsEngine, Result,
};

impl<T: KvsEngine> Service<KvsRequest, KvsResponse> for T {
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

pub struct KvServer<E, P> {
    _phantom_e: PhantomData<E>,
    _phantom_p: PhantomData<P>,
}

/// A Server provide network rpc service for kv database
impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    pub fn serve(engine: E, thread_pool: P, addr: SocketAddr) -> Result<ThreadHandle> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let listener = TcpListener::bind(addr)?;

        let flag = stop_flag.clone();
        let join = spawn(move || Self::run(engine, thread_pool, listener, flag));
        Ok(ThreadHandle {
            join,
            stop_flag,
            addr,
        })
    }

    fn run(engine: E, thread_pool: P, listener: TcpListener, cond: Arc<AtomicBool>) {
        for stream in listener.incoming() {
            // check and stop this thread
            if cond.load(Ordering::SeqCst) {
                break;
            }
            let mut engine = engine.clone();
            thread_pool.spawn(move || match stream {
                Ok(mut stream) => {
                    if let Err(e) = handle_connection(&mut engine, &mut stream) {
                        error!("Error on serve client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }
    }
}

fn handle_connection<E: KvsEngine>(engine: &mut E, stream: &mut TcpStream) -> Result<()> {
    let peer = stream.peer_addr()?;
    debug!("Connection for {} connected!", peer);
    while engine.response(stream)? {}
    stream.shutdown(Shutdown::Both)?;
    debug!("Connection for {} close!", peer);
    Ok(())
}

pub struct ThreadHandle {
    // a handler to wait unit KvServer to finished
    join: JoinHandle<()>,

    // a flag to stop this thread
    stop_flag: Arc<AtomicBool>,

    // a server addr for fake connect to stop it.
    addr: SocketAddr,
}

impl ThreadHandle {
    pub fn shutdown(self) -> Result<()> {
        // send message close and connect once dummy
        if let Ok(_) =
            self.stop_flag
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            info!("close this kvserver.");
            TcpStream::connect(self.addr)?;
        };
        warn!("This kv server may have been closed.");
        Ok(())
    }

    pub fn join(self) -> Result<()> {
        match self.join.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorCode::InternalError("join thread failed".to_string()).into()),
        }
    }
}
