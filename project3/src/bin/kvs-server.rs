#![feature(let_chains)]

use std::{str::FromStr, net::{TcpListener, IpAddr, TcpStream, Shutdown}, io::{Read, BufRead, Write}, path::PathBuf, fs, fmt::Display};

use kvs::{cli::{Ipv4Port, Command, GetResponse, SetResponse, RmResponse}, kv::KvStore, error::{Result, ErrorCode}};
use kvs::cli::handle_send;
use kvs::cli::handle_receive;
use kvs::cli::KvsRequest;
use clap::{Parser, ValueEnum};
use tracing::{info, debug, error};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[arg(long)]
    #[arg(default_value_t = crate::Ipv4Port::default())]
    #[arg(value_parser = crate::Ipv4Port::from_str)]
    addr: Ipv4Port,
    #[arg(long)]
    #[arg(default_value_t)]
    #[arg(value_enum)]
    engine: KvEngine,
}

impl Display for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}, --addr {} --engine {}", 
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            self.addr,
            self.engine)
    }
}

#[derive(ValueEnum, Clone, Debug)]
enum KvEngine {
    KVS,
    SLED
}   

impl Default for KvEngine {
    fn default() -> Self {
        Self::KVS
    }
}

impl Display for KvEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::KVS => "kvs",
            Self::SLED => "sled",
        };
        write!(f, "{}", str)
    }
}

fn main() -> Result<()> {
    let cli = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    info!("Welcome to use {}:{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    info!("Backend engine: {}", cli.engine);
    info!("Listen on {}", cli.addr);
    let mut kv_store = KvStore::open(&std::env::current_dir()?)?;

    let listener = TcpListener::bind((IpAddr::V4(cli.addr.ipv4), cli.addr.port))?;
    // accept connections and process them serially
    for mut stream in listener.incoming() {
        if let Ok(ref mut stream) = stream {
            match handle_connection(&mut kv_store, stream) {
                Ok(_) => {
                    info!("connection closed!")
                }
                Err(e) => error!("connection occur exception: {}", e)
            }
        }
    }
    Ok(())
}

fn handle_connection(kv_store: &mut KvStore, stream: &mut TcpStream) -> Result<()> {
    info!("Connection connected! for {}", stream.peer_addr()?);
    while let KvsRequest::Cmd(cmd) = handle_receive(stream)? {
        match cmd {
            Command::Get { key } => {
                let res = GetResponse::Ok(kv_store.get(key)?);
                handle_send(stream, &res)?        
            }
            Command::Set { key, value } => {
                let res = SetResponse::Ok(kv_store.set(key, value)?);
                handle_send(stream, &res)?
            }
            Command::Rm { key } => {
                let rm_res = kv_store.remove(key);
                let res = if let Err(error) = &rm_res && let ErrorCode::RmError(_) = **error {
                    Ok(RmResponse::Err("Key not found".into()))
                } else if let Err(error) = rm_res {
                    Err(error)
                } else {
                    Ok(RmResponse::Ok(()))
                }?;
                handle_send(stream, &res)?
            }
        }
    }
    stream.shutdown(Shutdown::Both)?;
    Ok(())
}
