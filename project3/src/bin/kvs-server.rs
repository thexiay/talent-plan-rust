#![feature(let_chains)]

use std::{str::FromStr, net::{TcpListener, IpAddr, TcpStream, Shutdown}, io::{Read, BufRead, Write}, path::PathBuf, fs};

use kvs::{cli::{Ipv4Port, Command, KvsResponse}, kv::KvStore, error::{Result, ErrorCode}};
use kvs::cli::handle_send;
use kvs::cli::handle_receive;
use kvs::cli::KvsRequest;
use clap::{Parser, ValueEnum};
use tracing::{info, debug};

#[derive(Parser)]
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

#[derive(ValueEnum, Clone)]
enum KvEngine {
    KVS,
    SLED
}   

impl Default for KvEngine {
    fn default() -> Self {
        Self::KVS
    }
}

fn main() -> Result<()> {
    let cli = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let kvs_home = PathBuf::from_str(&std::env::var("HOME")?).unwrap().join(".kvs");
    info!("Open kv store at {:#?}", kvs_home);
    let mut kv_store = KvStore::open(kvs_home.as_path())?;

    // begin listen
    let listener = TcpListener::bind((IpAddr::V4(cli.addr.ipv4), cli.addr.port))?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        handle_connection(&mut kv_store, &mut stream?)?;
    }
    Ok(())
}

fn handle_connection(kv_store: &mut KvStore, stream: &mut TcpStream) -> Result<()> {
    info!("Connection connected! for {}", stream.peer_addr()?);
    while let KvsRequest::Cmd(cmd) = handle_receive(stream)? {
        handle_send(stream, &handle_command(kv_store, cmd)?)?;
    }
    info!("connection closed");
    stream.shutdown(Shutdown::Both)?;
    Ok(())
}

fn handle_command(kv_store: &mut KvStore, cmd: Command) -> Result<KvsResponse> {
    match cmd {
        Command::Get { key } => {
            match kv_store.get(key)? {
                Some(value) => Ok(KvsResponse::Normal(value)),
                None => Ok(KvsResponse::Warning("Key not found".into())),
            }
        }
        Command::Set { key, value } => {
            kv_store.set(key, value)?;
            Ok(KvsResponse::Normal("".into()))
        }
        Command::Rm { key } => {
            let res = kv_store.remove(key);
            if let Err(error) = &res && let ErrorCode::RmError(_) = **error {
                Ok(KvsResponse::Exception("Key not found".into()))
            } else if let Err(error) = res {
                Err(error)
            } else {
                Ok(KvsResponse::Normal("".into()))
            }
        }
    }
}