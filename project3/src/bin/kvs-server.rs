#![feature(let_chains)]

use std::{str::FromStr, net::{TcpListener, IpAddr, TcpStream, Shutdown}, io::{Read, BufRead, Write}, path::{PathBuf, Path}, fs::{self, OpenOptions}, fmt::Display};

use kvs::{cli::{Ipv4Port, Command, GetResponse, SetResponse, RmResponse}, KvStore, error::{Result, ErrorCode}, KvsEngine};
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
    engine: Engine,
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
enum Engine {
    KVS,
    SLED
}   

impl Default for Engine {
    fn default() -> Self {
        Self::KVS
    }
}

impl Display for Engine {
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
    let path = std::env::current_dir()?;
    check(&path, cli.engine.to_string())?;
    let mut kv_store = KvStore::open(&path)?;

    let listener = TcpListener::bind((IpAddr::V4(cli.addr.ipv4), cli.addr.port))?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(e) = handle_connection(&mut kv_store, &mut stream) {
                    error!("Error on serve client: {}", e);
                }
            }
            Err(e) => error!("Connection failed: {}", e),
        }
    }
    Ok(())
}

fn handle_connection(kv_store: &mut KvStore, stream: &mut TcpStream) -> Result<()> {
    info!("Connection connected! for {}", stream.peer_addr()?);
    while let Some(KvsRequest::Cmd(cmd)) = handle_receive(stream)? {
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

fn check(path: &Path, kv_type: String) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let flag_file = path.join(".kvs");
    if let Ok(meta) = fs::metadata(flag_file.clone()) && meta.is_file() {
        let mut file = OpenOptions::new()
            .read(true)
            .open(flag_file.clone())?;
        let mut flag = String::new();
        file.read_to_string(&mut flag)?;
        if flag != kv_type {
            return Err(ErrorCode::InternalError(format!("Illegal kvs database {} start", flag)).into());
        } else {
            return Ok(());
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(flag_file)?;
    file.write_fmt(format_args!("{}", kv_type))?;
    Ok(())
}