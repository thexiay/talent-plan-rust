#![feature(let_chains)]

use std::{str::FromStr, net::{TcpListener, IpAddr, TcpStream, Shutdown, ToSocketAddrs}, io::{Read, BufRead, Write}, path::{PathBuf, Path}, fs::{self, OpenOptions}, fmt::Display, env::current_dir, process::exit};

use kvs::{cli::{Ipv4Port, Command, GetResponse, SetResponse, RmResponse}, KvStore, error::{Result, ErrorCode}, KvsEngine, SledStore};
use kvs::cli::handle_send;
use kvs::cli::handle_receive;
use kvs::cli::KvsRequest;
use clap::{Parser, ValueEnum};
use log::warn;
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

#[derive(ValueEnum, Clone, Debug, PartialEq, PartialOrd)]
enum Engine {
    KVS,
    SLED
}   

impl Default for Engine {
    fn default() -> Self {
        Self::KVS
    }
}

impl FromStr for Engine {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::KVS),
            "sled" => Ok(Engine::SLED),
            _ => Err(ErrorCode::InternalError(format!("error transfor from {}", s)))
        }
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            Engine::KVS => "kvs",
            Engine::SLED => "sled",
        })
    }
}

fn main() {
    let cli = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    info!("Welcome to use {}:{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    info!("Backend engine: {}", cli.engine);
    info!("Listen on {}", cli.addr);
    let res = current_engine().and_then(move |curr_engine| {
        if let Some(curr_engine) = curr_engine && cli.engine != curr_engine {
            error!("wrong engine!");
            exit(1)
        }

        let path = std::env::current_dir()?;
        fs::write(path.join(".engine"), format!("{}", cli.engine))?;
        match cli.engine {
            Engine::KVS => run(KvStore::open(&path)?, (IpAddr::V4(cli.addr.ipv4), cli.addr.port)),
            Engine::SLED => run(SledStore::open(&path)?, (IpAddr::V4(cli.addr.ipv4), cli.addr.port)),
            
        }
    });

    if let Err(e) = res {
        error!("{}", e);
        exit(1)
    }
}

fn run<E, Addr>(mut engine: E, addr: Addr) -> Result<()>
    where E: KvsEngine, Addr: ToSocketAddrs
{
    let listener = TcpListener::bind(addr)?;
    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(e) = handle_connection(&mut engine, &mut stream) {
                    error!("Error on serve client: {}", e);
                }
            }
            Err(e) => error!("Connection failed: {}", e),
        }
    }
    Ok(())
}

fn handle_connection<T>(kv_store: &mut T, stream: &mut TcpStream) -> Result<()>
    where T: KvsEngine    
{
    info!("Connection connected! for {}", stream.peer_addr()?);
    while let Some(KvsRequest::Cmd(cmd)) = handle_receive(stream)? {
        match cmd {
            Command::Get { key } => {
                let res = kv_store
                    .get(key)
                    .map_or_else(|x| GetResponse::Err(x.to_string()), 
                        |x| GetResponse::Ok(x));
                handle_send(stream, &res)?        
            }
            Command::Set { key, value } => {
                let res = kv_store
                    .set(key, value)
                    .map_or_else(|x| SetResponse::Err(x.to_string()),
                        |_| SetResponse::Ok(()));
                handle_send(stream, &res)?
            }
            Command::Rm { key } => {
                let res = kv_store
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

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join(".engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
