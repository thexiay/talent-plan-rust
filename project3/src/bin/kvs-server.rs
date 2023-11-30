#![feature(let_chains)]

use std::{str::FromStr, net::{TcpListener, IpAddr, TcpStream, Shutdown, ToSocketAddrs}, io::{Read, BufRead, Write}, path::{PathBuf, Path}, fs::{self, OpenOptions}, fmt::Display, env::current_dir, process::exit};

use kvs::{common::{Ipv4Port, Command, GetResponse, SetResponse, RmResponse}, KvStore, error::{Result, ErrorCode}, KvsEngine, SledStore, KvServer};
use kvs::common::handle_send;
use kvs::common::handle_receive;
use kvs::common::KvsRequest;
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
        let addr = (cli.addr.ipv4, cli.addr.port);
        match cli.engine {
            Engine::KVS => KvServer::serve_with_engine(KvStore::open(&path)?, addr),
            Engine::SLED => KvServer::serve_with_engine(SledStore::open(&path)?, addr),
        }
    });

    if let Err(e) = res {
        error!("{}", e);
        exit(1)
    }
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
