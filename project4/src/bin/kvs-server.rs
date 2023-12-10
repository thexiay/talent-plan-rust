#![feature(let_chains)]

use std::{
    env::current_dir,
    fmt::Display,
    fs::{self},
    process::exit,
    str::FromStr, net::SocketAddr,
};

use clap::{Parser, ValueEnum};
use kvs::{
    common::Ipv4Port,
    error::{ErrorCode, Result},
    KvServer, KvStore, KvsEngine, SledStore, thread_pool::{SharedQueueThreadPool, ThreadPool},
};
use log::warn;
use tracing::{error, info};

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
        write!(
            f,
            "{}:{}, --addr {} --engine {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            self.addr,
            self.engine
        )
    }
}

#[derive(ValueEnum, Clone, Debug, PartialEq, PartialOrd)]
enum Engine {
    Kvs,
    Sled,
}

impl Default for Engine {
    fn default() -> Self {
        Self::Kvs
    }
}

impl FromStr for Engine {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            _ => Err(ErrorCode::InternalError(format!(
                "error transfor from {}",
                s
            ))),
        }
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Engine::Kvs => "kvs",
                Engine::Sled => "sled",
            }
        )
    }
}

fn main() {
    let cli = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    info!(
        "Welcome to use {}:{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    info!("Backend engine: {}", cli.engine);
    info!("Listen on {}", cli.addr);
    let res = current_engine().and_then(move |curr_engine| {
        if let Some(curr_engine) = curr_engine && cli.engine != curr_engine {
            error!("wrong engine!");
            exit(1)
        }

        let path = std::env::current_dir()?;
        fs::write(path.join(".engine"), format!("{}", cli.engine))?;
        let pool = SharedQueueThreadPool::new(10)?;
        let addr: SocketAddr = (cli.addr.ipv4, cli.addr.port).into();
        match cli.engine {
            Engine::Kvs => KvServer::serve(KvStore::open(&path)?, pool, addr),
            Engine::Sled => KvServer::serve(SledStore::open(&path)?, pool, addr),
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
