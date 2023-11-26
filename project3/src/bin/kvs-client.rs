#![feature(let_chains)]

use std::str::FromStr;

use clap::{Parser, Subcommand};
use kvs::error::ErrorCode;
use kvs::error::Result;
use kvs::kv::KvStore;
use kvs::cli::Ipv4Port;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    command: Commands,
    #[arg(long)]
    #[arg(default_value_t = Ipv4Port::default())]
    #[arg(value_parser = Ipv4Port::from_str)]
    addr: Ipv4Port,
}

#[derive(Subcommand)]
enum Commands {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    
    let mut kv_store = KvStore::open(&std::env::current_dir()?)?;
    match opts.command {
        Commands::Get { key } => {
            match kv_store.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
            Ok(())
        }
        Commands::Set { key, value } => {
            kv_store.set(key, value)?;
            Ok(())
        }
        Commands::Rm { key } => {
            let res = kv_store.remove(key);
            if let Err(error) = &res && let ErrorCode::RmError(_) = **error {
                println!("Key not found"); 
            }
            res
        }
    }
}
