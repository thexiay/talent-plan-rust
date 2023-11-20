use std::path::Path;
use std::process::exit;

use clap::{Parser, Subcommand};
use kvs::KvStore;
use kvs::Result;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    command: Commands,
    // 支持子命令: get ,set,remove
}

#[derive(Subcommand)]
enum Commands {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    // 构建app，能对kvs传入命令行解析
    let opts = Opts::parse();
    let mut kv_store = KvStore::open(&std::env::current_dir()?)?;
    match opts.command {
        Commands::Get { key } => {
            match kv_store.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found")
            }
            Ok(())
        }
        Commands::Set { key, value } => {
            kv_store.set(key, value)?;
            Ok(())
        }
        Commands::Rm { key} => {
            kv_store.remove(key)
        }
    }
}
