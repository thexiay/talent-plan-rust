use std::process::exit;

use clap::{Parser, Subcommand};
use kvs::KvStore;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    command: Commands,
    // 支持子命令: get ,set,remove
}

#[derive(Subcommand)]
enum Commands {
    Get { name: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() {
    // 构建app，能对kvs传入命令行解析
    let opts = Opts::parse();
    let _kv_store = KvStore::new();
    match opts.command {
        Commands::Get { name: _ } => {
            eprint!("unimplemented");
            exit(1);
        }
        Commands::Set { key: _, value: _ } => {
            eprint!("unimplemented");
            exit(1);
        }
        Commands::Rm { key: _ } => {
            eprint!("unimplemented");
            exit(1);
        }
    }
}
