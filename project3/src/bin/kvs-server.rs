use std::str::FromStr;
use kvs::cli::Ipv4Port;
use clap::{Parser, ValueEnum};


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

fn main() {
    let cli = Opts::parse();

    println!("stub here");
}