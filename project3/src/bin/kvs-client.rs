#![feature(let_chains)]

use std::net::IpAddr;
use std::net::TcpStream;
use std::process::exit;
use std::str::FromStr;

use clap::Parser;
use clap::Subcommand;
use kvs::common::Ipv4Port;
use kvs::error::Result;
use kvs::KvClient;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    cmd: Command,
    #[arg(long, global = true)]
    #[arg(default_value_t = Ipv4Port::default())]
    #[arg(value_parser = Ipv4Port::from_str)]
    addr: Ipv4Port,
}

#[derive(Subcommand, Clone)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
    Get { key: String },
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // begin connect
    let stream = TcpStream::connect((IpAddr::V4(opts.addr.ipv4), opts.addr.port))?;
    let mut client = KvClient { stream };
    match opts.cmd {
        Command::Get { key } => {
            client.get(key).map_or_else(
                |e| {
                    eprintln!("{}", e);
                    exit(1);
                },
                |res| {
                    res.map_or_else(|| println!("Key not found"), |x| println!("{}", x));
                },
            );
        }
        Command::Rm { key } => {
            client.rm(key).map_or_else(
                |e| {
                    eprintln!("{}", e);
                    exit(1);
                },
                |_| (),
            );
        }
        Command::Set { key, value } => {
            client.set(key, value).map_or_else(
                |e| {
                    eprintln!("{}", e);
                    exit(1);
                },
                |_| (),
            );
        }
    }
    Ok(())
}
