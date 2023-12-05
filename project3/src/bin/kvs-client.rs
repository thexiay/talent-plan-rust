#![feature(let_chains)]

use std::net::IpAddr;
use std::net::Shutdown;
use std::net::TcpStream;
use std::process::exit;
use std::str::FromStr;
use std::io::Write;

use kvs::error::Result;
use clap::Parser;
use kvs::KvClient;
use kvs::common::Command;
use kvs::common::Ipv4Port;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    cmd: Command,
    #[arg(long, global=true)]
    #[arg(default_value_t = Ipv4Port::default())]
    #[arg(value_parser = Ipv4Port::from_str)]
    addr: Ipv4Port,
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // begin connect
    let stream = TcpStream::connect((IpAddr::V4(opts.addr.ipv4), opts.addr.port))?;
    let mut client = KvClient{ stream };
    match opts.cmd {
        Command::Get{ key } => {
            client.get(key)
                .map_or_else(|e| {
                    eprintln!("{}", e);
                    exit(1);
                }, |res| {
                    res.map_or_else(
                        || println!("Key not found"), 
                        |x| println!("{}", x)
                    );
                });
        }
        Command::Rm{ key } => {
            client.rm(key)
                .map_or_else(|e| {
                    eprintln!("{}", e);
                    exit(1);
                }, |_| ());
        }   
        Command::Set{ key, value } => {
            client.set(key, value)
                .map_or_else(|e| {
                    eprintln!("{}", e);
                    exit(1);
                }, |_| ());
        }
    }
    Ok(())
}
