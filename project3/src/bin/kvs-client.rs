#![feature(let_chains)]

use std::net::IpAddr;
use std::net::Shutdown;
use std::net::TcpStream;
use std::process::exit;
use std::str::FromStr;
use std::io::Write;

use clap::Parser;
use kvs::cli::GetResponse;
use kvs::cli::KvsRequest;
use kvs::cli::RmResponse;
use kvs::cli::SetResponse;
use kvs::cli::handle_receive;
use kvs::error::Result;
use kvs::cli::Command;
use kvs::cli::handle_send;
use kvs::cli::Ipv4Port;
use tracing::error;

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
    let mut stream = TcpStream::connect((IpAddr::V4(opts.addr.ipv4), opts.addr.port))?;
    handle_send(&mut stream, &KvsRequest::Cmd(opts.cmd.clone()))?;

    match opts.cmd {
        Command::Get{ .. } => {
            if let Some(res) = handle_receive(&mut stream)? {
                match res {
                    GetResponse::Ok(Some(value)) => println!("{}", value),
                    GetResponse::Ok(None) => println!("Key not found!"),
                    GetResponse::Err(msg) => {
                        eprintln!("{}", msg);
                        exit(1)
                    }
                }
            }
        }
        Command::Rm{ .. } => {
            if let Some(res) = handle_receive(&mut stream)? {
                match res {
                    RmResponse::Ok(()) => (),
                    RmResponse::Err(msg) => {
                        eprintln!("{}", msg);
                        exit(1)
                    }
                }
            }
        }
        Command::Set{ .. } => {
            if let Some(res) = handle_receive(&mut stream)? {
                match res {
                    SetResponse::Ok(()) => (),
                    SetResponse::Err(msg) => {
                        eprintln!("{}", msg);
                        exit(1)
                    }
                }
            }
        }
    }
    handle_send(&mut stream, &KvsRequest::End)?;
    Ok(())
}
