#![feature(let_chains)]

use std::net::IpAddr;
use std::net::Shutdown;
use std::net::TcpStream;
use std::process::exit;
use std::str::FromStr;
use std::io::Write;

use clap::Parser;
use kvs::cli::KvsResponse;
use kvs::cli::KvsRequest;
use kvs::cli::handle_receive;
use kvs::error::Result;
use kvs::cli::Command;
use kvs::cli::handle_send;
use kvs::cli::Ipv4Port;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[command(subcommand)]
    cmd: Command,
    #[arg(long)]
    #[arg(default_value_t = Ipv4Port::default())]
    #[arg(value_parser = Ipv4Port::from_str)]
    addr: Ipv4Port,
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // begin connect
    let mut stream = TcpStream::connect((IpAddr::V4(opts.addr.ipv4), opts.addr.port))?;
    handle_send(&mut stream, &KvsRequest::Cmd(opts.cmd))?;
    match handle_receive(&mut stream)? {
        KvsResponse::Normal(value) => println!("{}", value),
        KvsResponse::Warning(warn) => eprintln!("{}", warn),
        KvsResponse::Exception(exception) | KvsResponse::Error(exception) => {
            eprintln!("{}", exception);
            exit(1)
        }
    }
    handle_send(&mut stream, &KvsRequest::End)?;
    Ok(())
}
