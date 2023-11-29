use std::{net::{Ipv4Addr, TcpStream}, fmt::Display, str::FromStr, io::{Read, Write}};

use clap::Subcommand;
use serde_derive::{Serialize, Deserialize};
use tracing::debug;

use crate::error::ErrorCode;

#[derive(Serialize, Deserialize, Subcommand, Clone)]
pub enum Command {
    Set { key: String, value: String },
    Rm { key: String },
    Get { key: String},
}

#[derive(Clone, Debug)]
pub struct Ipv4Port {
    pub ipv4: Ipv4Addr,
    pub port: u16,
}


impl Default for Ipv4Port {
    fn default() -> Self {
        Self { 
            ipv4: Ipv4Addr::new(127, 0, 0, 1), 
            port: 4000
        }
    }
}

impl Display for Ipv4Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.ipv4, self.port)
    }
}

impl FromStr for Ipv4Port {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        match s.split_once(':') {
            Some((host, port_str)) => {
                let ipv4 = host.parse::<Ipv4Addr>()?;
                let port = port_str.parse::<u16>()?;
                Ok(Ipv4Port {
                    ipv4,
                    port,
                })
            }
            None =>  {
                let ipv4 = s.parse::<Ipv4Addr>()?;
                Ok(Ipv4Port{
                    ipv4,
                    port: 4040
                })
            }
        }
    }
}

/// here is the kvs protocol message
/// 
#[derive(Serialize, Deserialize)]
pub enum KvsRequest {
    Cmd(Command),
    End,
}

#[derive(Serialize, Deserialize)]
pub enum SetResponse {
    Ok(()),
    Err(String)
}

#[derive(Serialize, Deserialize)]
pub enum RmResponse {
    Ok(()),
    Err(String)
}

#[derive(Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String)
}

pub fn handle_send<T>(stream: &mut TcpStream, value: &T) -> crate::error::Result<()> 
    where T: serde::ser::Serialize
{
    let sered_message = serde_json::to_vec(&value)?;
    if sered_message.len() > u16::MAX as usize {
        return Err(ErrorCode::InternalError(format!("valid len for send")).into())
    }
    
    stream.write(&(sered_message.len() as u16).to_be_bytes())?;
    stream.write(&sered_message)?;
    Ok(())
}

pub fn handle_receive<T>(stream: &mut TcpStream) -> crate::error::Result<T>
    where T: serde::de::DeserializeOwned
{
    let mut buf = Vec::<u8>::new();
    let mut len_stream = stream.take(std::mem::size_of::<u16>() as u64);
    len_stream.read_to_end(&mut buf)?;
    if buf.len() != std::mem::size_of::<u16>() {
        return Err(ErrorCode::InternalError("invalid len for socket receive".into()).into())    
    }
    
    let cmd_len =  (buf[0] as u16) << 8 | (buf[1] as u16);
    buf.clear();

    let value_stream = stream.take(cmd_len as u64);
    let cmd = serde_json::from_reader(value_stream)?;
    Ok(cmd)
}