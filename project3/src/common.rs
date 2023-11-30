use std::{net::{Ipv4Addr, TcpStream}, fmt::Display, str::FromStr, io::{Read, Write}};

use clap::Subcommand;
use serde_derive::{Serialize, Deserialize};
use tracing::{debug, warn};

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
    let b_value = serde_json::to_vec(&value)?;
    if b_value.len() > u16::MAX as usize {
        return Err(ErrorCode::InternalError(format!("valid len for send")).into())
    }
    
    stream.write(&(b_value.len() as u16).to_be_bytes())?;
    stream.write(&b_value)?;
    Ok(())
}

pub fn handle_receive<T>(stream: &mut TcpStream) -> crate::error::Result<Option<T>>
    where T: serde::de::DeserializeOwned
{
    debug!("handle receive");
    let mut b_len = [0_u8; 2];
    debug!("handle receive1");
    match stream.read(&mut b_len) {
        Err(e) => return Err(e.into()),
        Ok(len) if len == 0 => {
            warn!("Another side close socket");
            return Ok(None);
        }
        _ => ()
    }
    
    let cmd = serde_json::from_reader(stream.take(u16::from_be_bytes(b_len) as u64))?;
    Ok(cmd)
}