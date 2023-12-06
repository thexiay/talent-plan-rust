use std::{
    fmt::Display,
    io::{Read, Write},
    net::{Ipv4Addr, TcpStream},
    str::FromStr,
};

use log::warn;
use serde_derive::{Deserialize, Serialize};

use crate::error::ErrorCode;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Ipv4Port {
    pub ipv4: Ipv4Addr,
    pub port: u16,
}

impl Default for Ipv4Port {
    fn default() -> Self {
        Self {
            ipv4: Ipv4Addr::new(127, 0, 0, 1),
            port: 4000,
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

    fn from_str(s: &str) -> core::result::Result<Self, anyhow::Error> {
        match s.split_once(':') {
            Some((host, port_str)) => {
                let ipv4 = host.parse::<Ipv4Addr>()?;
                let port = port_str.parse::<u16>()?;
                Ok(Ipv4Port { ipv4, port })
            }
            None => {
                let ipv4 = s.parse::<Ipv4Addr>()?;
                Ok(Ipv4Port { ipv4, port: 4040 })
            }
        }
    }
}

// todo: 自动映射
#[derive(Serialize, Deserialize)]
pub enum KvsRequest {
    Set { key: String, value: String },
    Rm { key: String },
    Get { key: String },
}

// todo: 自动映射
#[derive(Serialize, Deserialize, Debug)]
pub enum KvsResponse {
    Set(core::result::Result<(), String>),
    Rm(core::result::Result<(), String>),
    Get(core::result::Result<Option<String>, String>),
}

pub trait Service<Req, Res>
where
    Req: serde::ser::Serialize + serde::de::DeserializeOwned,
    Res: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    fn handle(&mut self, req: Req) -> Res;

    /// This is for Server
    fn response(&mut self, stream: &mut TcpStream) -> Result<bool> {
        handle_receive::<Req>(stream)?.map_or(Ok(false), |req| {
            handle_send(stream, &(self.handle(req)))?;
            Ok(true)
        })
    }
}

pub trait ServiceProxy<Req, Res>
where
    Req: serde::ser::Serialize + serde::de::DeserializeOwned,
    Res: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    /// This is for client
    fn request(stream: &mut TcpStream, req: &Req) -> Result<Res> {
        handle_send(stream, req)?;
        handle_receive::<Res>(stream)?.ok_or(
            ErrorCode::NetworkError(std::io::Error::from(std::io::ErrorKind::ConnectionAborted))
                .into(),
        )
    }
}

pub fn handle_send<T>(stream: &mut TcpStream, value: &T) -> crate::error::Result<()>
where
    T: serde::ser::Serialize,
{
    let b_value = serde_json::to_vec(&value)?;
    if b_value.len() > u16::MAX as usize {
        return Err(ErrorCode::InternalError("valid len for send".to_string()).into());
    }

    stream.write_all(&(b_value.len() as u16).to_be_bytes())?;
    stream.write_all(&b_value)?;
    Ok(())
}

pub fn handle_receive<T>(stream: &mut TcpStream) -> crate::error::Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    let mut b_len = [0_u8; 2];
    match stream.read(&mut b_len) {
        Err(e) => return Err(e.into()),
        Ok(0) => {
            warn!("Another side close socket");
            return Ok(None);
        }
        _ => (),
    }

    let cmd = serde_json::from_reader(stream.take(u16::from_be_bytes(b_len) as u64))?;
    Ok(cmd)
}
