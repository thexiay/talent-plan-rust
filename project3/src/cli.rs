use std::{net::Ipv4Addr, fmt::Display, str::FromStr};

use anyhow::Error;

#[derive(Clone)]
pub struct Ipv4Port {
    ipv4: Ipv4Addr,
    port: u16,
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
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