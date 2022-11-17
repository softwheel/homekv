use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use serde::Serialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use anyhow::{bail, Context};

use super::node::Node;


/// Trait for serializing messages.
///
/// HomeTalk uses a custom binary serialization format.
/// The point of this format is to make it possible to truncate
/// the delta payload to a given mtu.
pub trait Serializable: Sized {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self>;
    fn serialized_len(&self) -> usize;
}

impl Serializable for u16 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let _ = self.to_le_bytes().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u16_bytes: [u8; 2] = Serializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u16_bytes))
    }

    fn serialized_len(&self) -> usize {
        2
    }
}

impl Serializable for u64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let _ = self.to_le_bytes().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u64_bytes: [u8; 8] = Serializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u64_bytes))
    }

    fn serialized_len(&self) -> usize {
        8
    }
}

#[repr(u8)]
enum IpVersion {
    V4 = 4u8,
    V6 = 6u8,
}

impl TryFrom<u8> for IpVersion {
    type Error = anyhow::Error;

    fn try_from(ip_type_byte: u8) -> anyhow::Result<Self> {
        if ip_type_byte == IpVersion::V4 as u8 {
            Ok(IpVersion::V4)
        } else if ip_type_byte == IpVersion::V6 as u8 {
            Ok(IpVersion::V6)
        } else {
            bail!("Invalid ip version byte. Expected 4 or 6 but got {ip_type_byte}");
        }
    }
}

impl Serializable for IpAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IpAddr::V4(ip_v4) => {
                buf.push(IpVersion::V4 as u8);
                buf.extend_from_slice(&ip_v4.octets())
            }
            IpAddr::V6(ip_v6) => {
                buf.push(IpVersion::V6 as u8);
                buf.extend_from_slice(&ip_v6.octets());
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_version_byte = buf.first().cloned()
            .context("Failed to deserialize IpAddr: empty buffer.")?;
        let ip_version = IpVersion::try_from(ip_version_byte)?;
        buf.consume(1);
        match ip_version {
            IpVersion::V4 => {
                let bytes: [u8; 4] = Serializable::deserialize(buf)?;
                Ok(Ipv4Addr::from(bytes).into())
            }
            IpVersion::V6 => {
                let bytes: [u8; 16] = Serializable::deserialize(buf)?;
                Ok(Ipv6Addr::from(bytes).into())
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            IpAddr::V4(_) => 4,
            IpAddr::V6(_) => 16,
        }
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let _ = (self.len() as u16).serialize(buf);
        buf.extend(self.as_bytes())
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let len: usize = u16::deserialize(buf)? as usize;
        let s = std::str::from_utf8(&buf[..len])?.to_string();
        buf.consume(len as usize);
        Ok(s)
    }

    fn serialized_len(&self) -> usize {
        2 + self.len()
    }
}

impl<const N: usize> Serializable for [u8; N] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self[..]);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < N {
            bail!("Buffer too short");
        }
        let val_bytes: [u8; N] = buf[..N].try_into()?;
        buf.consume(N);
        Ok(val_bytes)
    }

    fn serialized_len(&self) -> usize {
        N
    }
}

impl Serializable for SocketAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let _ = self.ip().serialize(buf);
        let _ = self.port().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_addr = IpAddr::deserialize(buf)?;
        let port = u16::deserialize(buf)?;
        Ok(SocketAddr::new(ip_addr, port))
    }

    fn serialized_len(&self) -> usize {
        todo!()
    }
}