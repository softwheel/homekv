use std::net::SocketAddr;
use anyhow::Context;
use async_trait::async_trait;
use tracing::warn;
use crate::gossip::serialize::Serializable;

use super::message::GossipMessage;
use super::MTU;

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>>;
}

pub trait Socket: Send + Sync + 'static {
    async fn send(&mut self, to: SocketAddr, msg: GossipMessage) -> anyhow::Result<()>;
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, GossipMessage)>;
}

pub struct UdpTransport;

#[async_trait]
impl Transport for UdpTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = tokio::net::UdpSocket::bind(listen_addr)
            .await
            .with_context(|| format!("Failed to bind to {listen_addr}/UDP for gossip."))?;
        Ok(Box::new(UdpSocket {
            buf_send: Vec::with_capacity(MTU),
            buf_recv: Box::new([0u8; MTU]),
            socket,
        }))
    }
}

struct UdpSocket {
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; MTU]>,
    socket: tokio::net::UdpSocket,
}

#[async_trait]
impl Socket for UdpSocket {
    async fn send(&mut self, to: SocketAddr, msg: GossipMessage) -> anyhow::Result<()> {
        self.buf_send.clear();
        msg.serialize(&mut self.buf_send);
        self.send_bytes(to, &self.buf_send).await?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, GossipMessage)> {
        loop {
            if let Some(msg) = self.receive_one().await? {
                return Ok(msg);
            }
        }
    }
}

impl UdpSocket {
    async fn receive_one(&mut self) -> anyhow::Result<Option<(SocketAddr, GossipMessage)>> {
        let (len, from_addr) = self.socket.recv_from(&mut self.buf_recv[..])
            .await.context("Error while receiving UDP message")?;
        let mut buf = &self.buf_recv[..len];
        match GossipMessage::deserialize(&mut buf) {
            Ok(msg) => Ok(Some((from_addr, msg))),
            Err(err) => {
                warn!(payload_len=len, from=%from_addr, err=%err, "invalid-gossip-payload");
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(&self, to_addr: SocketAddr, payload: &[u8]) -> anyhow::Result<()> {
        self.socket.send_to(payload, to_addr)
            .await
            .context("Failed to send gossip message to target")?;
        Ok(())
    }
}