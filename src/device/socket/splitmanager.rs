//! Split connection manager for concurrent TX/RX access without locks.
//!
//! This module provides [`SplitConnectionManagerTx`] and [`SplitConnectionManagerRx`] which
//! allow sending and receiving vsock data concurrently from separate threads without any mutex.
//!
//! # Architecture
//!
//! - Each VirtQueue is exclusively owned by one half (`&mut self`), so no lock is needed.
//! - Per-connection state uses atomics and a lock-free SPSC ring buffer.
//! - `poll()` returns [`TxAction`]s instead of sending control responses inline, so the RX side
//!   never touches the TX queue.
//! - `recv()` reads directly from the per-connection SPSC ring with no lock.

use super::error::SocketError;
use super::protocol::{VsockAddr, VMADDR_CID_HOST};
use super::spsc::SpscRingBuffer;
use super::vsock::{
    ConnectionInfo, DisconnectReason, VirtIOSocketDeviceRx, VirtIOSocketDeviceTx, VirtIOSocketRx,
    VirtIOSocketTx, VsockEvent, VsockEventType, VsockTx,
};
use super::DEFAULT_RX_BUFFER_SIZE;
use crate::transport::{DeviceTransport, Transport};
use crate::{DeviceHal, Hal, Result};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use log::debug;

const DEFAULT_PER_CONNECTION_BUFFER_CAPACITY: u32 = 1024;

/// An action that the TX side must perform in response to an event received by the RX side.
///
/// Returned by [`SplitConnectionManagerRx::poll`]. The caller should pass these to
/// [`SplitManagerTx::execute`].
#[derive(Clone, Debug)]
pub enum TxAction {
    /// Accept an incoming connection.
    Accept(ConnectionInfo),
    /// Reject an incoming connection.
    Reject(ConnectionInfo),
    /// Send a credit update to the peer.
    CreditUpdate(ConnectionInfo),
    /// Forcibly close a connection.
    ForceClose(ConnectionInfo),
}

/// Per-connection state shared between the TX and RX halves via atomics and a SPSC ring.
pub struct SharedConnection {
    /// The peer address (immutable after creation).
    pub dst: VsockAddr,
    /// The local port (immutable after creation).
    pub src_port: u32,
    /// The peer's buffer allocation.
    peer_buf_alloc: AtomicU32,
    /// The peer's forwarded count.
    peer_fwd_cnt: AtomicU32,
    /// The number of bytes we have sent.
    tx_cnt: AtomicU32,
    /// Our buffer allocation.
    buf_alloc: AtomicU32,
    /// The number of bytes we have forwarded.
    fwd_cnt: AtomicU32,
    /// Whether we have a pending credit request.
    has_pending_credit_request: AtomicBool,
    /// Whether the peer requested shutdown but we still have buffered data.
    peer_requested_shutdown: AtomicBool,
    /// Lock-free ring buffer for received data. Producer: RX/poll side. Consumer: recv caller.
    rx_ring: SpscRingBuffer,
    /// Whether this connection has been removed/closed.
    closed: AtomicBool,
}

impl SharedConnection {
    fn new(dst: VsockAddr, src_port: u32, buffer_capacity: u32) -> Self {
        Self {
            dst,
            src_port,
            peer_buf_alloc: AtomicU32::new(0),
            peer_fwd_cnt: AtomicU32::new(0),
            tx_cnt: AtomicU32::new(0),
            buf_alloc: AtomicU32::new(buffer_capacity),
            fwd_cnt: AtomicU32::new(0),
            has_pending_credit_request: AtomicBool::new(false),
            peer_requested_shutdown: AtomicBool::new(false),
            rx_ring: SpscRingBuffer::new(buffer_capacity as usize),
            closed: AtomicBool::new(false),
        }
    }

    /// Returns a [`ConnectionInfo`] snapshot for building packet headers.
    pub fn connection_info(&self) -> ConnectionInfo {
        let mut info = ConnectionInfo::new(self.dst, self.src_port);
        info.buf_alloc = self.buf_alloc.load(Ordering::Relaxed);
        info.fwd_cnt = self.fwd_cnt.load(Ordering::Relaxed);
        info
    }

    /// Returns the number of bytes of RX buffer space the peer has available.
    fn peer_free(&self) -> u32 {
        let buf_alloc = self.peer_buf_alloc.load(Ordering::Relaxed);
        let tx_cnt = self.tx_cnt.load(Ordering::Relaxed);
        let peer_fwd_cnt = self.peer_fwd_cnt.load(Ordering::Relaxed);
        buf_alloc.wrapping_sub(tx_cnt.wrapping_sub(peer_fwd_cnt))
    }

    fn update_for_event(&self, event: &VsockEvent) {
        self.peer_buf_alloc
            .store(event.buffer_status.buffer_allocation, Ordering::Relaxed);
        self.peer_fwd_cnt
            .store(event.buffer_status.forward_count, Ordering::Relaxed);
        if let VsockEventType::CreditUpdate = event.event_type {
            self.has_pending_credit_request
                .store(false, Ordering::Relaxed);
        }
    }

    /// Returns the number of bytes available to be read from this connection.
    pub fn available(&self) -> usize {
        self.rx_ring.used()
    }

    /// Reads received data from this connection's ring buffer (consumer side).
    ///
    /// Returns the number of bytes read.
    pub fn recv(&self, out: &mut [u8]) -> usize {
        let n = self.rx_ring.drain(out);
        if n > 0 {
            self.fwd_cnt.fetch_add(n as u32, Ordering::Relaxed);
        }
        n
    }

    /// Whether this connection has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// A shared table of connections accessible by both TX and RX sides.
pub struct ConnectionTable {
    connections: Vec<Arc<SharedConnection>>,
    listening_ports: Vec<u32>,
    per_connection_buffer_capacity: u32,
}

impl ConnectionTable {
    fn new(per_connection_buffer_capacity: u32) -> Self {
        Self {
            connections: Vec::new(),
            listening_ports: Vec::new(),
            per_connection_buffer_capacity,
        }
    }

    /// Find a connection by peer address and local port.
    pub fn find(&self, peer: VsockAddr, local_port: u32) -> Option<&Arc<SharedConnection>> {
        self.connections
            .iter()
            .find(|c| c.dst == peer && c.src_port == local_port && !c.is_closed())
    }

    fn find_for_event(
        &self,
        event: &VsockEvent,
        local_cid: u64,
    ) -> Option<&Arc<SharedConnection>> {
        self.connections.iter().find(|c| {
            event.matches_connection(&c.connection_info(), local_cid) && !c.is_closed()
        })
    }

    fn add_connection(&mut self, dst: VsockAddr, src_port: u32) -> Arc<SharedConnection> {
        let conn = Arc::new(SharedConnection::new(
            dst,
            src_port,
            self.per_connection_buffer_capacity,
        ));
        self.connections.push(conn.clone());
        conn
    }

    fn remove_closed(&mut self) {
        self.connections.retain(|c| !c.is_closed());
    }

    /// Allows incoming connections on the given port number.
    pub fn listen(&mut self, port: u32) {
        if !self.listening_ports.contains(&port) {
            self.listening_ports.push(port);
        }
    }

    /// Stops allowing incoming connections on the given port number.
    pub fn unlisten(&mut self, port: u32) {
        self.listening_ports.retain(|p| *p != port);
    }

    fn is_listening(&self, port: u32) -> bool {
        self.listening_ports.contains(&port)
    }
}

/// Trait for the TX half of a split connection manager, shared by both the driver and device sides.
///
/// This allows writing code that is generic over whether it operates on a driver-side or
/// device-side connection manager.
pub trait SplitManagerTx {
    /// Returns a reference to the connection table.
    fn connections(&self) -> &ConnectionTable;

    /// Returns a mutable reference to the connection table.
    fn connections_mut(&mut self) -> &mut ConnectionTable;

    /// Sends the buffer to the destination.
    fn send(&mut self, destination: VsockAddr, src_port: u32, buffer: &[u8]) -> Result;

    /// Sends a credit update to the given peer.
    fn update_credit(&mut self, peer: VsockAddr, src_port: u32) -> Result;

    /// Requests to shut down the connection cleanly.
    fn shutdown(&mut self, destination: VsockAddr, src_port: u32) -> Result;

    /// Forcibly closes the connection without waiting for the peer.
    fn force_close(&mut self, destination: VsockAddr, src_port: u32) -> Result;

    /// Executes a [`TxAction`] returned by the RX side's `poll`.
    fn execute(&mut self, action: TxAction) -> Result;

    /// Allows incoming connections on the given port number.
    fn listen(&mut self, port: u32);

    /// Stops allowing incoming connections on the given port number.
    fn unlisten(&mut self, port: u32);
}

/// Generic TX half of a split connection manager, parameterized over the inner TX type.
///
/// This is the single implementation backing both [`SplitConnectionManagerTx`] and
/// [`SplitDeviceConnectionManagerTx`].
pub struct SplitManagerTxImpl<Tx: VsockTx> {
    tx: Tx,
    connections: ConnectionTable,
}

impl<Tx: VsockTx> SplitManagerTx for SplitManagerTxImpl<Tx> {
    fn connections(&self) -> &ConnectionTable {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut ConnectionTable {
        &mut self.connections
    }

    fn send(&mut self, destination: VsockAddr, src_port: u32, buffer: &[u8]) -> Result {
        let conn = self
            .connections
            .find(destination, src_port)
            .ok_or(SocketError::NotConnected)?;

        if (conn.peer_free() as usize) < buffer.len() {
            if !conn.has_pending_credit_request.load(Ordering::Relaxed) {
                self.tx.request_credit(&conn.connection_info())?;
                conn.has_pending_credit_request
                    .store(true, Ordering::Relaxed);
            }
            return Err(SocketError::InsufficientBufferSpaceInPeer.into());
        }

        let len = buffer.len() as u32;
        let mut info = conn.connection_info();
        self.tx.send(buffer, &mut info)?;
        conn.tx_cnt.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    fn update_credit(&mut self, peer: VsockAddr, src_port: u32) -> Result {
        let conn = self
            .connections
            .find(peer, src_port)
            .ok_or(SocketError::NotConnected)?;
        self.tx.credit_update(&conn.connection_info())
    }

    fn shutdown(&mut self, destination: VsockAddr, src_port: u32) -> Result {
        let conn = self
            .connections
            .find(destination, src_port)
            .ok_or(SocketError::NotConnected)?;
        self.tx.shutdown(&conn.connection_info())
    }

    fn force_close(&mut self, destination: VsockAddr, src_port: u32) -> Result {
        let conn = self
            .connections
            .find(destination, src_port)
            .ok_or(SocketError::NotConnected)?;
        self.tx.force_close(&conn.connection_info())?;
        conn.closed.store(true, Ordering::Release);
        self.connections.remove_closed();
        Ok(())
    }

    fn execute(&mut self, action: TxAction) -> Result {
        match action {
            TxAction::Accept(info) => self.tx.accept(&info),
            TxAction::Reject(info) => {
                self.tx.force_close(&info)?;
                if let Some(conn) = self.connections.find(info.dst, info.src_port) {
                    conn.closed.store(true, Ordering::Release);
                }
                self.connections.remove_closed();
                Ok(())
            }
            TxAction::CreditUpdate(info) => self.tx.credit_update(&info),
            TxAction::ForceClose(info) => {
                self.tx.force_close(&info)?;
                if let Some(conn) = self.connections.find(info.dst, info.src_port) {
                    conn.closed.store(true, Ordering::Release);
                }
                self.connections.remove_closed();
                Ok(())
            }
        }
    }

    fn listen(&mut self, port: u32) {
        self.connections.listen(port);
    }

    fn unlisten(&mut self, port: u32) {
        self.connections.unlisten(port);
    }
}

// ==================== Driver-side split connection manager ====================

/// The TX (sending) half of a split driver-side connection manager.
///
/// Owns the TX virtqueue exclusively. Methods take `&mut self` but no lock is needed
/// because this half is owned by a single task/thread.
///
/// Implements [`SplitManagerTx`] for methods shared with the device side.
/// Additionally provides [`connect`](Self::connect) and [`guest_cid`](Self::guest_cid)
/// which are driver-only.
pub type SplitConnectionManagerTx<H, T> = SplitManagerTxImpl<VirtIOSocketTx<H, T>>;

impl<H: Hal, T: Transport> SplitConnectionManagerTx<H, T> {
    /// Returns the guest CID.
    pub fn guest_cid(&self) -> u64 {
        self.tx.guest_cid()
    }

    /// Sends a request to connect to the given destination.
    pub fn connect(&mut self, destination: VsockAddr, src_port: u32) -> Result {
        if self.connections.find(destination, src_port).is_some() {
            return Err(SocketError::ConnectionExists.into());
        }

        let conn = self.connections.add_connection(destination, src_port);
        self.tx.connect(&conn.connection_info())?;
        debug!("Connection requested: {:?}", conn.connection_info());
        Ok(())
    }
}

/// The RX (receiving/polling) half of a split driver-side connection manager.
///
/// Owns the RX virtqueue exclusively. Methods take `&mut self` but no lock is needed
/// because this half is owned by a single task/thread.
pub struct SplitConnectionManagerRx<
    H: Hal,
    T: Transport,
    const RX_BUFFER_SIZE: usize = DEFAULT_RX_BUFFER_SIZE,
> {
    rx: VirtIOSocketRx<H, T, RX_BUFFER_SIZE>,
}

impl<H: Hal, T: Transport, const RX_BUFFER_SIZE: usize>
    SplitConnectionManagerRx<H, T, RX_BUFFER_SIZE>
{
    /// Returns the guest CID.
    pub fn guest_cid(&self) -> u64 {
        self.rx.guest_cid()
    }

    /// Polls the RX virtqueue for the next event.
    ///
    /// Returns the event and an optional [`TxAction`] that the TX side must execute.
    /// Pass any returned `TxAction` to [`SplitManagerTx::execute`].
    pub fn poll(
        &mut self,
        connections: &mut ConnectionTable,
        local_cid: u64,
    ) -> Result<(Option<VsockEvent>, Option<TxAction>)> {
        poll_inner(&mut self.rx, connections, local_cid)
    }
}

// ==================== Device-side split connection manager ====================

/// The TX (sending) half of a split device-side connection manager.
///
/// Implements [`SplitManagerTx`] for methods shared with the driver side.
pub type SplitDeviceConnectionManagerTx<H, T> = SplitManagerTxImpl<VirtIOSocketDeviceTx<H, T>>;

/// The RX (receiving/polling) half of a split device-side connection manager.
pub struct SplitDeviceConnectionManagerRx<H: DeviceHal, T: DeviceTransport> {
    rx: VirtIOSocketDeviceRx<H, T>,
}

impl<H: DeviceHal, T: DeviceTransport> SplitDeviceConnectionManagerRx<H, T> {
    /// Polls the driver's TX queue for the next event.
    ///
    /// Returns the event and an optional [`TxAction`] that the TX side must execute.
    pub fn poll(
        &mut self,
        connections: &mut ConnectionTable,
    ) -> Result<(Option<VsockEvent>, Option<TxAction>)> {
        poll_inner(&mut self.rx, connections, VMADDR_CID_HOST)
    }
}

// ==================== Shared poll implementation ====================

/// Trait for the RX poll operation, implemented by both driver and device RX halves.
trait VsockRx {
    fn poll(
        &mut self,
        handler: impl FnOnce(VsockEvent, &[u8]) -> Result<Option<VsockEvent>>,
    ) -> Result<Option<VsockEvent>>;
}

impl<H: Hal, T: Transport, const RX_BUFFER_SIZE: usize> VsockRx
    for VirtIOSocketRx<H, T, RX_BUFFER_SIZE>
{
    fn poll(
        &mut self,
        handler: impl FnOnce(VsockEvent, &[u8]) -> Result<Option<VsockEvent>>,
    ) -> Result<Option<VsockEvent>> {
        self.poll(handler)
    }
}

impl<H: DeviceHal, T: DeviceTransport> VsockRx for VirtIOSocketDeviceRx<H, T> {
    fn poll(
        &mut self,
        handler: impl FnOnce(VsockEvent, &[u8]) -> Result<Option<VsockEvent>>,
    ) -> Result<Option<VsockEvent>> {
        self.poll(handler)
    }
}

fn poll_inner(
    rx: &mut impl VsockRx,
    connections: &mut ConnectionTable,
    local_cid: u64,
) -> Result<(Option<VsockEvent>, Option<TxAction>)> {
    let mut tx_action: Option<TxAction> = None;

    let result = rx.poll(|event, body| {
        let conn = connections.find_for_event(&event, local_cid);

        let conn = if let Some(conn) = conn {
            conn.clone()
        } else if let VsockEventType::ConnectionRequest = event.event_type {
            if connections.find_for_event(&event, local_cid).is_some()
                || event.destination.cid != local_cid
            {
                return Ok(None);
            }
            connections.add_connection(event.source, event.destination.port)
        } else {
            return Ok(None);
        };

        conn.update_for_event(&event);

        if let VsockEventType::Received { length: _ } = event.event_type {
            if !conn.rx_ring.add(body) {
                return Err(SocketError::OutputBufferTooShort(body.len()).into());
            }
        }

        match event.event_type {
            VsockEventType::ConnectionRequest => {
                let info = conn.connection_info();
                if connections.is_listening(event.destination.port) {
                    tx_action = Some(TxAction::Accept(info));
                } else {
                    tx_action = Some(TxAction::Reject(info));
                    return Ok(None);
                }
            }
            VsockEventType::Disconnected { reason } => {
                if conn.rx_ring.is_empty() {
                    if reason == DisconnectReason::Shutdown {
                        tx_action = Some(TxAction::ForceClose(conn.connection_info()));
                    }
                    conn.closed.store(true, Ordering::Release);
                } else {
                    conn.peer_requested_shutdown.store(true, Ordering::Relaxed);
                }
            }
            VsockEventType::CreditRequest => {
                tx_action = Some(TxAction::CreditUpdate(conn.connection_info()));
                return Ok(None);
            }
            _ => {}
        }

        Ok(Some(event))
    })?;

    connections.remove_closed();
    Ok((result, tx_action))
}

// ==================== Constructor functions ====================

/// Creates a split driver-side connection manager from the TX and RX halves of a
/// [`VirtIOSocket`](super::VirtIOSocket).
///
/// Use [`VirtIOSocket::split`](super::VirtIOSocket::split) first to get the halves.
pub fn split_connection_manager<H: Hal, T: Transport, const RX_BUFFER_SIZE: usize>(
    tx: VirtIOSocketTx<H, T>,
    rx: VirtIOSocketRx<H, T, RX_BUFFER_SIZE>,
) -> (
    SplitConnectionManagerTx<H, T>,
    SplitConnectionManagerRx<H, T, RX_BUFFER_SIZE>,
) {
    split_connection_manager_with_capacity(tx, rx, DEFAULT_PER_CONNECTION_BUFFER_CAPACITY)
}

/// Creates a split driver-side connection manager with a custom per-connection buffer capacity.
pub fn split_connection_manager_with_capacity<
    H: Hal,
    T: Transport,
    const RX_BUFFER_SIZE: usize,
>(
    tx: VirtIOSocketTx<H, T>,
    rx: VirtIOSocketRx<H, T, RX_BUFFER_SIZE>,
    per_connection_buffer_capacity: u32,
) -> (
    SplitConnectionManagerTx<H, T>,
    SplitConnectionManagerRx<H, T, RX_BUFFER_SIZE>,
) {
    (
        SplitManagerTxImpl {
            tx,
            connections: ConnectionTable::new(per_connection_buffer_capacity),
        },
        SplitConnectionManagerRx { rx },
    )
}

/// Creates a split device-side connection manager from the TX and RX halves of a
/// [`VirtIOSocketDevice`](super::VirtIOSocketDevice).
///
/// Use [`VirtIOSocketDevice::split`](super::VirtIOSocketDevice::split) first to get the halves.
pub fn split_device_connection_manager<H: DeviceHal, T: DeviceTransport>(
    tx: VirtIOSocketDeviceTx<H, T>,
    rx: VirtIOSocketDeviceRx<H, T>,
) -> (
    SplitDeviceConnectionManagerTx<H, T>,
    SplitDeviceConnectionManagerRx<H, T>,
) {
    split_device_connection_manager_with_capacity(tx, rx, DEFAULT_PER_CONNECTION_BUFFER_CAPACITY)
}

/// Creates a split device-side connection manager with a custom per-connection buffer capacity.
pub fn split_device_connection_manager_with_capacity<H: DeviceHal, T: DeviceTransport>(
    tx: VirtIOSocketDeviceTx<H, T>,
    rx: VirtIOSocketDeviceRx<H, T>,
    per_connection_buffer_capacity: u32,
) -> (
    SplitDeviceConnectionManagerTx<H, T>,
    SplitDeviceConnectionManagerRx<H, T>,
) {
    (
        SplitManagerTxImpl {
            tx,
            connections: ConnectionTable::new(per_connection_buffer_capacity),
        },
        SplitDeviceConnectionManagerRx { rx },
    )
}
