//! K07 loopback MySQL proxy for real-DB pool lifecycle tests.
//!
//! Included with `#[path]` by `tests/k07_pool_lifecycle.rs` and by the
//! `k07_store_` tests in `src/store.rs` (cfg(test) only). It relays plaintext
//! MySQL between 127.0.0.1 clients and the task-owned server, learns each connection's server
//! thread id from the greeting, and can withhold one connection's next COM_PING OK reply (or the
//! greeting of new connections) until released. It never records packet contents, credentials
//! or URLs; only counters and timestamps.
#![allow(dead_code)]

use std::collections::{HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

const MAX_PACKET: usize = 1 << 20;
const COM_PING: u8 = 0x0e;

#[derive(Clone, Debug, Default)]
pub struct ConnSnapshot {
    pub index: usize,
    pub mysql_id: Option<u32>,
    /// COM_PING packets sent by the client.
    pub pings: u32,
    /// Replies (ping OK or greeting) currently withheld.
    pub held_now: u32,
    pub held_total: u32,
    /// Withheld replies that were later forwarded.
    pub released_total: u32,
    pub client_eof_at: Option<Instant>,
    pub relay_done_at: Option<Instant>,
}

#[derive(Default)]
struct State {
    conns: Vec<ConnSnapshot>,
    hold_ping_for: HashSet<u32>,
    hold_greetings: bool,
    relays_active: usize,
}

struct Shared {
    upstream: SocketAddr,
    state: Mutex<State>,
    changed: Notify,
    release: watch::Sender<u64>,
}

impl Shared {
    fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let out = f(&mut self.state.lock().unwrap());
        self.changed.notify_waiters();
        out
    }
}

pub struct PingProxy {
    addr: SocketAddr,
    shared: Arc<Shared>,
    accept: JoinHandle<()>,
    relays: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

#[derive(Debug)]
pub struct ProxyCleanup {
    pub accepted: usize,
    pub relays_active_after_wait: usize,
    pub relays_force_aborted: usize,
    pub listener_closed: bool,
}

impl PingProxy {
    pub async fn start(upstream: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (release, _) = watch::channel(0u64);
        let shared = Arc::new(Shared {
            upstream,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            release,
        });
        let relays = Arc::new(Mutex::new(Vec::new()));
        let accept = {
            let shared = shared.clone();
            let relays = relays.clone();
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let (index, hold_greeting) = shared.update(|s| {
                        s.conns.push(ConnSnapshot {
                            index: s.conns.len(),
                            ..Default::default()
                        });
                        s.relays_active += 1;
                        (s.conns.len() - 1, s.hold_greetings)
                    });
                    let task = tokio::spawn(relay(shared.clone(), index, client, hold_greeting));
                    relays.lock().unwrap().push(task);
                }
            })
        };
        Ok(Self {
            addr,
            shared,
            accept,
            relays,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Withhold the next COM_PING OK reply sent to this server thread id.
    pub fn hold_next_ping_reply(&self, mysql_id: u32) {
        self.shared.update(|s| s.hold_ping_for.insert(mysql_id));
    }

    /// Withhold the server greeting of connections accepted from now on.
    pub fn hold_greetings(&self, on: bool) {
        self.shared.update(|s| s.hold_greetings = on);
    }

    /// Forward every reply withheld so far.
    pub fn release_held(&self) {
        self.shared
            .release
            .send_modify(|generation| *generation += 1);
    }

    pub fn snapshot(&self) -> Vec<ConnSnapshot> {
        self.shared.state.lock().unwrap().conns.clone()
    }

    pub fn by_mysql_id(&self, mysql_id: u32) -> ConnSnapshot {
        self.snapshot()
            .into_iter()
            .find(|c| c.mysql_id == Some(mysql_id))
            .unwrap_or_else(|| panic!("proxy never saw mysql thread {mysql_id}"))
    }

    pub fn relays_active(&self) -> usize {
        self.shared.state.lock().unwrap().relays_active
    }

    /// Wait (bounded) until `pred` holds for the connection table.
    pub async fn wait_for(
        &self,
        what: &str,
        bound: Duration,
        pred: impl Fn(&[ConnSnapshot]) -> bool,
    ) -> Instant {
        let deadline = Instant::now() + bound;
        loop {
            let notified = self.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if pred(&self.snapshot()) {
                return Instant::now();
            }
            let now = Instant::now();
            assert!(
                now < deadline,
                "proxy: {what} not observed within {bound:?}"
            );
            let _ = tokio::time::timeout(deadline - now, notified).await;
        }
    }

    /// Stop accepting, release anything held, wait (bounded) for relays to end on their own and
    /// abort whatever is left. Returns what had to be forced.
    pub async fn shutdown(self) -> ProxyCleanup {
        let PingProxy {
            shared,
            accept,
            relays,
            ..
        } = self;
        accept.abort();
        let listener_closed = accept.await.is_err_and(|e| e.is_cancelled());
        shared.release.send_modify(|generation| *generation += 1);
        let active = || shared.state.lock().unwrap().relays_active;
        let deadline = Instant::now() + Duration::from_secs(3);
        while active() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let relays_active_after_wait = active();
        let tasks: Vec<_> = relays.lock().unwrap().drain(..).collect();
        let mut relays_force_aborted = 0;
        for task in tasks {
            if !task.is_finished() {
                relays_force_aborted += 1;
                task.abort();
            }
            let _ = task.await;
        }
        let accepted = shared.state.lock().unwrap().conns.len();
        ProxyCleanup {
            accepted,
            relays_active_after_wait,
            relays_force_aborted,
            listener_closed,
        }
    }
}

async fn read_packet(reader: &mut (impl AsyncReadExt + Unpin)) -> io::Result<([u8; 4], Vec<u8>)> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    if len > MAX_PACKET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fixture packet too large",
        ));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok((header, payload))
}

/// Server thread id from a protocol-10 greeting: version byte, NUL-terminated version, u32 id.
fn greeting_thread_id(payload: &[u8]) -> Option<u32> {
    if payload.first() != Some(&10) {
        return None;
    }
    let nul = payload[1..].iter().position(|b| *b == 0)? + 1;
    let id = payload.get(nul + 1..nul + 5)?;
    Some(u32::from_le_bytes([id[0], id[1], id[2], id[3]]))
}

async fn wait_released(shared: &Shared, held_at: u64) {
    let mut rx = shared.release.subscribe();
    while *rx.borrow_and_update() <= held_at {
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn relay(shared: Arc<Shared>, index: usize, client: TcpStream, hold_greeting: bool) {
    let upstream = TcpStream::connect(shared.upstream).await;
    let Ok(server) = upstream else {
        shared.update(|s| {
            s.conns[index].relay_done_at = Some(Instant::now());
            s.relays_active -= 1;
        });
        return;
    };
    let (mut client_r, mut client_w) = client.into_split();
    let (mut server_r, mut server_w) = server.into_split();
    let pending_pings: Arc<Mutex<VecDeque<bool>>> = Default::default();

    let up = {
        let shared = shared.clone();
        let pending_pings = pending_pings.clone();
        async move {
            loop {
                let (header, payload) = match read_packet(&mut client_r).await {
                    Ok(packet) => packet,
                    Err(_) => {
                        shared.update(|s| s.conns[index].client_eof_at = Some(Instant::now()));
                        return;
                    }
                };
                if header[3] == 0 && payload.as_slice() == [COM_PING] {
                    let hold = shared.update(|s| {
                        let conn = &mut s.conns[index];
                        conn.pings += 1;
                        conn.mysql_id.is_some_and(|id| s.hold_ping_for.remove(&id))
                    });
                    pending_pings.lock().unwrap().push_back(hold);
                }
                if server_w.write_all(&header).await.is_err()
                    || server_w.write_all(&payload).await.is_err()
                {
                    return;
                }
            }
        }
    };

    let down = {
        let shared = shared.clone();
        async move {
            let mut first = true;
            loop {
                let Ok((header, payload)) = read_packet(&mut server_r).await else {
                    return;
                };
                let mut hold = false;
                if first {
                    first = false;
                    let id = greeting_thread_id(&payload);
                    shared.update(|s| s.conns[index].mysql_id = id);
                    hold = hold_greeting;
                } else if let Some(held) = pending_pings.lock().unwrap().pop_front() {
                    hold = held;
                }
                if hold {
                    let held_at = *shared.release.borrow();
                    shared.update(|s| {
                        s.conns[index].held_now += 1;
                        s.conns[index].held_total += 1;
                    });
                    wait_released(&shared, held_at).await;
                    shared.update(|s| {
                        s.conns[index].held_now -= 1;
                        s.conns[index].released_total += 1;
                    });
                }
                if client_w.write_all(&header).await.is_err()
                    || client_w.write_all(&payload).await.is_err()
                {
                    return;
                }
            }
        }
    };

    // Either direction ending ends the relay; both sockets close when this function returns.
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
    shared.update(|s| {
        let conn = &mut s.conns[index];
        conn.held_now = 0;
        conn.relay_done_at = Some(Instant::now());
        s.relays_active -= 1;
    });
}
