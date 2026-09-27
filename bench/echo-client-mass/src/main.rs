//! High-concurrency echo load generator (neton-io SPEC §26.3).
//!
//! Usage: echo-client-mass ADDR ACTIVE SECS PAYLOAD THREADS [IDLE=0] [SOURCES=8]
//!
//! ACTIVE connections run a closed loop (write PAYLOAD, read it back, repeat: one request in flight
//! per connection); IDLE connections are opened and then left silent. Connections are spread over
//! THREADS threads, each with its own epoll (mio). Source addresses rotate over 127.0.0.1 ..
//! 127.0.0.SOURCES, so more connections than one destination's ephemeral port range can be opened.
//! Timing starts once every connection is established. Prints the echo-client-fair fields.
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use socket2::{Domain, SockAddr, Socket, Type};
use std::io::{ErrorKind, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Latency histogram: 1 µs buckets up to 1 s, one overflow bucket.
const BUCKETS: usize = 1_000_001;

struct Conn {
    sock: TcpStream,
    written: usize,
    read: usize,
    started: Instant,
    count: u64,
}

fn open(target: SocketAddr, source: Ipv4Addr) -> std::net::TcpStream {
    let s = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket");
    s.bind(&SockAddr::from(SocketAddrV4::new(source, 0))).expect("bind source");
    s.connect(&SockAddr::from(target)).expect("connect");
    s.set_tcp_nodelay(true).unwrap();
    s.set_nonblocking(true).unwrap();
    s.into()
}

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let target: SocketAddr = a.first().map(|s| s.as_str()).unwrap_or("127.0.0.1:18090").parse().expect("ADDR");
    let arg = |i: usize, d: usize| a.get(i).and_then(|v| v.parse().ok()).unwrap_or(d);
    let active = arg(1, 1000);
    let secs = arg(2, 10) as u64;
    let payload = arg(3, 128).max(1);
    let threads = arg(4, 2).max(1);
    let idle = arg(5, 0);
    let sources = arg(6, 8).clamp(1, 254);

    // Idle connections are opened first, by the main thread, and just held.
    let t0 = Instant::now();
    let mut idle_socks = Vec::with_capacity(idle);
    for i in 0..idle {
        idle_socks.push(open(target, Ipv4Addr::new(127, 0, 0, 1 + (i % sources) as u8)));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for t in 0..threads {
        let n = active / threads + usize::from(t < active % threads);
        let (stop, ready) = (stop.clone(), ready.clone());
        handles.push(std::thread::spawn(move || {
            let mut poll = Poll::new().expect("poll");
            let mut conns: Vec<Conn> = (0..n)
                .map(|i| {
                    let src = Ipv4Addr::new(127, 0, 0, 1 + ((idle + t + i * threads) % sources) as u8);
                    Conn { sock: TcpStream::from_std(open(target, src)), written: 0, read: 0, started: Instant::now(), count: 0 }
                })
                .collect();
            for (i, c) in conns.iter_mut().enumerate() {
                poll.registry().register(&mut c.sock, Token(i), Interest::READABLE | Interest::WRITABLE).unwrap();
            }
            let mut out = vec![b'x'; payload];
            *out.last_mut().unwrap() = b'\n';
            let mut buf = vec![0u8; payload.max(64 * 1024)];
            let mut hist = vec![0u64; BUCKETS];
            ready.wait();
            let now = Instant::now();
            for c in conns.iter_mut() { c.started = now; }
            let mut events = Events::with_capacity(1024);
            // Kick every connection: edge-triggered, so try the first write now.
            let mut todo: Vec<usize> = (0..n).collect();
            while !stop.load(Ordering::Relaxed) {
                for &i in &todo { step(&mut conns[i], &out, &mut buf, &mut hist); }
                todo.clear();
                poll.poll(&mut events, Some(Duration::from_millis(10))).expect("poll");
                for ev in events.iter() { todo.push(ev.token().0); }
            }
            let counts: Vec<u64> = conns.iter().map(|c| c.count).collect();
            (counts, hist)
        }));
    }
    ready.wait();
    let connect_secs = t0.elapsed().as_secs_f64();
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    let mut counts = Vec::with_capacity(active);
    let mut hist = vec![0u64; BUCKETS];
    for h in handles {
        let (c, hh) = h.join().unwrap();
        counts.extend(c);
        for (a, b) in hist.iter_mut().zip(hh) { *a += b; }
    }
    let elapsed = start.elapsed().as_secs_f64();
    drop(idle_socks);

    counts.sort_unstable();
    let total: u64 = hist.iter().sum();
    let pct = |p: f64| -> f64 {
        let want = ((total as f64) * p).ceil().max(1.0) as u64;
        let mut acc = 0u64;
        for (us, &n) in hist.iter().enumerate() { acc += n; if acc >= want { return us as f64; } }
        0.0
    };
    let cnt = |p: f64| -> u64 { counts[((counts.len() as f64 * p) as usize).min(counts.len() - 1)] };
    let sum: f64 = counts.iter().map(|&c| c as f64).sum();
    let sumsq: f64 = counts.iter().map(|&c| (c as f64) * (c as f64)).sum();
    let jain = if sumsq == 0.0 { 0.0 } else { sum * sum / (counts.len() as f64 * sumsq) };
    println!("target      {target}");
    println!("conns       {active}");
    println!("idle        {idle}");
    println!("threads     {threads}");
    println!("payload     {payload} bytes");
    println!("connect     {connect_secs:.2} s");
    println!("duration    {elapsed:.2} s");
    println!("requests    {total}");
    println!("qps         {:.0}", total as f64 / elapsed);
    println!("p50         {:.1} us", pct(0.50));
    println!("p99         {:.1} us", pct(0.99));
    println!("p999        {:.1} us", pct(0.999));
    println!("conn_min    {}", counts[0]);
    println!("conn_p10    {}", cnt(0.10));
    println!("conn_med    {}", cnt(0.50));
    println!("conn_max    {}", counts[counts.len() - 1]);
    println!("conn_zero   {}", counts.iter().filter(|&&c| c == 0).count());
    println!("jain        {jain:.3}");
}

/// Advance one connection as far as it goes without blocking: finish the write, read the echo,
/// and on a complete echo record the latency and start the next request.
fn step(c: &mut Conn, out: &[u8], buf: &mut [u8], hist: &mut [u64]) {
    loop {
        while c.written < out.len() {
            match c.sock.write(&out[c.written..]) {
                Ok(n) => c.written += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => panic!("write: {e}"),
            }
        }
        while c.read < out.len() {
            match c.sock.read(&mut buf[..out.len() - c.read]) {
                Ok(0) => panic!("server closed a connection"),
                Ok(n) => c.read += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => panic!("read: {e}"),
            }
        }
        let now = Instant::now();
        let us = now.duration_since(c.started).as_micros() as usize;
        hist[us.min(BUCKETS - 1)] += 1;
        c.count += 1;
        c.written = 0;
        c.read = 0;
        c.started = now;
    }
}
