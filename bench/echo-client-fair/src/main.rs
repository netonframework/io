//! Echo load generator with per-connection fairness counts.
//!
//! Same method as geario's bench-echo client (blocking sockets, one thread per connection, closed
//! loop of write-then-read). Additionally prints how many requests each connection completed, so a
//! server that serves some connections and starves others is visible: the latency percentiles only
//! sample requests that completed and would otherwise look excellent.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:8080".into());
    let conns: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(64);
    let secs: u64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(10);
    let payload: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(128);
    // Pipelining depth: send `depth` requests back to back, then read `depth` replies (SPEC §23.2).
    // Each request ends in '\n' so a line-framed server sees one request per payload.
    let depth: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(1).max(1);

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..conns {
        let addr = addr.clone();
        let stop = stop.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || {
            let mut sock = TcpStream::connect(&addr).expect("connect");
            sock.set_nodelay(true).unwrap();
            let mut one = vec![b'x'; payload];
            if let Some(last) = one.last_mut() { *last = b'\n'; }
            let out: Vec<u8> = one.iter().cycle().take(payload * depth).cloned().collect();
            let mut buf = vec![0u8; payload * depth];
            let mut lat = Vec::with_capacity(1 << 16);
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                if sock.write_all(&out).is_err() { break; }
                if sock.read_exact(&mut buf).is_err() { break; }
                lat.push(t.elapsed().as_nanos() as u64);
                total.fetch_add(depth as u64, Ordering::Relaxed);
            }
            lat
        }));
    }
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    let per_conn: Vec<Vec<u64>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let elapsed = start.elapsed().as_secs_f64();

    let mut counts: Vec<u64> = per_conn.iter().map(|v| v.len() as u64).collect();
    counts.sort_unstable();
    let mut all: Vec<u64> = per_conn.into_iter().flatten().collect();
    all.sort_unstable();
    let n = all.len();
    let pct = |p: f64| -> f64 { if n == 0 { 0.0 } else { all[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0 } };
    let cnt = |p: f64| -> u64 { counts[((counts.len() as f64 * p) as usize).min(counts.len() - 1)] };
    let sum: f64 = counts.iter().map(|&c| c as f64).sum();
    let sumsq: f64 = counts.iter().map(|&c| (c as f64) * (c as f64)).sum();
    let jain = if sumsq == 0.0 { 0.0 } else { sum * sum / (counts.len() as f64 * sumsq) };

    let count = total.load(Ordering::Relaxed);
    println!("target      {addr}");
    println!("conns       {conns}");
    println!("payload     {payload} bytes");
    println!("depth       {depth}");
    println!("duration    {elapsed:.2} s");
    println!("requests    {count}");
    println!("qps         {:.0}", count as f64 / elapsed);
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
