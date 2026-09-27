//! High-concurrency echo load generator (neton-io SPEC §26.3, §28.4).
//!
//! Legacy usage (closed loop, one class):
//!   echo-client-mass ADDR ACTIVE SECS PAYLOAD THREADS [IDLE=0] [SOURCES=8]
//!
//! Class usage (SPEC §28.4):
//!   echo-client-mass ADDR --secs 30 --warmup 3 --threads 2 --payload 64 [--sources 8] [--idle 0]
//!                    --class NAME:CONNS:DEPTH:RATE [--class ...]
//!
//! A class is CONNS connections that each send DEPTH requests at a time (pipelined in one write).
//! RATE is requests per second per connection: 0 = closed loop (the next batch goes out when the
//! last response of the previous one arrives); > 0 = open loop (a batch every DEPTH / RATE seconds
//! on a fixed schedule with a random phase, sent whether or not earlier responses have arrived).
//! Latency is measured from the *scheduled* send time to the response, so a slow server cannot hide
//! its queueing by slowing the generator down (no coordinated omission). A request is PAYLOAD bytes
//! ending in '\n', so the same load drives the raw and the line-framed echo servers.
//!
//! Only completions inside [warmup, warmup + secs) count. Per class it reports latency percentiles
//! (with the sample count, so the caller can apply the p99 / p999 sample rules), the send lag
//! (how late the generator itself fired open-loop batches), per-connection completion counts (Jain,
//! min, median) and the longest gap between two completions of one connection. The generator's own
//! CPU use (user + sys over the run, per thread) is printed as `gen_cpu`: above 0.8 the run is void.
//!
//! Connections are spread over THREADS threads, each with its own epoll (mio). Source addresses
//! rotate over 127.0.0.1 .. 127.0.0.SOURCES, so more connections than one destination's ephemeral
//! port range can be opened. Server errors (reset, close) are counted per class, never fatal.
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use socket2::{Domain, SockAddr, Socket, Type};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::io::{ErrorKind, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Latency histogram: 1 µs buckets up to 1 s, then 1 ms buckets up to 60 s, one overflow bucket.
const US_BUCKETS: usize = 1_000_000;
const MS_BUCKETS: usize = 59_000;
const BUCKETS: usize = US_BUCKETS + MS_BUCKETS + 1;

fn bucket(us: u64) -> usize {
    if (us as usize) < US_BUCKETS { us as usize } else { (US_BUCKETS + (us as usize / 1000 - 1000)).min(BUCKETS - 1) }
}
fn bucket_us(b: usize) -> u64 {
    if b < US_BUCKETS { b as u64 } else { ((b - US_BUCKETS) as u64 + 1000) * 1000 }
}

#[derive(Clone)]
struct Class { name: String, conns: usize, depth: usize, rate: f64 }

struct Conn {
    sock: TcpStream,
    class: usize,
    dead: bool,
    /// Bytes still to write, and how far into the payload pattern the next write starts.
    owed: usize,
    wpos: usize,
    /// Scheduled send time of every request written or owed, oldest first.
    sched: VecDeque<Instant>,
    /// Bytes of the oldest outstanding response received so far.
    partial: usize,
    next_send: Instant,
    count: u64,
    last_done: Instant,
    max_gap: Duration,
    src: Ipv4Addr,
}

/// Seconds covered by the per-second completion timeline.
const TIMELINE_SECS: usize = 3600;

struct ClassStats { hist: Vec<u64>, lag: Vec<u64>, errors: u64, max_us: u64, rejected: u64, reconnects: u64, timeline: Vec<u64> }
impl ClassStats {
    fn new() -> Self {
        ClassStats { hist: vec![0; BUCKETS], lag: vec![0; BUCKETS], errors: 0, max_us: 0, rejected: 0, reconnects: 0, timeline: vec![0; TIMELINE_SECS] }
    }
    fn merge(&mut self, o: ClassStats) {
        for (a, b) in self.hist.iter_mut().zip(o.hist) { *a += b; }
        for (a, b) in self.lag.iter_mut().zip(o.lag) { *a += b; }
        for (a, b) in self.timeline.iter_mut().zip(o.timeline) { *a += b; }
        self.rejected += o.rejected;
        self.reconnects += o.reconnects;
        self.errors += o.errors;
        self.max_us = self.max_us.max(o.max_us);
    }
}

fn open(target: SocketAddr, source: Ipv4Addr) -> std::net::TcpStream {
    let s = Socket::new(Domain::IPV4, Type::STREAM, None).expect("socket");
    s.bind(&SockAddr::from(SocketAddrV4::new(source, 0))).expect("bind source");
    s.connect(&SockAddr::from(target)).expect("connect");
    s.set_tcp_nodelay(true).unwrap();
    s.set_nonblocking(true).unwrap();
    s.into()
}

fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Small xorshift for the open-loop phases; the generator needs spread, not security.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
    fn unit(&mut self) -> f64 { (self.next() >> 11) as f64 / (1u64 << 53) as f64 }
}

struct Opts {
    target: SocketAddr,
    secs: f64,
    warmup: f64,
    threads: usize,
    payload: usize,
    sources: usize,
    idle: usize,
    classes: Vec<Class>,
    legacy: bool,
    reconnect: bool,
    timeline: bool,
    send_deadline: Option<Duration>,
}

fn parse() -> Opts {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let target: SocketAddr = a.first().map(|s| s.as_str()).unwrap_or("127.0.0.1:18090").parse().expect("ADDR");
    if a.get(1).map(|s| s.starts_with("--")) != Some(true) {
        let arg = |i: usize, d: usize| a.get(i).and_then(|v| v.parse().ok()).unwrap_or(d);
        return Opts {
            target,
            classes: vec![Class { name: "all".into(), conns: arg(1, 1000), depth: 1, rate: 0.0 }],
            secs: arg(2, 10) as f64,
            payload: arg(3, 128).max(1),
            threads: arg(4, 2).max(1),
            idle: arg(5, 0),
            sources: arg(6, 8).clamp(1, 254),
            warmup: 0.0,
            legacy: true,
            reconnect: false,
            timeline: false,
            send_deadline: None,
        };
    }
    let mut o = Opts { target, secs: 30.0, warmup: 3.0, threads: 2, payload: 64, sources: 8, idle: 0, classes: vec![], legacy: false, reconnect: false, timeline: false, send_deadline: None };
    let mut i = 1;
    while i < a.len() {
        let v = a.get(i + 1).cloned().unwrap_or_else(|| panic!("{} needs a value", a[i]));
        match a[i].as_str() {
            "--secs" => o.secs = v.parse().expect("--secs"),
            "--warmup" => o.warmup = v.parse().expect("--warmup"),
            "--threads" => o.threads = v.parse::<usize>().expect("--threads").max(1),
            "--payload" => o.payload = v.parse::<usize>().expect("--payload").max(1),
            "--sources" => o.sources = v.parse::<usize>().expect("--sources").clamp(1, 254),
            "--idle" => o.idle = v.parse().expect("--idle"),
            // 1: a connection the server closes is reopened 10 ms later (overload runs, SPEC §28.4 L3).
            "--reconnect" => o.reconnect = v == "1",
            // 1: print completions per second of the measured window.
            "--timeline" => o.timeline = v == "1",
            // N > 0: an open-loop request still unsent N ms after its scheduled time is dropped and counted
            // as rejected, as a client with a send timeout would (overload runs: TCP backpressure otherwise
            // grows the generator's own queue without bound).
            "--send-deadline-ms" => { let ms: u64 = v.parse().expect("--send-deadline-ms"); o.send_deadline = (ms > 0).then(|| Duration::from_millis(ms)); }
            "--class" => {
                let p: Vec<&str> = v.split(':').collect();
                assert!(p.len() == 4, "--class NAME:CONNS:DEPTH:RATE");
                o.classes.push(Class {
                    name: p[0].into(),
                    conns: p[1].parse().expect("CONNS"),
                    depth: p[2].parse::<usize>().expect("DEPTH").max(1),
                    rate: p[3].parse().expect("RATE"),
                });
            }
            x => panic!("unknown option {x}"),
        }
        i += 2;
    }
    assert!(!o.classes.is_empty(), "at least one --class");
    o
}

fn main() {
    let o = parse();
    let (target, payload, threads, sources, idle) = (o.target, o.payload, o.threads, o.sources, o.idle);
    let classes = Arc::new(o.classes.clone());

    // Idle connections are opened first, by the main thread, and just held.
    let t0 = Instant::now();
    let mut idle_socks = Vec::with_capacity(idle);
    for i in 0..idle {
        idle_socks.push(open(target, Ipv4Addr::new(127, 0, 0, 1 + (i % sources) as u8)));
    }

    // Round-robin every class's connections over the threads.
    let mut plan: Vec<Vec<usize>> = vec![Vec::new(); threads];
    let mut k = 0;
    for (ci, c) in classes.iter().enumerate() {
        for _ in 0..c.conns { plan[k % threads].push(ci); k += 1; }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(threads + 1));
    let go = Arc::new(Barrier::new(threads + 1));
    let start_at = Arc::new(std::sync::OnceLock::<Instant>::new());
    let mut handles = Vec::new();
    for (t, mine) in plan.into_iter().enumerate() {
        let (stop, ready, go, classes, start_at) = (stop.clone(), ready.clone(), go.clone(), classes.clone(), start_at.clone());
        let (warmup, secs, reconnect, deadline) = (o.warmup, o.secs, o.reconnect, o.send_deadline);
        handles.push(std::thread::spawn(move || {
            let mut poll = Poll::new().expect("poll");
            let mut conns: Vec<Conn> = mine
                .iter()
                .enumerate()
                .map(|(i, &ci)| {
                    let src = Ipv4Addr::new(127, 0, 0, 1 + ((idle + t + i * threads) % sources) as u8);
                    let now = Instant::now();
                    Conn {
                        sock: TcpStream::from_std(open(target, src)), class: ci, dead: false, owed: 0, wpos: 0,
                        sched: VecDeque::new(), partial: 0, next_send: now, count: 0, last_done: now, max_gap: Duration::ZERO, src,
                    }
                })
                .collect();
            for (i, c) in conns.iter_mut().enumerate() {
                poll.registry().register(&mut c.sock, Token(i), Interest::READABLE | Interest::WRITABLE).unwrap();
            }
            // The write source: the payload pattern repeated, so any run of requests is one slice.
            let mut pattern = vec![b'x'; payload];
            *pattern.last_mut().unwrap() = b'\n';
            let reps = (256 * 1024 / payload).max(2);
            let out: Vec<u8> = pattern.iter().cycle().take(payload * reps).cloned().collect();
            let mut buf = vec![0u8; 256 * 1024];
            let mut stats: Vec<ClassStats> = (0..classes.len()).map(|_| ClassStats::new()).collect();
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (t as u64 + 1));
            ready.wait();
            go.wait();
            let start = *start_at.get().unwrap();
            let measure_from = start + Duration::from_secs_f64(warmup);
            let measure_to = measure_from + Duration::from_secs_f64(secs);

            // Open loop: a timer heap of next batch times. Closed loop: the first batch now.
            let mut timers: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::new();
            let mut todo: Vec<usize> = Vec::new();
            // Connections that stopped at their per-turn budget with work left: served again next turn.
            let mut again: Vec<usize> = Vec::new();
            // Closed connections waiting to be reopened (--reconnect), in due order (constant delay).
            let mut reconnects: VecDeque<(Instant, usize)> = VecDeque::new();
            for (i, c) in conns.iter_mut().enumerate() {
                let cl = &classes[c.class];
                c.last_done = measure_from;
                if cl.rate > 0.0 {
                    let interval = Duration::from_secs_f64(cl.depth as f64 / cl.rate);
                    c.next_send = start + interval.mul_f64(rng.unit());
                    timers.push(Reverse((c.next_send, i)));
                } else {
                    for _ in 0..cl.depth { c.sched.push_back(start); }
                    c.owed = cl.depth * payload;
                    todo.push(i);
                }
            }
            let mut events = Events::with_capacity(1024);
            while !stop.load(Ordering::Relaxed) {
                // Fire due open-loop batches (catching up if the generator fell behind).
                let now = Instant::now();
                while let Some(&Reverse((due, i))) = timers.peek() {
                    if due > now { break; }
                    timers.pop();
                    let c = &mut conns[i];
                    let cl = &classes[c.class];
                    let interval = Duration::from_secs_f64(cl.depth as f64 / cl.rate);
                    if c.dead {
                        // Disconnected: the batches it should have sent are rejected, not silently skipped.
                        while c.next_send <= now {
                            if c.next_send >= measure_from && c.next_send < measure_to { stats[c.class].rejected += cl.depth as u64; }
                            c.next_send += interval;
                        }
                        timers.push(Reverse((c.next_send, i)));
                        continue;
                    }
                    while c.next_send <= now {
                        for _ in 0..cl.depth { c.sched.push_back(c.next_send); }
                        c.owed += cl.depth * payload;
                        if c.next_send >= measure_from && c.next_send < measure_to {
                            let lag = now.duration_since(c.next_send).as_micros() as u64;
                            stats[c.class].lag[bucket(lag)] += 1;
                        }
                        c.next_send += interval;
                    }
                    timers.push(Reverse((c.next_send, i)));
                    todo.push(i);
                }
                while let Some(&(due, i)) = reconnects.front() {
                    if due > now { break; }
                    reconnects.pop_front();
                    let c = &mut conns[i];
                    let Some(sock) = open_nonblocking(target, c.src) else { reconnects.push_back((now + RECONNECT_DELAY, i)); continue };
                    c.sock = TcpStream::from_std(sock);
                    poll.registry().register(&mut c.sock, Token(i), Interest::READABLE | Interest::WRITABLE).unwrap();
                    c.dead = false;
                    stats[c.class].reconnects += 1;
                    let cl = &classes[c.class];
                    if cl.rate == 0.0 {
                        for _ in 0..cl.depth { c.sched.push_back(now); }
                        c.owed = cl.depth * payload;
                    }
                    todo.push(i);
                }
                if let Some(d) = deadline {
                    for &i in &todo { drop_late(&mut conns[i], &classes, payload, d, &mut stats, measure_from, measure_to); }
                }
                for &i in &todo {
                    let was_dead = conns[i].dead;
                    if step(&mut conns[i], i, &classes, &out, payload, &mut buf, &mut stats, &poll, measure_from, measure_to) {
                        again.push(i);
                    }
                    if reconnect && !was_dead && conns[i].dead { reconnects.push_back((Instant::now() + RECONNECT_DELAY, i)); }
                }
                todo.clear();
                let wait = if !again.is_empty() { Duration::ZERO } else { match timers.peek() {
                    Some(&Reverse((due, _))) => due.saturating_duration_since(Instant::now()).min(Duration::from_millis(10)),
                    None => Duration::from_millis(10),
                } };
                poll.poll(&mut events, Some(wait)).expect("poll");
                for ev in events.iter() { todo.push(ev.token().0); }
                todo.append(&mut again);
            }
            let end = measure_to;
            let per_conn: Vec<(usize, u64, Duration)> = conns
                .iter()
                .map(|c| {
                    let tail = if c.last_done < end { end.duration_since(c.last_done) } else { Duration::ZERO };
                    (c.class, c.count, c.max_gap.max(tail))
                })
                .collect();
            let outstanding: Vec<(usize, usize)> = conns.iter().map(|c| (c.class, c.sched.len())).collect();
            (per_conn, stats, outstanding)
        }));
    }
    ready.wait();
    let start = Instant::now();
    start_at.set(start).unwrap();
    go.wait();
    let connect_secs = t0.elapsed().as_secs_f64();
    let cpu0 = cpu_seconds();
    std::thread::sleep(Duration::from_secs_f64(o.warmup + o.secs));
    stop.store(true, Ordering::Relaxed);
    let wall = start.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;

    let mut per_conn: Vec<(usize, u64, Duration)> = Vec::new();
    let mut stats: Vec<ClassStats> = (0..classes.len()).map(|_| ClassStats::new()).collect();
    let mut outstanding = vec![0usize; classes.len()];
    for h in handles {
        let (pc, st, out) = h.join().unwrap();
        per_conn.extend(pc);
        for (a, b) in stats.iter_mut().zip(st) { a.merge(b); }
        for (ci, n) in out { outstanding[ci] += n; }
    }
    drop(idle_socks);
    let gen_cpu = cpu / (wall * threads as f64);

    // Legacy summary over every class (the fields run.sh and older scripts read).
    let mut all = ClassStats::new();
    for s in &stats { for (a, b) in all.hist.iter_mut().zip(&s.hist) { *a += b; } }
    let mut counts: Vec<u64> = per_conn.iter().map(|p| p.1).collect();
    counts.sort_unstable();
    let total: u64 = all.hist.iter().sum();
    println!("target      {target}");
    println!("conns       {}", counts.len());
    println!("idle        {idle}");
    println!("threads     {threads}");
    println!("payload     {payload} bytes");
    println!("connect     {connect_secs:.2} s");
    println!("duration    {:.2} s", o.secs);
    println!("requests    {total}");
    println!("qps         {:.0}", total as f64 / o.secs);
    println!("p50         {:.1} us", pct(&all.hist, 0.50) as f64);
    println!("p99         {:.1} us", pct(&all.hist, 0.99) as f64);
    println!("p999        {:.1} us", pct(&all.hist, 0.999) as f64);
    println!("conn_min    {}", counts[0]);
    println!("conn_p10    {}", counts[((counts.len() as f64 * 0.10) as usize).min(counts.len() - 1)]);
    println!("conn_med    {}", counts[counts.len() / 2]);
    println!("conn_max    {}", counts[counts.len() - 1]);
    println!("conn_zero   {}", counts.iter().filter(|&&c| c == 0).count());
    println!("jain        {:.3}", jain(&counts));
    println!("gen_cpu     {gen_cpu:.2}");
    if o.legacy { return; }
    if o.timeline {
        let secs = (o.secs.ceil() as usize).min(TIMELINE_SECS);
        let per: Vec<String> = (0..secs).map(|k| stats.iter().map(|s| s.timeline[k]).sum::<u64>().to_string()).collect();
        println!("timeline_qps {}", per.join(","));
    }
    for (ci, cl) in classes.iter().enumerate() {
        let s = &stats[ci];
        let mut c: Vec<u64> = per_conn.iter().filter(|p| p.0 == ci).map(|p| p.1).collect();
        c.sort_unstable();
        let gap = per_conn.iter().filter(|p| p.0 == ci).map(|p| p.2).max().unwrap_or_default();
        let n: u64 = s.hist.iter().sum();
        let lag_n: u64 = s.lag.iter().sum();
        let med = c[c.len() / 2];
        println!(
            "class {} conns={} depth={} rate={} mode={} samples={} qps={:.0} p50_us={} p99_us={} p999_us={} max_us={} \
             lag_p99_us={} lag_max_us={} jain={:.3} conn_min={} conn_med={} min_over_med={:.3} max_gap_ms={} errors={} outstanding={} rejected={} reconnects={}",
            cl.name, cl.conns, cl.depth, cl.rate, if cl.rate > 0.0 { "open" } else { "closed" },
            n, n as f64 / o.secs, pct(&s.hist, 0.50), pct(&s.hist, 0.99), pct(&s.hist, 0.999), s.max_us,
            if lag_n > 0 { pct(&s.lag, 0.99) } else { 0 }, if lag_n > 0 { pct(&s.lag, 1.0) } else { 0 },
            jain(&c), c[0], med, if med == 0 { 0.0 } else { c[0] as f64 / med as f64 }, gap.as_millis(), s.errors, outstanding[ci], s.rejected, s.reconnects,
        );
    }
}

fn pct(hist: &[u64], p: f64) -> u64 {
    let total: u64 = hist.iter().sum();
    if total == 0 { return 0; }
    let want = ((total as f64) * p).ceil().max(1.0) as u64;
    let mut acc = 0u64;
    for (b, &n) in hist.iter().enumerate() { acc += n; if acc >= want { return bucket_us(b); } }
    0
}

fn jain(counts: &[u64]) -> f64 {
    let sum: f64 = counts.iter().map(|&c| c as f64).sum();
    let sumsq: f64 = counts.iter().map(|&c| (c as f64) * (c as f64)).sum();
    if sumsq == 0.0 { 0.0 } else { sum * sum / (counts.len() as f64 * sumsq) }
}

/// Advance one connection without blocking: write what is owed, read responses, record each completed
/// one, and (closed loop) queue the next batch when the last one completes. At most [STEP_OPS] reads
/// and writes per call; returns true when it stopped there with work left, so one busy connection
/// (a 10 000-request closed-loop burst) cannot monopolise its thread and delay everyone else's
/// sends and receipts.
#[allow(clippy::too_many_arguments)]
fn step(
    c: &mut Conn, token: usize, classes: &[Class], out: &[u8], payload: usize, buf: &mut [u8],
    stats: &mut [ClassStats], poll: &Poll, from: Instant, to: Instant,
) -> bool {
    if c.dead { return false; }
    let cl = &classes[c.class];
    let mut ops = 0;
    loop {
        let mut progressed = false;
        while c.owed > 0 {
            let off = c.wpos % payload;
            let n = c.owed.min(out.len() - off);
            match c.sock.write(&out[off..off + n]) {
                Ok(w) => { c.owed -= w; c.wpos += w; progressed = true; ops += 1; if ops >= STEP_OPS { return true; } }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => { fail(c, token, stats, poll, from, to); return false; }
            }
        }
        loop {
            match c.sock.read(buf) {
                Ok(0) => { fail(c, token, stats, poll, from, to); return false; }
                Ok(n) => {
                    progressed = true;
                    c.partial += n;
                    let now = Instant::now();
                    while c.partial >= payload {
                        c.partial -= payload;
                        let Some(sent) = c.sched.pop_front() else { fail(c, token, stats, poll, from, to); return false; };
                        if now >= from && now < to {
                            let us = now.duration_since(sent).as_micros() as u64;
                            let s = &mut stats[c.class];
                            s.hist[bucket(us)] += 1;
                            s.max_us = s.max_us.max(us);
                            c.count += 1;
                            let sec = now.duration_since(from).as_secs() as usize;
                            if sec < TIMELINE_SECS { s.timeline[sec] += 1; }
                            let gap = now.duration_since(c.last_done);
                            if gap > c.max_gap { c.max_gap = gap; }
                            c.last_done = now;
                        }
                    }
                    if cl.rate == 0.0 && c.sched.is_empty() {
                        for _ in 0..cl.depth { c.sched.push_back(now); }
                        c.owed += cl.depth * payload;
                    }
                    ops += 1;
                    if ops >= STEP_OPS { return true; }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => { fail(c, token, stats, poll, from, to); return false; }
            }
        }
        if !progressed || c.owed == 0 { return false; }
    }
}

const STEP_OPS: usize = 8;

fn fail(c: &mut Conn, _token: usize, stats: &mut [ClassStats], poll: &Poll, from: Instant, to: Instant) {
    c.dead = true;
    let s = &mut stats[c.class];
    s.errors += 1;
    // Requests sent or owed on this connection will never be answered: rejected.
    s.rejected += c.sched.iter().filter(|&&t| t >= from && t < to).count() as u64;
    c.sched.clear();
    c.owed = 0;
    c.wpos = 0;
    c.partial = 0;
    let _ = poll.registry().deregister(&mut c.sock);
}

const RECONNECT_DELAY: Duration = Duration::from_millis(10);

/// Drop open-loop requests not yet written whose scheduled time is more than [deadline] ago
/// (oldest first; they sit right after the ones already on the wire).
fn drop_late(c: &mut Conn, classes: &[Class], payload: usize, deadline: Duration, stats: &mut [ClassStats], from: Instant, to: Instant) {
    if c.dead || classes[c.class].rate == 0.0 { return; }
    let unsent = c.owed / payload;               // whole requests not started; a partial one stays
    if unsent == 0 { return; }
    let first = c.sched.len() - unsent;
    let now = Instant::now();
    let mut k = 0;
    while k < unsent && now.duration_since(c.sched[first + k]) > deadline { k += 1; }
    if k == 0 { return; }
    let s = &mut stats[c.class];
    s.rejected += c.sched.range(first..first + k).filter(|&&t| t >= from && t < to).count() as u64;
    c.sched.drain(first..first + k);
    c.owed -= k * payload;
}

/// A connection started without waiting for the handshake (reconnects happen inside the event loop).
fn open_nonblocking(target: SocketAddr, source: Ipv4Addr) -> Option<std::net::TcpStream> {
    let s = Socket::new(Domain::IPV4, Type::STREAM, None).ok()?;
    s.bind(&SockAddr::from(SocketAddrV4::new(source, 0))).ok()?;
    s.set_nonblocking(true).ok()?;
    s.set_tcp_nodelay(true).ok()?;
    match s.connect(&SockAddr::from(target)) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) || e.kind() == ErrorKind::WouldBlock => {}
        Err(_) => return None,
    }
    Some(s.into())
}
