//! Session request and loopback scenarios sharing the service gate fixture.

use std::hash::{DefaultHasher, Hash, Hasher};

use super::{
    AtomicBool, Barrier, BufReader, Fixture, Instant, Json, MAX_OUTPUT_BYTES, Message, PHYSICAL,
    Peer, REPEATS, ROUNDS, Scenario, Session, WARMUP, Write, each_round, json, loopback_pair,
    thread, websocket, websocket_peer,
};
use lab_server::service::GateTiming;
use lab_server::session::metrics::RequestMetrics;

const NETWORK_REQUESTS: u64 = 48;
const UNSEEDED: &str = "device/unseeded";
const MISSING: &str = "device/missing";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Ping,
    GetPhysical,
    GetDerived,
    GetNoData,
    GetDerivedNoData,
    GetUnknown,
    Describe,
    Mixed,
}

impl Workload {
    const ALL: [Self; 7] = [
        Self::Ping,
        Self::GetPhysical,
        Self::GetDerived,
        Self::GetNoData,
        Self::GetUnknown,
        Self::Describe,
        Self::Mixed,
    ];
    const NETWORK: [Self; 3] = [Self::Ping, Self::GetDerived, Self::Describe];

    fn label(self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::GetPhysical => "get_physical",
            Self::GetDerived => "get_derived",
            Self::GetNoData => "get_no_data",
            Self::GetDerivedNoData => "get_derived_no_data",
            Self::GetUnknown => "get_unknown_key",
            Self::Describe => "describe",
            Self::Mixed => "mixed",
        }
    }

    fn operation(self, mode: Mode, client: usize, id: u64) -> Self {
        if self != Self::Mixed {
            return self;
        }
        let index = if mode == Mode::Many {
            client
        } else {
            (id as usize - 1) % 3
        };
        Self::NETWORK[index]
    }

    fn key(self, fixture: &Fixture) -> &str {
        match self {
            Self::GetPhysical => PHYSICAL,
            Self::GetDerived | Self::GetDerivedNoData => &fixture.last_key,
            Self::GetNoData => UNSEEDED,
            Self::GetUnknown => MISSING,
            _ => unreachable!("only get needs a key"),
        }
    }

    fn frame(self, fixture: &Fixture, id: u64) -> String {
        let json = match self {
            Self::Ping => json!({"id":id,"op":"ping"}),
            Self::Describe => json!({"id":id,"op":"describe"}),
            _ => json!({"id":id,"op":"get","key":self.key(fixture)}),
        };
        json.to_string()
    }

    fn check(self, fixture: &Fixture, id: u64, raw: &str, extended: bool) -> Option<u64> {
        let reply: Json = serde_json::from_str(raw).unwrap();
        assert_eq!(reply["id"], id, "{reply}");
        match self {
            Self::GetUnknown => assert_eq!(
                reply,
                json!({"id":id,"status":"error","error":"unknown_key"})
            ),
            Self::GetNoData | Self::GetDerivedNoData => {
                assert_eq!(
                    reply,
                    json!({"id":id,"status":"no_data","key":self.key(fixture)})
                )
            }
            Self::Ping => assert_eq!(reply, json!({"id":id,"status":"ok"})),
            Self::GetPhysical | Self::GetDerived => {
                assert_eq!(reply["status"], "ok", "{reply}");
                assert_eq!(reply["key"], self.key(fixture));
                assert!(reply["value"].as_f64().is_some_and(f64::is_finite));
                assert!((0.0..=99.0).contains(&reply["value"].as_f64().unwrap()));
                if self == Self::GetPhysical {
                    assert_eq!(
                        reply["value"].as_f64().unwrap(),
                        (reply["completed_at_unix_ms"].as_u64().unwrap() % 100) as f64
                    );
                }
            }
            Self::Describe => {
                assert_eq!(reply["status"], "ok", "{reply}");
                let parameters = reply["parameters"].as_array().unwrap();
                assert_eq!(
                    parameters.len(),
                    fixture.outputs + 1 + usize::from(extended)
                );
                let keys: Vec<_> = parameters
                    .iter()
                    .map(|p| p["key"].as_str().unwrap())
                    .collect();
                assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
                assert!(keys.contains(&PHYSICAL) && keys.contains(&fixture.last_key.as_str()));
                let actions = if extended {
                    json!([
                        {"key":"device/a_reset","description":"Mock a_reset"},
                        {"key":"device/z_reset","description":"Mock z_reset"},
                    ])
                } else {
                    json!([])
                };
                assert_eq!(reply["actions"], actions);
            }
            Self::Mixed => unreachable!("mixed expands to individual requests"),
        }
        reply["completed_at_unix_ms"].as_u64()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Solo,
    WithProducer,
    Many,
    Tcp,
    WebSocket,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Solo => "solo",
            Self::WithProducer => "producer",
            Self::Many => "many",
            Self::Tcp => "tcp",
            Self::WebSocket => "websocket",
        }
    }

    fn clients(self) -> usize {
        if self == Self::Many { 3 } else { 1 }
    }

    fn produces(self) -> bool {
        matches!(self, Self::WithProducer | Self::Many)
    }

    fn network(self) -> bool {
        matches!(self, Self::Tcp | Self::WebSocket)
    }
}

struct Sizes {
    count: u64,
    min: usize,
    max: usize,
}

impl Sizes {
    fn new() -> Self {
        Self {
            count: 0,
            min: usize::MAX,
            max: 0,
        }
    }

    fn record(&mut self, size: usize) {
        self.count += 1;
        self.min = self.min.min(size);
        self.max = self.max.max(size);
    }

    fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}

struct Replies {
    all: Sizes,
    describe: Sizes,
    describe_digest: u64,
}

impl Replies {
    fn new() -> Self {
        Self {
            all: Sizes::new(),
            describe: Sizes::new(),
            describe_digest: 0,
        }
    }

    fn record(&mut self, body: &str, fingerprint: bool) {
        self.all.record(body.len());
        if fingerprint {
            let mut hasher = DefaultHasher::new();
            body.hash(&mut hasher);
            // Merge the same replies from concurrent clients regardless of arrival order.
            self.describe_digest = self.describe_digest.wrapping_add(hasher.finish());
            self.describe.record(body.len());
        }
    }

    fn merge(&mut self, other: Self) {
        self.all.merge(other.all);
        self.describe.merge(other.describe);
        self.describe_digest = self.describe_digest.wrapping_add(other.describe_digest);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Recording {
    GateOnly,
    Phases,
    DescribeComparison,
}

fn record<R>(metrics: Option<&RequestMetrics>, run: impl FnOnce() -> R) -> R {
    if let Some(metrics) = metrics {
        metrics.record_on_thread(run)
    } else {
        run()
    }
}

fn direct(
    fixture: &Fixture,
    workload: Workload,
    mode: Mode,
    offset: u64,
    rounds: u64,
    metrics: Option<&RequestMetrics>,
    fingerprint: bool,
) -> Replies {
    let start = Barrier::new(mode.clients() + usize::from(mode.produces()));
    let start = &start;
    let extended = workload != Workload::Mixed;
    thread::scope(|scope| {
        let clients: Vec<_> = (0..mode.clients())
            .map(|client| {
                scope.spawn(move || {
                    let mut session = Session::new(1, MAX_OUTPUT_BYTES);
                    let mut replies = Replies::new();
                    let mut previous = 0;
                    record(metrics, || {
                        each_round(start, rounds, |id| {
                            let op = workload.operation(mode, client, id);
                            let frame = op.frame(fixture, id);
                            fixture
                                .service
                                .receive_message(&mut session, &frame, Instant::now())
                                .unwrap();
                            let response = session.next_message().unwrap().unwrap();
                            if let Some(sequence) = op.check(fixture, id, &response, extended) {
                                assert!(sequence >= previous);
                                previous = sequence;
                                if mode.produces() {
                                    assert!((offset + id - 1..=offset + id).contains(&sequence));
                                } else {
                                    assert_eq!(sequence, 1);
                                }
                            }
                            assert!(session.next_message().unwrap().is_none());
                            replies.record(&response, fingerprint && op == Workload::Describe);
                        })
                    });
                    replies
                })
            })
            .collect();
        if mode.produces() {
            fixture.produce(start, offset, rounds);
        }
        let mut replies = Replies::new();
        for join in clients {
            replies.merge(join.join().unwrap());
        }
        replies
    })
}

fn loopback(
    fixture: &Fixture,
    workload: Workload,
    mode: Mode,
    rounds: u64,
    metrics: Option<&RequestMetrics>,
    fingerprint: bool,
) -> (Replies, Vec<u64>) {
    let (client, server) = loopback_pair();
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        let stop = &stop;
        scope.spawn(move || {
            record(metrics, || match mode {
                Mode::Tcp => fixture.service.serve_connection(server).unwrap(),
                Mode::WebSocket => {
                    websocket::serve_connection(server, &fixture.service, &[], stop).unwrap()
                }
                _ => unreachable!("loopback only"),
            })
        });
        let mut peer = if mode == Mode::Tcp {
            Peer::Tcp(BufReader::new(client))
        } else {
            websocket_peer(client)
        };
        let mut replies = Replies::new();
        let mut rtts = Vec::with_capacity(rounds as usize);
        for id in 1..=rounds {
            let frame = workload.frame(fixture, id);
            let sent = Instant::now();
            match &mut peer {
                Peer::Tcp(stream) => writeln!(stream.get_mut(), "{frame}").unwrap(),
                Peer::WebSocket(socket) => socket.send(Message::Text(frame.into())).unwrap(),
            }
            peer.with_received_text(|response| {
                rtts.push(sent.elapsed().as_nanos().try_into().unwrap());
                if let Some(sequence) = workload.check(fixture, id, response, true) {
                    assert_eq!(sequence, 1);
                }
                replies.record(response, fingerprint && workload == Workload::Describe);
            });
        }
        peer.close();
        (replies, rtts)
    })
}

fn rtt_timing(samples: &mut [u64]) -> GateTiming {
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    GateTiming {
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        p99_ns: percentile(99),
        max_ns: *samples.last().unwrap(),
        total_ns: samples.iter().sum(),
    }
}

struct Report {
    nodes: usize,
    workload: Workload,
    mode: Mode,
    repeat: usize,
    rounds: u64,
    recording: Recording,
}

impl Report {
    fn prefix(&self, kind: &str, metric: &str, count: u64) {
        print!(
            "{},{},{},{},{},{},{},{kind},{metric},{count}",
            self.nodes,
            self.workload.label(),
            self.mode.label(),
            self.repeat,
            self.mode.clients(),
            self.rounds,
            self.recording != Recording::GateOnly
        );
    }

    fn timing(&self, kind: &str, metric: &str, count: u64, timing: GateTiming) {
        self.prefix(kind, metric, count);
        for ns in [
            timing.p50_ns,
            timing.p95_ns,
            timing.p99_ns,
            timing.max_ns,
            timing.total_ns,
        ] {
            print!(",{:.3}", ns as f64 / 1000.0);
        }
        print!(",,");
        if self.recording == Recording::DescribeComparison {
            print!(",");
        }
        println!();
    }

    fn sizes(&self, metric: &str, sizes: Sizes, digest: Option<u64>) {
        self.prefix("client", metric, sizes.count);
        print!(",,,,,,{},{}", sizes.min, sizes.max);
        if self.recording == Recording::DescribeComparison {
            print!(",");
            if let Some(digest) = digest {
                print!("{digest:016x}");
            }
        }
        println!();
    }
}

fn run_trial(nodes: usize, workload: Workload, mode: Mode, repeat: usize, recording: Recording) {
    let rounds = if mode.network() {
        NETWORK_REQUESTS
    } else {
        ROUNDS
    };
    let (mut fixture, _port) =
        Fixture::with_catalog(Scenario::Requests, nodes, workload != Workload::Mixed);
    let seeded = workload != Workload::GetDerivedNoData;
    if seeded {
        fixture.produce(&Barrier::new(1), 0, 1);
    }
    if mode.network() {
        loopback(&fixture, workload, mode, WARMUP, None, false);
    } else {
        direct(&fixture, workload, mode, 1, WARMUP, None, false);
    }
    fixture.service.reset_gate_metrics();
    let metrics = (recording != Recording::GateOnly).then(RequestMetrics::default);
    let fingerprint = recording == Recording::DescribeComparison;
    let offset = 1 + if mode.produces() { WARMUP } else { 0 };
    let (replies, mut rtts) = if mode.network() {
        loopback(
            &fixture,
            workload,
            mode,
            rounds,
            metrics.as_ref(),
            fingerprint,
        )
    } else {
        (
            direct(
                &fixture,
                workload,
                mode,
                offset,
                rounds,
                metrics.as_ref(),
                fingerprint,
            ),
            vec![],
        )
    };
    let requests = rounds * mode.clients() as u64;
    assert_eq!(replies.all.count, requests);
    let gate = fixture.service.gate_metrics();
    let count = |operation| {
        gate.iter()
            .find(|m| m.operation == operation)
            .unwrap()
            .count
    };
    assert_eq!(
        count("apply_event"),
        if mode.produces() { rounds } else { 0 }
    );
    if mode == Mode::Tcp {
        assert!(count("tcp_drive") >= requests);
    } else {
        assert_eq!(count("receive_message"), requests);
    }
    let mut verifier = Session::new(1, MAX_OUTPUT_BYTES);
    let cached = super::call(
        &fixture.service,
        &mut verifier,
        json!({
            "id":1,"op":"get","key":fixture.last_key
        }),
    );
    if seeded {
        assert_eq!(cached["status"], "ok");
        assert_eq!(
            cached["completed_at_unix_ms"],
            offset + if mode.produces() { rounds } else { 0 }
        );
    } else {
        assert_eq!(cached["status"], "no_data");
    }

    let report = Report {
        nodes,
        workload,
        mode,
        repeat,
        rounds,
        recording,
    };
    for m in gate.into_iter().filter(|m| m.count != 0) {
        report.timing("gate_wait", m.operation, m.count, m.wait);
        report.timing("gate_hold", m.operation, m.count, m.hold);
    }
    if let Some(metrics) = metrics {
        let phases = metrics.snapshot();
        let count = |name| {
            phases
                .iter()
                .find(|m| m.phase == name)
                .map_or(0, |m| m.count)
        };
        let operations = (0..mode.clients())
            .flat_map(|client| (1..=rounds).map(move |id| workload.operation(mode, client, id)));
        let gets = operations
            .clone()
            .filter(|op| {
                !matches!(
                    op,
                    Workload::Ping | Workload::Describe | Workload::GetUnknown
                )
            })
            .count() as u64;
        assert_eq!(count("availability"), gets);
        assert_eq!(
            count("snapshot_build"),
            operations.filter(|op| *op != Workload::Ping).count() as u64
        );
        for expected in ["parse", "dispatch", "response"] {
            assert_eq!(
                phases.iter().find(|m| m.phase == expected).unwrap().count,
                requests
            );
        }
        if fingerprint {
            assert_eq!(
                phases.iter().find(|m| m.phase == "describe").unwrap().count,
                replies.describe.count
            );
        }
        for m in phases.into_iter().filter(|m| m.count != 0) {
            report.timing("phase", m.phase, m.count, m.timing);
        }
    }
    if !rtts.is_empty() {
        report.timing("client", "rtt", requests, rtt_timing(&mut rtts));
    }
    report.sizes("payload", replies.all, None);
    if replies.describe.count != 0 {
        report.sizes(
            "describe_payload",
            replies.describe,
            Some(replies.describe_digest),
        );
    }
}

fn print_header(comparison: bool) {
    print!(
        "nodes,workload,mode,repeat,clients,rounds,instrumented,kind,metric,count,p50_us,p95_us,p99_us,max_us,total_us,min_bytes,max_bytes"
    );
    if comparison {
        print!(",digest");
    }
    println!();
}

pub(super) fn benchmark() {
    print_header(false);
    for nodes in [1, 8, 32, 128] {
        for workload in Workload::ALL {
            for mode in [Mode::Solo, Mode::WithProducer, Mode::Many] {
                for repeat in 1..=REPEATS {
                    run_trial(nodes, workload, mode, repeat, Recording::Phases);
                }
            }
        }
        for workload in Workload::NETWORK {
            for mode in [Mode::Tcp, Mode::WebSocket] {
                for repeat in 1..=REPEATS {
                    run_trial(nodes, workload, mode, repeat, Recording::Phases);
                }
            }
        }
    }
}

pub(super) fn calibrate() {
    print_header(false);
    for nodes in [1, 128] {
        for workload in Workload::NETWORK {
            for repeat in 1..=REPEATS {
                let first = repeat.is_multiple_of(2);
                for instrumented in [first, !first] {
                    let recording = if instrumented {
                        Recording::Phases
                    } else {
                        Recording::GateOnly
                    };
                    run_trial(nodes, workload, Mode::Solo, repeat, recording);
                }
            }
        }
    }
}

pub(super) fn compare_describe(pair: usize) {
    print_header(true);
    let recording = Recording::DescribeComparison;
    for nodes in [8, 32, 128] {
        for mode in [Mode::Solo, Mode::WithProducer, Mode::Many] {
            run_trial(nodes, Workload::Describe, mode, pair, recording);
        }
        run_trial(nodes, Workload::Mixed, Mode::Many, pair, recording);
        for workload in Workload::NETWORK {
            if workload != Workload::Describe {
                for mode in [Mode::Solo, Mode::Many] {
                    run_trial(nodes, workload, mode, pair, recording);
                }
            }
            for mode in [Mode::Tcp, Mode::WebSocket] {
                run_trial(nodes, workload, mode, pair, recording);
            }
        }
    }
}

pub(super) fn compare_get(pair: usize) {
    print_header(false);
    for nodes in [8, 32, 128] {
        for mode in [
            Mode::Solo,
            Mode::WithProducer,
            Mode::Many,
            Mode::Tcp,
            Mode::WebSocket,
        ] {
            run_trial(nodes, Workload::GetDerived, mode, pair, Recording::Phases);
        }
        run_trial(
            nodes,
            Workload::GetDerivedNoData,
            Mode::Solo,
            pair,
            Recording::Phases,
        );
        run_trial(
            nodes,
            Workload::GetUnknown,
            Mode::Solo,
            pair,
            Recording::Phases,
        );
        run_trial(nodes, Workload::Mixed, Mode::Many, pair, Recording::Phases);
        for workload in [Workload::GetPhysical, Workload::Ping, Workload::Describe] {
            for mode in [Mode::Solo, Mode::Many] {
                run_trial(nodes, workload, mode, pair, Recording::Phases);
            }
        }
    }
}
