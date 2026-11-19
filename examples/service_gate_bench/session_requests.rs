//! Session request and loopback scenarios sharing the service gate fixture.

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
            Self::GetDerived => &fixture.last_key,
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

    fn check(self, fixture: &Fixture, id: u64, raw: &str, extended: bool) {
        let reply: Json = serde_json::from_str(raw).unwrap();
        assert_eq!(reply["id"], id, "{reply}");
        match self {
            Self::GetUnknown => assert_eq!(
                reply,
                json!({"id":id,"status":"error","error":"unknown_key"})
            ),
            Self::GetNoData => {
                assert_eq!(reply, json!({"id":id,"status":"no_data","key":UNSEEDED}))
            }
            Self::Ping => assert_eq!(reply, json!({"id":id,"status":"ok"})),
            Self::GetPhysical | Self::GetDerived => {
                assert_eq!(reply["status"], "ok", "{reply}");
                assert_eq!(reply["key"], self.key(fixture));
                assert!(reply["value"].as_f64().is_some_and(f64::is_finite));
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
) -> Sizes {
    let start = Barrier::new(mode.clients() + usize::from(mode.produces()));
    let start = &start;
    let extended = workload != Workload::Mixed;
    thread::scope(|scope| {
        let clients: Vec<_> = (0..mode.clients())
            .map(|client| {
                scope.spawn(move || {
                    let mut session = Session::new(1, MAX_OUTPUT_BYTES);
                    let mut sizes = Sizes::new();
                    record(metrics, || {
                        each_round(start, rounds, |id| {
                            let op = workload.operation(mode, client, id);
                            let frame = op.frame(fixture, id);
                            fixture
                                .service
                                .receive_message(&mut session, &frame, Instant::now())
                                .unwrap();
                            let response = session.next_message().unwrap().unwrap();
                            op.check(fixture, id, &response, extended);
                            assert!(session.next_message().unwrap().is_none());
                            sizes.record(response.len());
                        })
                    });
                    sizes
                })
            })
            .collect();
        if mode.produces() {
            fixture.produce(start, offset, rounds);
        }
        let mut sizes = Sizes::new();
        for join in clients {
            sizes.merge(join.join().unwrap());
        }
        sizes
    })
}

fn loopback(
    fixture: &Fixture,
    workload: Workload,
    mode: Mode,
    rounds: u64,
    metrics: Option<&RequestMetrics>,
) -> (Sizes, Vec<u64>) {
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
        let mut sizes = Sizes::new();
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
                workload.check(fixture, id, response, true);
                sizes.record(response.len());
            });
        }
        peer.close();
        (sizes, rtts)
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
    instrumented: bool,
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
            self.instrumented
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
        println!(",,");
    }

    fn sizes(&self, sizes: Sizes) {
        self.prefix("client", "payload", sizes.count);
        println!(",,,,,,{},{}", sizes.min, sizes.max);
    }
}

fn run_trial(nodes: usize, workload: Workload, mode: Mode, repeat: usize, instrumented: bool) {
    let rounds = if mode.network() {
        NETWORK_REQUESTS
    } else {
        ROUNDS
    };
    let (mut fixture, _port) =
        Fixture::with_catalog(Scenario::Requests, nodes, workload != Workload::Mixed);
    fixture.produce(&Barrier::new(1), 0, 1);
    if mode.network() {
        loopback(&fixture, workload, mode, WARMUP, None);
    } else {
        direct(&fixture, workload, mode, 1, WARMUP, None);
    }
    fixture.service.reset_gate_metrics();
    let metrics = instrumented.then(RequestMetrics::default);
    let offset = 1 + if mode.produces() { WARMUP } else { 0 };
    let (sizes, mut rtts) = if mode.network() {
        loopback(&fixture, workload, mode, rounds, metrics.as_ref())
    } else {
        (
            direct(&fixture, workload, mode, offset, rounds, metrics.as_ref()),
            vec![],
        )
    };
    let requests = rounds * mode.clients() as u64;
    assert_eq!(sizes.count, requests);
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
    assert_eq!(cached["status"], "ok");
    assert_eq!(
        cached["completed_at_unix_ms"],
        offset + if mode.produces() { rounds } else { 0 }
    );

    let report = Report {
        nodes,
        workload,
        mode,
        repeat,
        rounds,
        instrumented,
    };
    for m in gate.into_iter().filter(|m| m.count != 0) {
        report.timing("gate_wait", m.operation, m.count, m.wait);
        report.timing("gate_hold", m.operation, m.count, m.hold);
    }
    if let Some(metrics) = metrics {
        let phases = metrics.snapshot();
        for expected in ["parse", "dispatch", "response"] {
            assert_eq!(
                phases.iter().find(|m| m.phase == expected).unwrap().count,
                requests
            );
        }
        for m in phases.into_iter().filter(|m| m.count != 0) {
            report.timing("phase", m.phase, m.count, m.timing);
        }
    }
    if !rtts.is_empty() {
        report.timing("client", "rtt", requests, rtt_timing(&mut rtts));
    }
    report.sizes(sizes);
}

fn print_header() {
    println!(
        "nodes,workload,mode,repeat,clients,rounds,instrumented,kind,metric,count,p50_us,p95_us,p99_us,max_us,total_us,min_bytes,max_bytes"
    );
}

pub(super) fn benchmark() {
    print_header();
    for nodes in [1, 8, 32, 128] {
        for workload in Workload::ALL {
            for mode in [Mode::Solo, Mode::WithProducer, Mode::Many] {
                for repeat in 1..=REPEATS {
                    run_trial(nodes, workload, mode, repeat, true);
                }
            }
        }
        for workload in Workload::NETWORK {
            for mode in [Mode::Tcp, Mode::WebSocket] {
                for repeat in 1..=REPEATS {
                    run_trial(nodes, workload, mode, repeat, true);
                }
            }
        }
    }
}

pub(super) fn calibrate() {
    print_header();
    for nodes in [1, 128] {
        for workload in Workload::NETWORK {
            for repeat in 1..=REPEATS {
                let first = repeat.is_multiple_of(2);
                for instrumented in [first, !first] {
                    run_trial(nodes, workload, Mode::Solo, repeat, instrumented);
                }
            }
        }
    }
}
