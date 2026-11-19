//! Hardware-free service lock experiment. See docs/service-gate-performance.md.

use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Barrier, atomic::AtomicBool};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use lab_server::command_router::CommandRouter;
use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::{DeviceIo, Driver, DriverError};
use lab_server::model::{
    ActionDescription, Observation, ParameterDescription, SourceMetadata, Spec, Value,
};
use lab_server::observation_aggregator::AggregatorEvent;
use lab_server::port_worker::{DeviceInstance, PortChannels, PortWorker};
use lab_server::processing::{Configuration, Management, Parameters, signal};
use lab_server::service::{DeviceService, GateMetric};
use lab_server::session::Session;
use lab_server::websocket::{self, MAX_OUTPUT_BYTES, PATH, SUBPROTOCOL};
use serde_json::{Value as Json, json};
use tungstenite::{Message, WebSocket, client::IntoClientRequest};

#[path = "service_gate_bench/session_requests.rs"]
mod session_requests;

const ROUNDS: u64 = 256;
const NETWORK_ROUNDS: u64 = 64;
const WARMUP: u64 = 8;
const REPEATS: usize = 5;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const PHYSICAL: &str = "device/value";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Baseline,
    Requests,
    Retune,
    Rewire,
    Watchdog,
    TreeRequests,
    ShadowPid,
    Loopback,
    LoopbackSubscribed,
}

impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Requests => "requests",
            Self::Retune => "retune",
            Self::Rewire => "rewire",
            Self::Watchdog => "watchdog",
            Self::TreeRequests => "tree_requests",
            Self::ShadowPid => "shadow_pid",
            Self::Loopback => "loopback",
            Self::LoopbackSubscribed => "loopback_subscribed",
        }
    }

    fn network(self) -> bool {
        matches!(self, Self::Loopback | Self::LoopbackSubscribed)
    }

    fn editing(self) -> bool {
        matches!(self, Self::Retune | Self::Rewire)
    }

    fn watchdog(self) -> bool {
        matches!(self, Self::Watchdog | Self::ShadowPid)
    }
}

struct Mock {
    request_catalog: bool,
}

impl Driver for Mock {
    fn parameters(&self) -> Vec<ParameterDescription> {
        let mut parameters = vec![ParameterDescription {
            id: "value".into(),
            spec: Spec::Float {
                min: 0.0,
                max: 100.0,
            },
            unit: "C".into(),
            writable: false,
            default_poll_period: Some(Duration::from_millis(10)),
        }];
        if self.request_catalog {
            parameters.push(ParameterDescription {
                id: "unseeded".into(),
                ..parameters[0].clone()
            });
        }
        parameters
    }

    fn actions(&self) -> Vec<ActionDescription> {
        if self.request_catalog {
            ["z_reset", "a_reset"]
                .map(|name| ActionDescription {
                    name: name.into(),
                    description: format!("Mock {name}"),
                })
                .into()
        } else {
            vec![]
        }
    }

    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        panic!("the benchmark supplies measurements at the aggregator boundary");
    }

    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("the benchmark must not write");
    }
}

fn node(index: usize) -> String {
    format!("node{index:03}")
}

fn configuration(index: usize, kind: &str, input: &str, parameters: Parameters) -> Configuration {
    Configuration {
        name: node(index),
        type_id: kind.into(),
        inputs: [("input".into(), input.into())].into(),
        parameters,
        max_age_ms: 86_400_000,
    }
}

struct Fixture {
    service: DeviceService,
    source_instance: u64,
    origin: Instant,
    nodes: usize,
    outputs: usize,
    last_key: String,
}

struct IdlePort {
    _worker: PortWorker<BufReader<Cursor<Vec<u8>>>>,
    _channels: PortChannels,
}

impl Fixture {
    fn new(scenario: Scenario, nodes: usize) -> (Self, IdlePort) {
        Self::with_catalog(scenario, nodes, false)
    }

    fn with_catalog(scenario: Scenario, nodes: usize, request_catalog: bool) -> (Self, IdlePort) {
        let device = DeviceInstance::new("device", Mock { request_catalog }).unwrap();
        let (worker, channels) =
            PortWorker::new(BufReader::new(Cursor::new(vec![])), vec![device], 8, 8).unwrap();
        let mut registry = DeviceRegistry::new();
        registry
            .register_port("port", channels.commands.clone())
            .unwrap();
        registry
            .register_device("port", &worker.devices()[0])
            .unwrap();
        let service = DeviceService::new(CommandRouter::new(registry), 64);
        let pid = scenario == Scenario::ShadowPid;
        for index in 0..nodes {
            let input = if index == 0 {
                PHYSICAL.into()
            } else {
                let parent = if scenario == Scenario::TreeRequests {
                    (index - 1) / 2
                } else {
                    index - 1
                };
                signal(&node(parent), "value")
            };
            let kind = if pid && index == nodes - 1 {
                "pid"
            } else {
                "ema"
            };
            let snapshot = service
                .manage(Management::Create {
                    configuration: configuration(index, kind, &input, Parameters::new()),
                    expected_revision: index as u64,
                })
                .unwrap();
            assert_eq!(snapshot["revision"], index as u64 + 1);
        }
        (
            Self {
                service,
                source_instance: channels.commands.source_instance(),
                origin: Instant::now(),
                nodes,
                outputs: nodes + if pid { 6 } else { 0 },
                last_key: signal(&node(nodes - 1), if pid { "output" } else { "value" }),
            },
            IdlePort {
                _worker: worker,
                _channels: channels,
            },
        )
    }

    fn produce(&self, barrier: &Barrier, offset: u64, rounds: u64) {
        let mut previous = Instant::now();
        each_round(barrier, rounds, |round| {
            let sequence = offset + round;
            let at = Instant::now().max(previous + Duration::from_nanos(1));
            previous = at;
            let mut records = 0;
            self.service
                .apply_event_recorded(
                    AggregatorEvent::Measurement {
                        port_id: "port".into(),
                        global_key: PHYSICAL.into(),
                        observation: Observation {
                            source: "value".into(),
                            unit: "C".into(),
                            completed_at: UNIX_EPOCH + Duration::from_millis(sequence),
                            reading: Ok(Value::Float((sequence % 100) as f64)),
                            metadata: Some(SourceMetadata {
                                source_instance: self.source_instance,
                                sequence,
                                generation: 0,
                                acquired_at: at,
                                elapsed: at.duration_since(self.origin),
                            }),
                        },
                    },
                    |_| records += 1,
                )
                .unwrap();
            assert_eq!(records, self.outputs + 1, "sample {sequence}");
        });
    }

    fn edit(&self, scenario: Scenario, round: u64, expected: u64) {
        let root_output = signal(&node(0), "value");
        let command = if scenario == Scenario::Rewire {
            let input = if round % 2 == 1 {
                PHYSICAL
            } else {
                &root_output
            };
            Management::Configure {
                configuration: configuration(1, "ema", input, Parameters::new()),
                expected_revision: expected,
            }
        } else if round % 2 == 1 {
            Management::ParameterSet {
                name: node(0),
                parameter: "tau".into(),
                value: json!(2.0),
                expected_revision: Some(expected),
            }
        } else {
            Management::Configure {
                configuration: configuration(0, "ema", PHYSICAL, Parameters::new()),
                expected_revision: expected,
            }
        };
        let reply = self.service.manage(command).unwrap();
        assert_eq!(reply["revision"], expected + 1);
    }
}

// Keep rendezvous balanced if a correctness assertion or I/O check fails.
fn each_round(barrier: &Barrier, rounds: u64, mut operation: impl FnMut(u64)) {
    let mut result = Ok(());
    for round in 1..=rounds {
        barrier.wait();
        if result.is_ok() {
            result = catch_unwind(AssertUnwindSafe(|| operation(round)));
        }
    }
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}

#[derive(Clone, Copy)]
enum Query {
    Ping,
    Get,
    Describe,
}

impl Query {
    const ALL: [Self; 3] = [Self::Ping, Self::Get, Self::Describe];

    fn frame(self, id: u64, key: &str) -> Json {
        match self {
            Self::Ping => json!({"id":id,"op":"ping"}),
            Self::Get => json!({"id":id,"op":"get","key":key}),
            Self::Describe => json!({"id":id,"op":"describe"}),
        }
    }

    fn check(self, reply: &Json, id: u64, fixture: &Fixture) {
        assert_eq!(reply["id"], id, "{reply}");
        assert_eq!(reply["status"], "ok", "{reply}");
        match self {
            Self::Ping => {}
            Self::Get => {
                assert_eq!(reply["key"], fixture.last_key);
                assert!(reply["value"].as_f64().is_some_and(f64::is_finite));
            }
            Self::Describe => {
                let parameters = reply["parameters"].as_array().unwrap();
                assert_eq!(parameters.len(), fixture.outputs + 1);
                assert!(parameters.iter().any(|p| p["key"] == fixture.last_key));
            }
        }
    }
}

fn call(service: &DeviceService, session: &mut Session, request: Json) -> Json {
    service
        .receive_message(session, &request.to_string(), Instant::now())
        .unwrap();
    let reply = serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
    assert!(session.next_message().unwrap().is_none());
    reply
}

fn direct(fixture: &Fixture, scenario: Scenario, offset: u64, rounds: u64) {
    let requests = matches!(
        scenario,
        Scenario::Requests | Scenario::TreeRequests | Scenario::Watchdog | Scenario::ShadowPid
    );
    let others = usize::from(scenario.editing() || scenario.watchdog());
    let barrier = Barrier::new(1 + 3 * usize::from(requests) + others);
    let barrier = &barrier;
    thread::scope(|scope| {
        if requests {
            for query in Query::ALL {
                scope.spawn(move || {
                    let mut session = Session::new(1, MAX_OUTPUT_BYTES);
                    each_round(barrier, rounds, |id| {
                        let reply = call(
                            &fixture.service,
                            &mut session,
                            query.frame(id, &fixture.last_key),
                        );
                        query.check(&reply, id, fixture);
                    });
                });
            }
        }
        if scenario.editing() {
            scope.spawn(move || {
                each_round(barrier, rounds, |round| {
                    fixture.edit(scenario, round, fixture.nodes as u64 + offset + round - 2);
                });
            });
        }
        if scenario.watchdog() {
            scope.spawn(move || {
                each_round(barrier, rounds, |round| {
                    if (round - 1).is_multiple_of(8) {
                        fixture
                            .service
                            .tick_recorded(|_| panic!("sample went stale"))
                            .unwrap();
                    }
                });
            });
        }
        fixture.produce(barrier, offset, rounds);
    });
}

enum Peer {
    Tcp(BufReader<TcpStream>),
    WebSocket(Box<WebSocket<TcpStream>>),
}

impl Peer {
    fn send(&mut self, json: Json) {
        match self {
            Self::Tcp(stream) => writeln!(stream.get_mut(), "{json}").unwrap(),
            Self::WebSocket(socket) => socket.send(Message::Text(json.to_string().into())).unwrap(),
        }
    }

    fn receive(&mut self) -> Json {
        self.with_received_text(|text| serde_json::from_str(text).unwrap())
    }

    fn with_received_text<T>(&mut self, receive: impl FnOnce(&str) -> T) -> T {
        match self {
            Self::Tcp(stream) => {
                let mut line = String::new();
                assert!(
                    stream.read_line(&mut line).unwrap() > 0,
                    "unexpected TCP EOF"
                );
                receive(line.strip_suffix('\n').unwrap())
            }
            Self::WebSocket(socket) => {
                let Message::Text(text) = socket.read().unwrap() else {
                    panic!("expected WebSocket text");
                };
                receive(&text)
            }
        }
    }

    fn close(self) {
        match self {
            Self::Tcp(mut stream) => {
                stream.get_mut().shutdown(Shutdown::Write).unwrap();
                assert_eq!(stream.read_line(&mut String::new()).unwrap(), 0);
            }
            Self::WebSocket(mut socket) => {
                socket.close(None).unwrap();
                assert!(matches!(socket.read(), Ok(Message::Close(_))));
            }
        }
    }
}

fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let server = listener.accept().unwrap().0;
    peer.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
    peer.set_write_timeout(Some(IO_TIMEOUT)).unwrap();
    peer.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    (peer, server)
}

fn websocket_peer(stream: TcpStream) -> Peer {
    let mut request = format!("ws://{}{PATH}", stream.peer_addr().unwrap())
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
    let (socket, reply) = tungstenite::client(request, stream).unwrap();
    assert_eq!(reply.headers()["sec-websocket-protocol"], SUBPROTOCOL);
    Peer::WebSocket(Box::new(socket))
}

fn network(fixture: &Fixture, subscribed: bool, offset: u64, rounds: u64) {
    let barrier = Barrier::new(3);
    let stop = AtomicBool::new(false);
    let (tcp_peer, tcp_server) = loopback_pair();
    let (ws_peer, ws_server) = loopback_pair();
    thread::scope(|scope| {
        scope.spawn(move || fixture.service.serve_connection(tcp_server).unwrap());
        let stop = &stop;
        scope.spawn(move || {
            websocket::serve_connection(ws_server, &fixture.service, &[], stop).unwrap()
        });
        let mut peers = [Peer::Tcp(BufReader::new(tcp_peer)), websocket_peer(ws_peer)];
        if subscribed {
            for peer in &mut peers {
                peer.send(json!({"id":1,"op":"subscribe","key":fixture.last_key}));
                assert_eq!(
                    peer.receive(),
                    json!({"id":1,"status":"ok","keys":[fixture.last_key]})
                );
            }
        }
        let barrier = &barrier;
        for (index, mut peer) in peers.into_iter().enumerate() {
            scope.spawn(move || {
                each_round(barrier, rounds, |round| {
                    let id = round + u64::from(subscribed);
                    let query = Query::ALL[(round as usize + index - 1) % Query::ALL.len()];
                    peer.send(query.frame(id, &fixture.last_key));
                    let mut response = false;
                    let mut observation = !subscribed;
                    while !response || !observation {
                        let message = peer.receive();
                        if message["event"] == "measurement" {
                            assert!(!observation, "unexpected measurement {message}");
                            assert_eq!(message["key"], fixture.last_key);
                            assert_eq!(message["status"], "ok");
                            assert_eq!(message["completed_at_unix_ms"], offset + round);
                            observation = true;
                        } else {
                            assert!(!response, "unexpected reply {message}");
                            query.check(&message, id, fixture);
                            response = true;
                        }
                    }
                });
                peer.close();
            });
        }
        fixture.produce(barrier, offset, rounds);
    });
}

fn run(fixture: &Fixture, scenario: Scenario, offset: u64, rounds: u64) {
    if scenario.network() {
        network(
            fixture,
            scenario == Scenario::LoopbackSubscribed,
            offset,
            rounds,
        );
    } else {
        direct(fixture, scenario, offset, rounds);
    }
}

fn run_case(scenario: Scenario, nodes: usize, repetitions: usize) {
    let rounds = if scenario.network() {
        NETWORK_ROUNDS
    } else {
        ROUNDS
    };
    for repeat in 1..=repetitions {
        let (mut fixture, _port) = Fixture::new(scenario, nodes);
        // Seed the caches before allowing clients to read the derived signals.
        fixture.produce(&Barrier::new(1), 0, 1);
        // Warm sampling starts at sequence 2. Editing starts at revision nodes.
        run(&fixture, scenario, 1, WARMUP);
        fixture.service.reset_gate_metrics();
        run(&fixture, scenario, 1 + WARMUP, rounds);

        let mut snapshot = fixture.service.gate_metrics();
        let sample = snapshot
            .iter()
            .find(|m| m.operation == "apply_event")
            .unwrap();
        assert_eq!(sample.count, rounds, "one acquisition per sample");
        // The last rewire may follow the last measured sample. Probe that
        // committed graph with a fresh sample outside the measured series.
        let mut last_sequence = 1 + WARMUP + rounds;
        if scenario == Scenario::Rewire {
            fixture.produce(&Barrier::new(1), last_sequence, 1);
            last_sequence += 1;
        }
        let mut verifier = Session::new(1, MAX_OUTPUT_BYTES);
        let last = call(
            &fixture.service,
            &mut verifier,
            Query::Get.frame(1, &fixture.last_key),
        );
        Query::Get.check(&last, 1, &fixture);
        assert_eq!(last["completed_at_unix_ms"], last_sequence);
        if scenario == Scenario::ShadowPid {
            let last_page = call(
                &fixture.service,
                &mut verifier,
                json!({
                    "id":2,"op":"components","command":{"action":"list","offset":nodes-1,"limit":1}
                }),
            );
            assert_eq!(last_page["data"]["instances"][0]["state"], "shadow");
        }
        fixture.service.stop_controls();
        let stopped = fixture.service.gate_metrics();
        *snapshot
            .iter_mut()
            .find(|m| m.operation == "stop_controls")
            .unwrap() = *stopped
            .iter()
            .find(|m| m.operation == "stop_controls")
            .unwrap();
        for metric in snapshot.into_iter().filter(|m| m.count != 0) {
            print_row(scenario, nodes, repeat, rounds, metric);
        }
    }
}

fn print_row(scenario: Scenario, nodes: usize, repeat: usize, rounds: u64, metric: GateMetric) {
    print!(
        "{},{nodes},{repeat},{rounds},{},{}",
        scenario.label(),
        metric.operation,
        metric.count
    );
    for timing in [metric.wait, metric.hold] {
        for ns in [
            timing.p50_ns,
            timing.p95_ns,
            timing.p99_ns,
            timing.max_ns,
            timing.total_ns,
        ] {
            print!(",{:.3}", ns as f64 / 1000.0);
        }
    }
    println!();
}

fn main() {
    let comparison = match std::env::args().nth(1).as_deref() {
        None => false,
        Some("--rewire-comparison") => true,
        Some("--session-request-phases") => return session_requests::benchmark(),
        Some("--session-request-calibration") => return session_requests::calibrate(),
        Some(argument) => panic!("unknown argument {argument}"),
    };
    println!(
        "scenario,nodes,repeat,rounds,operation,count,wait_p50_us,wait_p95_us,wait_p99_us,wait_max_us,total_wait_us,hold_p50_us,hold_p95_us,hold_p99_us,hold_max_us,total_hold_us"
    );
    if comparison {
        for nodes in [8, 32, 128] {
            for scenario in [
                Scenario::Baseline,
                Scenario::Requests,
                Scenario::Retune,
                Scenario::Rewire,
            ] {
                run_case(scenario, nodes, 1);
            }
        }
        return;
    }
    for nodes in [1, 8, 32, 128] {
        for scenario in [
            Scenario::Baseline,
            Scenario::Requests,
            Scenario::Retune,
            Scenario::Rewire,
            Scenario::Watchdog,
            Scenario::Loopback,
            Scenario::LoopbackSubscribed,
        ] {
            if scenario != Scenario::Rewire || nodes > 1 {
                run_case(scenario, nodes, REPEATS);
            }
        }
    }
    run_case(Scenario::TreeRequests, 128, REPEATS);
    run_case(Scenario::ShadowPid, 128, REPEATS);
}
