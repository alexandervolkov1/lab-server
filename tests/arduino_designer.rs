//! Real Arduino driver, real TCP/WS adapters, memory-only serial ports.
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use lab_server::config::ServerConfig;
use lab_server::driver::DeviceIo;
use lab_server::startup::{create_driver, start_with};
use lab_server::websocket::{PATH, SUBPROTOCOL};
use serde_json::{Value as Json, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

const WAIT: Duration = Duration::from_secs(10);
struct State {
    calls: Vec<String>,
    staged: HashMap<String, String>,
    active: HashMap<String, String>,
    power: u8,
    action_reply: Option<String>,
}
impl Default for State {
    fn default() -> Self {
        let staged = HashMap::from([
            ("room.capacity".into(), "300000".into()),
            ("seed".into(), "1".into()),
        ]);
        Self {
            calls: vec![],
            active: staged.clone(),
            staged,
            power: 0,
            action_reply: None,
        }
    }
}
struct Serial {
    input: Cursor<Vec<u8>>,
    state: Arc<Mutex<State>>,
}
impl Read for Serial {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.input.read(buf)
    }
}
impl BufRead for Serial {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.input.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.input.consume(n);
    }
}
impl DeviceIo for Serial {
    fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        assert!(
            self.input.fill_buf()?.is_empty(),
            "interleaved serial transactions"
        );
        assert!(bytes.len() - 1 <= 64);
        let command = std::str::from_utf8(bytes).unwrap();
        let parts: Vec<_> = command.trim_end().split(' ').collect();
        let mut state = self.state.lock().unwrap();
        state.calls.push(command.into());
        let id = parts[1];
        let payload = match parts.as_slice() {
            ["o", _, "get", "power"] => format!("ok {}", state.power),
            ["o", _, "get", "t.sample" | "t.heater" | "t.room"] => "ok 20.0".into(),
            ["o", _, "set", "power", value] => {
                state.power = value.parse().unwrap();
                "ok".into()
            }
            ["d", _, "set", key, value] => {
                let value = if *key == "seed" {
                    value.parse::<u32>().unwrap().to_string()
                } else {
                    assert!(value.contains('e'));
                    format!("{:.8e}", value.parse::<f32>().unwrap())
                };
                state.staged.insert((*key).into(), value);
                "ok".into()
            }
            ["d", _, "get", key] => format!("ok {}", state.staged[*key]),
            ["d", _, action @ ("apply" | "reset")] => {
                if let Some(reply) = state.action_reply.take() {
                    self.input = Cursor::new(reply.replace("{id}", id).into_bytes());
                    return Ok(());
                }
                if *action == "apply" {
                    state.active = state.staged.clone();
                } else {
                    state.staged = state.active.clone();
                }
                state.power = 0;
                "ok".into()
            }
            _ => panic!("unexpected command: {command}"),
        };
        self.input = Cursor::new(format!("@{id} {payload}\r\n").into_bytes());
        Ok(())
    }
}
struct Running {
    tcp: SocketAddr,
    ws: SocketAddr,
    states: HashMap<String, Arc<Mutex<State>>>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<io::Result<()>>>,
}
impl Running {
    fn new() -> Self {
        let mut config =
            "[tcp]\nbind='127.0.0.1:0'\n[websocket]\nenabled=true\nbind='127.0.0.1:0'\n".to_owned();
        for id in ["a", "b"] {
            config += &format!(
                "[[ports]]\nid='{id}'\nname='MEMORY-{id}'\nbaud=57600\ndata_bits=8\nparity='none'\nstop_bits=1\nflow_control='none'\ntimeout_ms=100\n[[devices]]\nid='{id}'\nport='{id}'\ndriver='arduino'\n"
            );
        }
        let states: HashMap<_, _> = ["a", "b"]
            .into_iter()
            .map(|id| (id.into(), Arc::new(Mutex::new(State::default()))))
            .collect();
        let server = start_with(
            &ServerConfig::from_toml(&config).unwrap(),
            |config| {
                let mut setup = create_driver(config)?;
                assert_eq!(setup.post_open_delay, Duration::from_secs(2));
                // The production factory retains its hardware delay; this fake port
                // has no reset/boot period and needs no sleep.
                setup.post_open_delay = Duration::ZERO;
                Ok(setup)
            },
            |port, _| {
                Ok(Serial {
                    input: Cursor::new(vec![]),
                    state: states[&port.id].clone(),
                })
            },
        )
        .unwrap();
        let tcp = server.local_addr().unwrap();
        let ws = server.websocket_addr().unwrap().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = Some(thread::spawn(move || server.run_until(&flag)));
        Self {
            tcp,
            ws,
            states,
            stop,
            join,
        }
    }
    fn state(&self, id: &str) -> std::sync::MutexGuard<'_, State> {
        self.states[id].lock().unwrap()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.join.take().unwrap().join().unwrap().unwrap();
    }
}
enum Client {
    Tcp(BufReader<TcpStream>),
    Ws(Box<WebSocket<TcpStream>>),
}
impl Client {
    fn new(server: &Running, ws: bool) -> Self {
        let stream = TcpStream::connect(if ws { server.ws } else { server.tcp }).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.set_write_timeout(Some(WAIT)).unwrap();
        if ws {
            let mut request = format!("ws://{}{PATH}", server.ws)
                .into_client_request()
                .unwrap();
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
            Self::Ws(Box::new(tungstenite::client(request, stream).unwrap().0))
        } else {
            Self::Tcp(BufReader::new(stream))
        }
    }
    fn request(&mut self, message: Json) -> Json {
        match self {
            Self::Tcp(socket) => {
                writeln!(socket.get_mut(), "{message}").unwrap();
                let mut reply = String::new();
                socket.read_line(&mut reply).unwrap();
                serde_json::from_str(&reply).unwrap()
            }
            Self::Ws(socket) => {
                socket
                    .send(Message::Text(message.to_string().into()))
                    .unwrap();
                let Message::Text(reply) = socket.read().unwrap() else {
                    panic!("expected text")
                };
                serde_json::from_str(&reply).unwrap()
            }
        }
    }
}

#[test]
fn designer_json_staging_actions_and_operator_api_work_over_tcp_and_ws() {
    for ws in [false, true] {
        let server = Running::new();
        let mut client = Client::new(&server, ws);
        let catalog = client.request(json!({"id":1,"op":"describe"}));
        assert_eq!(catalog["parameters"].as_array().unwrap().len(), 46);
        assert_eq!(catalog["actions"].as_array().unwrap().len(), 4);
        assert!(
            catalog["actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|a| a["key"] == "a/apply")
        );
        let mut id = 2;
        for (key, value) in [
            ("room.capacity", json!(350000.5)),
            ("sensor.noise", json!(0.1)),
            ("seed", json!(u32::MAX)),
        ] {
            let global = format!("a/designer/{key}");
            assert_eq!(
                client.request(json!({"id":id,"op":"set","key":global,"value":value}))["status"],
                "ok"
            );
            id += 1;
            let reply = client.request(json!({"id":id,"op":"get","key":global}));
            id += 1;
            let expected = if key == "seed" {
                value
            } else {
                json!(f64::from(value.as_f64().unwrap() as f32))
            };
            assert_eq!(reply["value"], expected);
        }
        assert_eq!(server.state("a").active["room.capacity"], "300000");
        assert_eq!(
            client.request(json!({"id":id,"op":"set","key":"a/power","value":65}))["status"],
            "ok"
        );
        id += 1;
        assert_eq!(
            client.request(json!({"id":id,"op":"invoke_action","key":"a/apply"})),
            json!({"id":id,"status":"ok"})
        );
        id += 1;
        assert_eq!(server.state("a").power, 0);
        assert_eq!(server.state("a").active["seed"], "4294967295");
        assert_eq!(
            client.request(json!({"id":id,"op":"set","key":"a/designer/seed","value":0}))["status"],
            "ok"
        );
        id += 1;
        assert_eq!(
            client.request(json!({"id":id,"op":"invoke_action","key":"a/reset"}))["status"],
            "ok"
        );
        id += 1;
        assert_eq!(
            client.request(json!({"id":id,"op":"get","key":"a/designer/seed"}))["value"],
            u32::MAX
        );
        id += 1;
        // v1 float fields require a fraction or exponent, including room.capacity.
        assert_eq!(
            client.request(
                json!({"id":id,"op":"set","key":"a/designer/room.capacity","value":350000})
            )["status"],
            "error"
        );
        id += 1;
        assert_eq!(
            client
                .request(json!({"id":id,"op":"set","key":"a/designer/room.capacity","value":-0.0}))
                ["status"],
            "ok"
        );
        id += 1;
        let read =
            client.request(json!({"id":id,"op":"get","key":"a/designer/room.capacity"}))["value"]
                .as_f64()
                .unwrap();
        assert_eq!(read.to_bits(), (-0.0_f64).to_bits());
        let state = server.state("a");
        assert_eq!(
            state
                .calls
                .iter()
                .filter(|c| c.contains("set power"))
                .count(),
            1,
            "no synthetic power write for apply/reset"
        );
        assert_eq!(
            state
                .calls
                .iter()
                .filter(|c| c.ends_with("apply\n") || c.ends_with("reset\n"))
                .count(),
            2
        );
        assert!(state.calls.iter().any(|c| c.contains("get t.sample")));
        let ids: Vec<u16> = state
            .calls
            .iter()
            .map(|c| c.split(' ').nth(1).unwrap().parse().unwrap())
            .collect();
        assert!(ids.windows(2).all(|pair| pair[1] == pair[0] + 1));
    }
}

#[test]
fn explicit_config_error_stays_unknown_without_retry_and_bad_reply_is_port_local() {
    for reply in ["@999 ok\n", "@{id} ok"] {
        let server = Running::new();
        let mut tcp = Client::new(&server, false);
        let mut ws = Client::new(&server, true);
        server.state("a").action_reply = Some("@{id} err config\n".into());
        assert_eq!(
            tcp.request(json!({"id":1,"op":"invoke_action","key":"a/apply"})),
            json!({"id":1,"status":"error","error":"outcome_unknown"})
        );
        // The explicit refusal is synchronized; the port must remain usable.
        assert_eq!(
            ws.request(json!({"id":1,"op":"get","key":"a/designer/seed"}))["value"],
            1
        );
        server.state("a").action_reply = Some(reply.into());
        assert_eq!(
            ws.request(json!({"id":2,"op":"invoke_action","key":"a/reset"}))["error"],
            "outcome_unknown"
        );
        assert_eq!(
            tcp.request(json!({"id":2,"op":"get","key":"a/designer/seed"}))["error"],
            "runtime_unavailable"
        );
        assert_eq!(
            tcp.request(json!({"id":3,"op":"invoke_action","key":"b/apply"}))["status"],
            "ok"
        );
        assert_eq!(
            ws.request(json!({"id":3,"op":"get","key":"b/designer/seed"}))["value"],
            1
        );
        assert_eq!(
            server
                .state("a")
                .calls
                .iter()
                .filter(|c| c.ends_with("apply\n") || c.ends_with("reset\n"))
                .count(),
            2
        );
        // No late ACK can appear ahead of a subsequent response.
        assert_eq!(
            ws.request(json!({"id":4,"op":"ping"})),
            json!({"id":4,"status":"ok"})
        );
    }
}
