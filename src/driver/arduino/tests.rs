use std::io::{self, BufReader, Cursor, Read, Write};

use super::*;

struct FakePort {
    input: Cursor<Vec<u8>>,
    output: Vec<u8>,
}

impl Read for FakePort {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.input.read(buf)
    }
}

impl Write for FakePort {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.output.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn reads_temperature() {
    let fake = FakePort {
        input: Cursor::new(b"@1 ok 23.75\r\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    let result = driver.read("sample_temperature", &mut port);

    assert_eq!(result, Ok(Value::Float(23.75)));
    assert_eq!(port.get_ref().output, b"o 1 get t.sample\n");
}

#[test]
fn error_states_require_a_complete_correlated_transaction() {
    for (response, expected) in [
        ("@1 ok invalid\n", PortState::Synchronized),
        ("@1 ok 9999\n", PortState::Synchronized),
        ("@1 err sensor\n", PortState::Synchronized),
        ("@2 ok 20\n", PortState::Desynchronized),
        ("@1 ok 20", PortState::Desynchronized),
        ("@1 ok 20 extra\n", PortState::Desynchronized),
        ("garbage\n", PortState::Desynchronized),
        ("", PortState::Disconnected),
    ] {
        let mut io = BufReader::new(FakePort {
            input: Cursor::new(response.as_bytes().to_vec()),
            output: vec![],
        });
        let error = Arduino::new()
            .read("sample_temperature", &mut io)
            .unwrap_err();
        assert_eq!(error.port_state, expected, "{response:?}");
    }
    for (response, expected) in [
        ("@1 err range\n", PortState::Synchronized),
        ("@2 ok\n", PortState::Desynchronized),
        ("@1 ok", PortState::Desynchronized),
        ("@1 ok extra\n", PortState::Desynchronized),
        ("", PortState::Disconnected),
    ] {
        let mut io = BufReader::new(FakePort {
            input: Cursor::new(response.as_bytes().to_vec()),
            output: vec![],
        });
        let error = Arduino::new()
            .write("power", &Value::Integer(1), &mut io)
            .unwrap_err();
        assert_eq!(error.port_state, expected, "{response:?}");
    }
}

#[test]
fn rejects_unexpected_response() {
    let fake = FakePort {
        input: Cursor::new(b"ERR\r\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    let result = driver.read("sample_temperature", &mut port);

    assert_eq!(
        result,
        Err(DriverError::new(
            "Unexpected response",
            PortState::Desynchronized
        ))
    );
}

#[test]
fn rejects_unknown_parameter() {
    let fake = FakePort {
        input: Cursor::new(Vec::new()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    let result = driver.read("unknown", &mut port);

    assert!(result.is_err());
    assert!(port.get_ref().output.is_empty());
}

#[test]
fn parses_numeric_reply() {
    let temperature = Spec::Float {
        min: -250.0,
        max: 1500.0,
    };

    let power = Spec::Integer { min: 0, max: 100 };

    assert_eq!(
        parse_numeric_reply("@7 ok 23.75", 7, &temperature),
        Ok(Value::Float(23.75))
    );

    assert_eq!(
        parse_numeric_reply("@8 ok 10", 8, &power),
        Ok(Value::Integer(10))
    );

    assert!(parse_numeric_reply("@8 ok 10.5", 8, &power).is_err());
    assert!(parse_numeric_reply("@8 ok 101", 8, &power).is_err());
    assert!(parse_numeric_reply("@8 ok -1", 8, &power).is_err());

    assert!(parse_numeric_reply("@8 ok 23.75", 7, &temperature).is_err());
    assert!(parse_numeric_reply("@7 ok abc", 7, &temperature).is_err());
    assert!(parse_numeric_reply("@7 err key", 7, &temperature).is_err());
    assert!(parse_numeric_reply("ERR", 7, &temperature).is_err());
    assert!(parse_numeric_reply("@7 ok 23.75 extra", 7, &temperature).is_err());
}

#[test]
fn increments_request_id() {
    let fake = FakePort {
        input: Cursor::new(b"@1 ok 20.00\n@2 ok 21.00\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    assert_eq!(
        driver.read("sample_temperature", &mut port),
        Ok(Value::Float(20.0))
    );

    assert_eq!(
        driver.read("sample_temperature", &mut port),
        Ok(Value::Float(21.0))
    );

    assert_eq!(
        port.get_ref().output,
        b"o 1 get t.sample\no 2 get t.sample\n"
    );
}

#[test]
fn writes_power() {
    let fake = FakePort {
        input: Cursor::new(b"@1 ok 20.00\n@2 ok\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    assert_eq!(
        driver.read("sample_temperature", &mut port),
        Ok(Value::Float(20.0))
    );

    assert_eq!(
        driver.write("power", &Value::Integer(10), &mut port),
        Ok(())
    );

    assert_eq!(
        port.get_ref().output,
        b"o 1 get t.sample\no 2 set power 10\n"
    );
}

#[test]
fn rejects_invalid_writes() {
    let fake = FakePort {
        input: Cursor::new(b"@1 ok\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    assert!(
        driver
            .write("power", &Value::Integer(-1), &mut port)
            .is_err()
    );
    assert!(
        driver
            .write("power", &Value::Integer(101), &mut port)
            .is_err()
    );
    assert!(
        driver
            .write("power", &Value::Float(10.0), &mut port)
            .is_err()
    );

    assert!(
        driver
            .write("sample_temperature", &Value::Float(20.0), &mut port)
            .is_err()
    );

    assert!(port.get_ref().output.is_empty());

    assert_eq!(
        driver.write("power", &Value::Integer(10), &mut port),
        Ok(())
    );

    assert_eq!(port.get_ref().output, b"o 1 set power 10\n");
}

#[test]
fn reports_arduino_write_error() {
    let fake = FakePort {
        input: Cursor::new(b"@1 err range\r\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    let result = driver.write("power", &Value::Integer(10), &mut port);

    assert_eq!(
        result,
        Err(DriverError::new(
            "Arduino error: range",
            PortState::Synchronized
        ))
    );
}

#[test]
fn reads_remaining_numeric_parameters() {
    let fake = FakePort {
        input: Cursor::new(b"@1 ok 31.5\n@2 ok 21.25\n@3 ok 10\n".to_vec()),
        output: Vec::new(),
    };

    let mut port = BufReader::new(fake);
    let mut driver = Arduino::new();

    assert_eq!(
        driver.read("heater_temperature", &mut port),
        Ok(Value::Float(31.5))
    );

    assert_eq!(
        driver.read("room_temperature", &mut port),
        Ok(Value::Float(21.25))
    );

    assert_eq!(driver.read("power", &mut port), Ok(Value::Integer(10)));

    assert_eq!(
        port.get_ref().output,
        b"o 1 get t.heater\no 2 get t.room\no 3 get power\n"
    );
}

fn fake(response: &str) -> BufReader<FakePort> {
    BufReader::new(FakePort {
        input: Cursor::new(response.as_bytes().to_vec()),
        output: vec![],
    })
}

const ALL_DESIGNER: [&str; 19] = [
    "heater.capacity",
    "sample.capacity",
    "room.capacity",
    "coupling",
    "heater.loss",
    "sample.loss",
    "room.loss",
    "heater.radiation",
    "sample.radiation",
    "heater.max_power",
    "initial.heater",
    "initial.sample",
    "initial.room",
    "background.temp",
    "seed",
    "parameter.spread",
    "power.fluctuation",
    "sensor.noise",
    "sensor.drift",
];

#[test]
fn designer_catalog_and_operator_polling_are_independent() {
    let driver = Arduino::new();
    let catalog = driver.parameters();
    assert_eq!(catalog.len(), 23);
    for key in ALL_DESIGNER {
        let parameter = catalog
            .iter()
            .find(|p| p.id == format!("designer/{key}"))
            .unwrap();
        assert!(parameter.writable);
        assert_eq!(parameter.default_poll_period, None);
        match (&parameter.spec, key) {
            (Spec::Integer { min: 0, max }, "seed") => assert_eq!(*max, i64::from(u32::MAX)),
            (Spec::Float { min, max }, key) if key != "seed" => {
                assert_eq!(*min, f64::from(f32::MIN));
                assert_eq!(*max, f64::from(f32::MAX));
            }
            _ => panic!("wrong type: {key}"),
        }
    }
    assert_eq!(
        catalog
            .iter()
            .filter(|p| p.default_poll_period == Some(Duration::from_secs(1)))
            .count(),
        4
    );
    let actions = driver.actions();
    assert_eq!(
        actions.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
        ["apply", "reset"]
    );
    assert!(actions.iter().all(|a| !a.description.is_empty()));
}

#[test]
fn every_designer_key_maps_to_get_and_set_with_one_shared_id_sequence() {
    let mut driver = Arduino::new();
    let mut id = 1;
    for key in ALL_DESIGNER {
        let parameter = format!("designer/{key}");
        let (text, value) = if key == "seed" {
            ("4294967295", Value::Integer(4294967295))
        } else {
            ("-1.25e0", Value::Float(-1.25))
        };
        let mut port = fake(&format!("@{id} ok\r\n@{} ok {text}\n", id + 1));
        driver.write(&parameter, &value, &mut port).unwrap();
        assert_eq!(driver.read(&parameter, &mut port).unwrap(), value);
        assert_eq!(
            String::from_utf8(port.get_ref().output.clone()).unwrap(),
            format!("d {id} set {key} {text}\nd {} get {key}\n", id + 1)
        );
        id += 2;
    }
    let mut port = fake(&format!(
        "@{id} ok 12\n@{} ok\n@{} ok\n@{} ok\n",
        id + 1,
        id + 2,
        id + 3
    ));
    assert_eq!(driver.read("power", &mut port).unwrap(), Value::Integer(12));
    driver.invoke_action("apply", &mut port).unwrap();
    driver
        .write("power", &Value::Integer(100), &mut port)
        .unwrap();
    driver.invoke_action("reset", &mut port).unwrap();
    assert_eq!(
        String::from_utf8(port.get_ref().output.clone()).unwrap(),
        format!(
            "o {id} get power\nd {} apply\no {} set power 100\nd {} reset\n",
            id + 1,
            id + 2,
            id + 3
        )
    );
}

#[test]
fn ids_wrap_across_operator_designer_and_actions_without_extra_power_write() {
    let mut driver = Arduino {
        next_id: u16::MAX - 1,
    };
    let mut port = fake("@65534 ok 20\n@65535 ok\n@1 ok\n@2 ok 42\n@3 ok\n");
    driver.read("heater_temperature", &mut port).unwrap();
    driver.invoke_action("apply", &mut port).unwrap();
    driver.invoke_action("reset", &mut port).unwrap();
    driver.read("designer/seed", &mut port).unwrap();
    driver
        .write("designer/room.capacity", &Value::Float(1.0), &mut port)
        .unwrap();
    assert_eq!(port.get_ref().output, b"o 65534 get t.heater\nd 65535 apply\nd 1 reset\nd 2 get seed\nd 3 set room.capacity 1e0\n");
}

#[test]
fn float_rounding_signed_zero_and_room_capacity_use_decimal_grammar() {
    // Binary64 halfway cases: nearest-even to binary32, on each side of 1.0.
    let halfway = 1.0 + 2.0_f64.powi(-24);
    for (input, expected) in [
        (0.1, 0.1_f32),
        (halfway, 1.0),
        (
            f64::from_bits(halfway.to_bits() + 1),
            f32::from_bits(1.0_f32.to_bits() + 1),
        ),
        (
            1.0 + 3.0 * 2.0_f64.powi(-24),
            f32::from_bits(1.0_f32.to_bits() + 2),
        ),
        (16777217.0, 16777216.0),
        (4294967295.0, 4294967296.0),
        (-1.0, -1.0),
        (-0.0, -0.0),
        (0.0, 0.0),
        (f64::from(f32::MAX), f32::MAX),
        (f64::from(f32::MIN), f32::MIN),
        (f64::from(f32::from_bits(1)), f32::from_bits(1)),
        (f64::from(f32::MIN_POSITIVE), f32::MIN_POSITIVE),
    ] {
        let mut driver = Arduino::new();
        let mut port = fake("@1 ok\n");
        driver
            .write("designer/room.capacity", &Value::Float(input), &mut port)
            .unwrap();
        let request = std::str::from_utf8(&port.get_ref().output).unwrap();
        let token = request.split_whitespace().last().unwrap();
        assert!(
            token.contains('e'),
            "room.capacity must bypass plain u32 grammar: {token}"
        );
        assert_eq!(token.parse::<f32>().unwrap().to_bits(), expected.to_bits());
        assert!(request.len() - 1 <= 64);
        let mut port = fake(&format!("@2 ok {token}\n"));
        let Value::Float(read) = driver.read("designer/room.capacity", &mut port).unwrap() else {
            panic!()
        };
        assert_eq!(read.to_bits(), f64::from(expected).to_bits());
    }
}

#[test]
fn float_encoding_round_trips_exponents_subnormals_and_random_finite_bits() {
    let mut state = 37u32;
    for index in 0..4096 {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        let bits = if index < 512 {
            ((index / 2) << 23) | (index & 1)
        } else {
            state
        };
        let value = f32::from_bits(bits);
        if !value.is_finite() {
            continue;
        }
        let mut port = fake("@65535 ok\n");
        Arduino { next_id: u16::MAX }
            .write(
                "designer/power.fluctuation",
                &Value::Float(f64::from(value)),
                &mut port,
            )
            .unwrap();
        let request = std::str::from_utf8(&port.get_ref().output).unwrap();
        assert!(request.len() - 1 <= 64);
        assert_eq!(
            request
                .split_whitespace()
                .last()
                .unwrap()
                .parse::<f32>()
                .unwrap()
                .to_bits(),
            bits
        );
    }
}

#[test]
fn invalid_designer_values_and_actions_do_not_touch_io_or_consume_ids() {
    let mut driver = Arduino::new();
    let mut port = fake("@1 ok\n");
    for value in [
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::from(f32::MAX) * 2.0),
        Value::Float(1e-50),
        Value::Float(-1e-50),
        Value::Float(f64::from(f32::from_bits(1)) / 2.0),
        Value::Integer(1),
        Value::Boolean(false),
    ] {
        let error = driver
            .write("designer/room.capacity", &value, &mut port)
            .unwrap_err();
        assert_eq!(error.port_state, PortState::Synchronized);
    }
    for value in [
        Value::Integer(-1),
        Value::Integer(4294967296),
        Value::Float(1.0),
        Value::Boolean(true),
    ] {
        assert!(driver.write("designer/seed", &value, &mut port).is_err());
    }
    for key in [
        "designer/unknown",
        "designer/seed/extra",
        "heater.capacity",
        "designer/room.capacity\n",
    ] {
        assert!(driver.read(key, &mut port).is_err());
        assert!(driver.write(key, &Value::Float(1.0), &mut port).is_err());
    }
    assert!(driver.invoke_action("apply extra", &mut port).is_err());
    assert!(port.get_ref().output.is_empty());
    driver.invoke_action("apply", &mut port).unwrap();
    assert_eq!(port.get_ref().output, b"d 1 apply\n");
}

#[test]
fn firmware_float_output_is_rounded_to_f32_before_catalog_range_validation() {
    for (token, expected) in [
        ("3.40282347e38", f32::MAX),
        ("-3.40282347e38", f32::MIN),
        ("1.40129846e-45", f32::from_bits(1)),
        ("-0.0", -0.0),
        ("350000", 350000.0),
    ] {
        let Value::Float(value) = Arduino::new()
            .read(
                "designer/room.capacity",
                &mut fake(&format!("@1 ok {token}\n")),
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(value.to_bits(), f64::from(expected).to_bits());
    }
    for token in [
        "NaN", "inf", "-inf", "1e39", "1e-99", "-1e-99", "1.2.3", "0x10", "1_000",
    ] {
        let error = Arduino::new()
            .read(
                "designer/room.capacity",
                &mut fake(&format!("@1 ok {token}\n")),
            )
            .unwrap_err();
        assert_eq!(
            error.port_state,
            PortState::Synchronized,
            "complete but invalid numeric payload: {token}"
        );
    }
}

#[test]
fn seed_reads_and_writes_preserve_full_u32_domain() {
    for seed in [0, 1, 16777217, u32::MAX] {
        let mut port = fake(&format!("@1 ok\n@2 ok {seed}\n"));
        let mut driver = Arduino::new();
        driver
            .write("designer/seed", &Value::Integer(i64::from(seed)), &mut port)
            .unwrap();
        assert_eq!(
            driver.read("designer/seed", &mut port).unwrap(),
            Value::Integer(i64::from(seed))
        );
    }
    for token in ["-1", "4294967296", "1.0", "1e0", "+1", "NaN"] {
        let error = Arduino::new()
            .read("designer/seed", &mut fake(&format!("@1 ok {token}\n")))
            .unwrap_err();
        assert_eq!(error.port_state, PortState::Synchronized);
    }
}

#[test]
fn designer_errors_are_correlated_but_damaged_frames_stop_synchronization() {
    for reason in ["syntax", "key", "value", "config", "unsupported"] {
        for operation in ["get", "set", "apply", "reset"] {
            let mut driver = Arduino::new();
            let mut port = fake(&format!("@1 err {reason}\r\n@2 ok 19\n"));
            let error = match operation {
                "get" => driver.read("designer/seed", &mut port).map(|_| ()),
                "set" => driver.write("designer/seed", &Value::Integer(1), &mut port),
                action => driver.invoke_action(action, &mut port),
            }
            .unwrap_err();
            assert_eq!(error, local_error(format!("Arduino error: {reason}")));
            assert_eq!(driver.read("power", &mut port).unwrap(), Value::Integer(19));
            assert_eq!(
                std::str::from_utf8(&port.get_ref().output)
                    .unwrap()
                    .lines()
                    .count(),
                2
            );
        }
    }
    for response in [
        "@2 ok\n",
        "@01 ok\n",
        "@1 OK\n",
        "@1 ok extra\n",
        "@1 err\n",
        "@1 err config extra\n",
        "@1 ok",
        "@1 ok\r",
        "ERR\n",
        "garbage\n",
        " @1 ok\n",
        "@1  ok\n",
        "@1\tok\n",
        "@1 ok \n",
        "@1 ok\0\n",
        "@1\u{a0}ok\n",
    ] {
        for action in ["apply", "reset"] {
            let mut port = fake(response);
            assert_eq!(
                Arduino::new()
                    .invoke_action(action, &mut port)
                    .unwrap_err()
                    .port_state,
                PortState::Desynchronized,
                "{response:?}"
            );
            assert_eq!(
                std::str::from_utf8(&port.get_ref().output)
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    for response in [
        "@1 ok\n",
        "@1 ok 1 extra\n",
        "@2 ok 1\n",
        "@1 ok 1",
        "@1 err config extra\n",
    ] {
        assert_eq!(
            Arduino::new()
                .read("designer/coupling", &mut fake(response))
                .unwrap_err()
                .port_state,
            PortState::Desynchronized
        );
    }
    let mut port = fake(&format!("@1 err {}\n", "x".repeat(1000)));
    assert_eq!(
        Arduino::new()
            .invoke_action("apply", &mut port)
            .unwrap_err()
            .port_state,
        PortState::Desynchronized
    );
    assert_eq!(
        Arduino::new()
            .invoke_action("reset", &mut fake(""))
            .unwrap_err()
            .port_state,
        PortState::Disconnected
    );
    let mut port = fake("");
    port.get_mut().input = Cursor::new(b"@1 ok\xff\n".to_vec());
    assert_eq!(
        Arduino::new()
            .invoke_action("apply", &mut port)
            .unwrap_err()
            .port_state,
        PortState::Desynchronized
    );
}

#[test]
fn json_v1_f64_boundary_is_explicit_before_designer_f32_conversion() {
    use crate::protocol::{Request, parse_request};
    for (token, expected) in [
        ("0.1", 0.1_f32),
        ("-0.0", -0.0),
        ("16777217.0", 16777216.0),
        // Existing v1 first rounds this decimal to an exact f64 midpoint.
        ("1.0000000596046448", 1.0),
        // A driver cannot recover a nonzero token already parsed as f64 zero.
        ("1e-400", 0.0),
    ] {
        let Request::Set { key, value, .. } = parse_request(&format!(
            "{{\"id\":1,\"op\":\"set\",\"key\":\"designer/room.capacity\",\"value\":{token}}}"
        ))
        .unwrap() else {
            panic!()
        };
        let mut port = fake("@1 ok\n");
        Arduino::new().write(&key, &value, &mut port).unwrap();
        let command = std::str::from_utf8(&port.get_ref().output).unwrap();
        assert_eq!(
            command
                .split_whitespace()
                .last()
                .unwrap()
                .parse::<f32>()
                .unwrap()
                .to_bits(),
            expected.to_bits()
        );
    }
    assert_eq!(
        "1.0000000596046448".parse::<f32>().unwrap().to_bits(),
        1.0_f32.to_bits() + 1
    );
}
