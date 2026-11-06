use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lab_server::config::{ConfigError, FlowControl, Parity, ServerConfig};

const TCP: &str = "[tcp]\nbind = '127.0.0.1:7240'\n";
const PORT: &str = r#"
[[ports]]
id = "link1"
name = "COM3"
baud = 57600
data_bits = 8
parity = "none"
stop_bits = 1
flow_control = "none"
timeout_ms = 3000
"#;
const DEVICE: &str = r#"
[[devices]]
id = "furnace1"
driver = "arduino"
port = "link1"
"#;

fn single() -> String {
    format!("{TCP}{PORT}{DEVICE}")
}

fn device(id: &str, driver: &str, port: &str) -> String {
    format!("\n[[devices]]\nid = '{id}'\ndriver = '{driver}'\nport = '{port}'\n")
}

fn assert_invalid(input: &str, field: &str) -> ConfigError {
    let error = ServerConfig::from_toml(input).unwrap_err();
    assert!(
        error.to_string().contains(field),
        "expected {field:?} in {error}"
    );
    error
}

#[test]
fn one_arduino_on_one_port() {
    let config = ServerConfig::from_toml(&single()).unwrap();
    assert_eq!(config.tcp.bind.to_string(), "127.0.0.1:7240");
    assert_eq!(config.ports.len(), 1);
    let port = &config.ports[0];
    assert_eq!(port.id, "link1");
    assert_eq!(port.name, "COM3");
    assert_eq!(port.baud, 57600);
    assert_eq!(port.data_bits, 8);
    assert_eq!(port.parity, Parity::None);
    assert_eq!(port.stop_bits, 1);
    assert_eq!(port.flow_control, FlowControl::None);
    assert_eq!(port.timeout_ms, 3000);
    assert_eq!(config.devices.len(), 1);
    assert_eq!(config.devices[0].id, "furnace1");
    assert_eq!(config.devices[0].port, "link1");
    assert_eq!(config.devices[0].driver, "arduino");
    assert!(config.devices[0].options.is_empty());
    assert!(config.devices[0].poll_periods_ms.is_empty());
}

#[test]
fn devices_can_use_distinct_physical_ports() {
    let second_port = PORT.replace("link1", "link2").replace("COM3", "COM4");
    let input = format!(
        "{TCP}{PORT}{second_port}{DEVICE}{}",
        device("gauge1", "metakon", "link2")
    );
    let config = ServerConfig::from_toml(&input).unwrap();
    assert_eq!(config.ports.len(), 2);
    assert_eq!(config.devices[0].port, "link1");
    assert_eq!(config.devices[1].port, "link2");
}

#[test]
fn shared_port_acceptance_is_independent_of_driver_names_and_addresses() {
    let drivers = ["arduino", "metakon", "custom_ascii", "unimplemented_driver"];
    for first in drivers {
        for second in drivers {
            let input = format!(
                "{TCP}{PORT}{}[devices.options]\naddress = 1\n{}[devices.options]\naddress = 1\n",
                device("device1", first, "link1"),
                device("device2", second, "link1"),
            );
            let config = ServerConfig::from_toml(&input).unwrap();
            assert_eq!(config.ports.len(), 1);
            assert_eq!(config.devices.len(), 2);
            assert_eq!(config.devices[0].driver, first);
            assert_eq!(config.devices[1].driver, second);
            assert_eq!(config.devices[0].port, config.devices[1].port);
            assert_eq!(config.devices[0].options["address"].as_integer(), Some(1));
            assert_eq!(config.devices[1].options["address"].as_integer(), Some(1));
        }
    }
}

#[test]
fn several_devices_of_one_type_can_share_a_port_without_addresses() {
    let mut input = format!("{TCP}{PORT}");
    for id in ["gauge1", "gauge2", "gauge3"] {
        input.push_str(&device(id, "metakon", "link1"));
    }
    let config = ServerConfig::from_toml(&input).unwrap();
    assert_eq!(config.devices.len(), 3);
    assert!(
        config
            .devices
            .iter()
            .all(|device| device.options.is_empty())
    );
}

#[test]
fn options_preserve_toml_types_and_nested_structures() {
    let input = format!(
        r#"{}
[devices.options]
address = 1
gain = 1.0
enabled = true
label = "1"
stamp = 2026-11-06T12:30:00Z
date = 2026-11-06
time = 12:30:00
local_stamp = 2026-11-06T12:30:00
mixed = [1, 1.0, true, "1", [false], {{ mode = "fast" }}]
empty = []
infinity = inf
not_a_number = nan
"" = "empty option keys are valid TOML"
"driver/specific" = {{ any = "value" }}
[devices.options.calibration]
coefficients = [1.0, 2.5]
[devices.options.calibration.flags]
invert = false
[[devices.options.steps]]
target = 10
[[devices.options.steps]]
target = 20
"#,
        single()
    );
    let config = ServerConfig::from_toml(&input).unwrap();
    let options = &config.devices[0].options;
    assert_eq!(options["address"].as_integer(), Some(1));
    assert_eq!(options["gain"].as_float(), Some(1.0));
    assert_eq!(options["enabled"].as_bool(), Some(true));
    assert_eq!(options["label"].as_str(), Some("1"));
    for key in ["stamp", "date", "time", "local_stamp"] {
        assert!(options[key].as_datetime().is_some(), "{key}");
    }
    let mixed = options["mixed"].as_array().unwrap();
    assert_eq!(mixed[0].as_integer(), Some(1));
    assert_eq!(mixed[1].as_float(), Some(1.0));
    assert_eq!(mixed[2].as_bool(), Some(true));
    assert_eq!(mixed[3].as_str(), Some("1"));
    assert_eq!(mixed[4][0].as_bool(), Some(false));
    assert_eq!(mixed[5]["mode"].as_str(), Some("fast"));
    assert!(options["empty"].as_array().unwrap().is_empty());
    assert!(options["infinity"].as_float().unwrap().is_infinite());
    assert!(options["not_a_number"].as_float().unwrap().is_nan());
    assert_eq!(
        options["calibration"]["coefficients"][1].as_float(),
        Some(2.5)
    );
    assert_eq!(
        options["calibration"]["flags"]["invert"].as_bool(),
        Some(false)
    );
    assert_eq!(options["steps"][1]["target"].as_integer(), Some(20));
    assert_eq!(options["driver/specific"]["any"].as_str(), Some("value"));
    assert!(options.contains_key(""));
}

#[test]
fn options_of_different_devices_are_independent() {
    let input = format!(
        "{}[devices.options]\naddress = 1\nnested = {{ enabled = true }}\n{}[devices.options]\naddress = 2\nnested = {{ enabled = true }}\n",
        single(),
        device("second", "custom_ascii", "link1"),
    );
    let mut config = ServerConfig::from_toml(&input).unwrap();
    config.devices[0]
        .options
        .insert("address".into(), toml::Value::Integer(99));
    config.devices[0].options["nested"]
        .as_table_mut()
        .unwrap()
        .insert("enabled".into(), toml::Value::Boolean(false));
    assert_eq!(config.devices[1].options["address"].as_integer(), Some(2));
    assert_eq!(
        config.devices[1].options["nested"]["enabled"].as_bool(),
        Some(true)
    );
}

#[test]
fn unknown_driver_and_nonexistent_physical_port_are_not_probed() {
    let input = single()
        .replace("arduino", "future_driver")
        .replace("COM3", "nonexistent-port-for-config-test");
    assert!(ServerConfig::from_toml(&input).is_ok());
}

#[test]
fn rejects_unknown_port_reference() {
    let input = single().replace("port = \"link1\"", "port = \"missing\"");
    let error = assert_invalid(&input, "devices[0].port");
    assert!(error.to_string().contains("missing"));
}

#[test]
fn rejects_duplicate_port_and_device_ids_with_locations() {
    let duplicate_port = PORT.replace("COM3", "COM4");
    let error = assert_invalid(
        &format!("{TCP}{PORT}{duplicate_port}{DEVICE}"),
        "ports[1].id",
    );
    assert!(error.to_string().contains("ports[0].id"));
    let error = assert_invalid(&format!("{}{DEVICE}", single()), "devices[1].id");
    assert!(error.to_string().contains("devices[0].id"));
}

#[test]
fn rejects_two_declarations_of_same_physical_port() {
    let duplicate_port = PORT.replace("link1", "another_id");
    let error = assert_invalid(
        &format!("{TCP}{PORT}{duplicate_port}{DEVICE}"),
        "ports[1].name",
    );
    assert!(error.to_string().contains("COM3"));
    assert!(error.to_string().contains("ports[0].name"));
}

#[test]
fn rejects_empty_identifiers_names_and_references() {
    for (original, key, field) in [
        ("id = \"link1\"", "id", "ports[0].id"),
        ("name = \"COM3\"", "name", "ports[0].name"),
        ("id = \"furnace1\"", "id", "devices[0].id"),
        ("driver = \"arduino\"", "driver", "devices[0].driver"),
        ("port = \"link1\"", "port", "devices[0].port"),
    ] {
        for value in ["", "   "] {
            assert_invalid(
                &single().replace(original, &format!("{key} = '{value}'")),
                field,
            );
        }
    }
}

#[test]
fn device_ids_reserve_slash_for_global_signal_keys() {
    for id in ["/", "furnace/1", "/furnace", "furnace/"] {
        assert_invalid(&single().replace("furnace1", id), "devices[0].id");
    }
    assert!(ServerConfig::from_toml(&single().replace("furnace1", "печь-1.v2")).is_ok());
}

#[test]
fn validates_serial_numbers_and_reports_fields() {
    for (field, original, invalid_values) in [
        (
            "baud",
            "57600",
            vec!["0", "-1", "4294967296", "9600.0", "'9600'"],
        ),
        ("data_bits", "8", vec!["0", "4", "9", "256", "-1", "8.0"]),
        ("stop_bits", "1", vec!["0", "3", "-1", "1.5"]),
        (
            "timeout_ms",
            "3000",
            vec!["0", "-1", "1.5", "true", "18446744073709551616"],
        ),
    ] {
        for value in invalid_values {
            assert_invalid(
                &single().replace(
                    &format!("{field} = {original}"),
                    &format!("{field} = {value}"),
                ),
                field,
            );
        }
    }
}

#[test]
fn validates_parity_and_flow_control_variants() {
    for (field, invalid_values) in [
        ("parity", vec!["'mark'", "'space'", "'NONE'", "''", "1"]),
        ("flow_control", vec!["'rts'", "'auto'", "''", "false"]),
    ] {
        for value in invalid_values {
            assert_invalid(
                &single().replace(
                    &format!("{field} = \"none\""),
                    &format!("{field} = {value}"),
                ),
                field,
            );
        }
    }
}

#[test]
fn accepts_supported_serial_settings_without_device_specific_limits() {
    for (parity_name, parity) in [
        ("none", Parity::None),
        ("odd", Parity::Odd),
        ("even", Parity::Even),
    ] {
        for (flow_name, flow) in [
            ("none", FlowControl::None),
            ("software", FlowControl::Software),
            ("hardware", FlowControl::Hardware),
        ] {
            for bits in 5..=8 {
                for stops in [1, 2] {
                    let input = single()
                        .replace("parity = \"none\"", &format!("parity = '{parity_name}'"))
                        .replace(
                            "flow_control = \"none\"",
                            &format!("flow_control = '{flow_name}'"),
                        )
                        .replace("data_bits = 8", &format!("data_bits = {bits}"))
                        .replace("stop_bits = 1", &format!("stop_bits = {stops}"))
                        .replace("baud = 57600", "baud = 12345");
                    let config = ServerConfig::from_toml(&input).unwrap();
                    assert_eq!(config.ports[0].parity, parity);
                    assert_eq!(config.ports[0].flow_control, flow);
                }
            }
        }
    }
}

#[test]
fn tcp_bind_must_be_a_numeric_loopback_socket_address() {
    for bind in [
        "127.0.0.1:7240",
        "127.20.30.40:1",
        "[::1]:7240",
        "127.0.0.1:0",
        "[::1]:65535",
    ] {
        assert!(
            ServerConfig::from_toml(&single().replace("127.0.0.1:7240", bind)).is_ok(),
            "{bind}"
        );
    }
    for bind in [
        "",
        "localhost:7240",
        "127.0.0.1",
        "127.0.0.1:65536",
        "127.0.0.1:-1",
        "127.0.0.1:abc",
        "0.0.0.0:7240",
        "192.168.1.1:7240",
        "8.8.8.8:7240",
        "[::]:7240",
        "[2001:db8::1]:7240",
        "[::ffff:192.168.1.1]:7240",
    ] {
        assert_invalid(&single().replace("127.0.0.1:7240", bind), "bind");
    }
}

#[test]
fn missing_required_fields_have_useful_errors() {
    for (declaration, field) in [
        (TCP, "tcp"),
        (PORT, "ports"),
        (DEVICE, "devices"),
        ("bind = '127.0.0.1:7240'\n", "bind"),
        ("id = \"link1\"\n", "id"),
        ("name = \"COM3\"\n", "name"),
        ("baud = 57600\n", "baud"),
        ("data_bits = 8\n", "data_bits"),
        ("parity = \"none\"\n", "parity"),
        ("stop_bits = 1\n", "stop_bits"),
        ("flow_control = \"none\"\n", "flow_control"),
        ("timeout_ms = 3000\n", "timeout_ms"),
        ("id = \"furnace1\"\n", "id"),
        ("driver = \"arduino\"\n", "driver"),
        ("port = \"link1\"\n", "port"),
    ] {
        let error = assert_invalid(&single().replace(declaration, ""), field);
        assert!(error.to_string().contains("missing field"), "{error}");
        assert!(error.source().is_some());
    }
}

#[test]
fn rejects_malformed_toml_wrong_structure_and_unknown_schema_fields() {
    for (input, field) in [
        ("[tcp".into(), "TOML"),
        (format!("{TCP}{TCP}{PORT}{DEVICE}"), "tcp"),
        (single().replace("[[ports]]", "[ports]"), "ports"),
        (single().replace("[[devices]]", "[devices]"), "devices"),
        (
            single().replace("baud = 57600", "baud = 57600\nprotocol = 'arduino'"),
            "protocol",
        ),
        (
            single().replace(
                "bind = '127.0.0.1:7240'",
                "bind = '127.0.0.1:7240'\nunknown = true",
            ),
            "unknown",
        ),
        (format!("{}options = 1\n", single()), "options"),
        (format!("{}unknown = true\n", single()), "unknown"),
        (format!("unknown = true\n{}", single()), "unknown"),
    ] {
        assert_invalid(&input, field);
    }
}

#[test]
fn optional_poll_periods_are_per_device_and_keep_local_names() {
    let input = format!(
        "{}[devices.poll_periods_ms]\nfuture_signal = 100\n{}[devices.poll_periods_ms]\nfuture_signal = 200\n",
        single(),
        device("second", "future_driver", "link1"),
    );
    let config = ServerConfig::from_toml(&input).unwrap();
    assert_eq!(config.devices[0].poll_periods_ms["future_signal"], 100);
    assert_eq!(config.devices[1].poll_periods_ms["future_signal"], 200);
    for value in ["0", "-1", "1.5", "'100'"] {
        assert_invalid(
            &format!("{}[devices.poll_periods_ms]\nsignal = {value}\n", single()),
            "signal",
        );
    }
    for name in ["", "   "] {
        assert_invalid(
            &format!("{}[devices.poll_periods_ms]\n'{name}' = 100\n", single()),
            "poll_periods_ms",
        );
    }
}

#[test]
fn validates_models_edited_in_memory() {
    let mut config = ServerConfig::from_toml(&single()).unwrap();
    config.devices[0].port = "absent".into();
    assert!(
        matches!(config.validate(), Err(ConfigError::Validation { field, .. }) if field == "devices[0].port")
    );
    config.devices[0].port = "link1".into();
    config.tcp.bind = "0.0.0.0:7240".parse().unwrap();
    assert!(
        matches!(config.validate(), Err(ConfigError::Validation { field, .. }) if field == "tcp.bind")
    );
}

#[test]
fn explicitly_empty_port_and_device_lists_are_allowed() {
    let config =
        ServerConfig::from_toml("ports = []\ndevices = []\n[tcp]\nbind = '[::1]:7240'\n").unwrap();
    assert!(config.ports.is_empty());
    assert!(config.devices.is_empty());
}

#[test]
fn shipped_example_loads_and_matches_string_api() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/lab-server.toml");
    let config = ServerConfig::load(path).unwrap();
    assert_eq!(
        config,
        ServerConfig::from_toml(include_str!("../examples/lab-server.toml")).unwrap()
    );
    assert_eq!(config.ports.len(), 2);
    assert_eq!(config.devices.len(), 3);
    assert_eq!(config.devices[1].port, config.devices[2].port);
    assert_ne!(config.devices[1].driver, config.devices[2].driver);
}

struct TestFile(PathBuf);

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn load_reports_io_parse_and_validation_errors() {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
    fs::create_dir_all(&target).unwrap();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = target.join(format!("config-test-{}-{unique}.toml", std::process::id()));
    let error = ServerConfig::load(&path).unwrap_err();
    assert!(matches!(&error, ConfigError::Io { path: error_path, .. } if error_path == &path));
    assert!(error.source().is_some());
    assert!(error.to_string().contains(&path.display().to_string()));

    fs::File::create_new(&path).unwrap();
    let file = TestFile(path);
    fs::write(&file.0, "[tcp").unwrap();
    assert!(matches!(
        ServerConfig::load(&file.0),
        Err(ConfigError::Parse(_))
    ));
    fs::write(&file.0, single().replace("57600", "0")).unwrap();
    assert!(matches!(
        ServerConfig::load(&file.0),
        Err(ConfigError::Validation { .. })
    ));
    fs::write(&file.0, [0xff]).unwrap();
    assert!(matches!(
        ServerConfig::load(&file.0),
        Err(ConfigError::Io { .. })
    ));
}
