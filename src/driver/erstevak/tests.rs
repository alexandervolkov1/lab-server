use super::*;

fn gauge(address: u16) -> Erstevak {
    Erstevak::from_options(&format!("address = {address}").parse().unwrap()).unwrap()
}

fn frame(body: &[u8]) -> Vec<u8> {
    [body, &[checksum(body), b'\r']].concat()
}

#[test]
fn official_pressure_example_and_decimal_address() {
    assert_eq!(gauge(1).request(), *b"001M^\r");
    assert_eq!(&gauge(999).request()[..4], b"999M");
    assert_eq!(gauge(1).pressure(b"001M982122V\r").unwrap(), 982.1);
    let pressure = gauge(1).pressure(&frame(b"001M123417")).unwrap();
    assert!((pressure - 0.001_234).abs() < 1e-12);
}

#[test]
fn range_and_error_frames_consume_a_complete_transaction() {
    for body in [
        b"001M000000".as_slice(),
        b"001M999999",
        b"001M1",
        b"001NO_DEF",
        b"001M_RANGE",
        b"001M_LOGIC",
    ] {
        assert_eq!(
            gauge(1).pressure(&frame(body)).unwrap_err().port_state,
            PortState::Synchronized
        );
    }
}

#[test]
fn malformed_pressure_frames_do_not_establish_synchronization() {
    let gauge = gauge(1);
    for invalid in [
        b"002M982122".as_slice(),
        b"001T982122",
        b"001M98212",
        b"001M9821222",
        b"001M98212x",
        b"001M012322",
        b"001T_LOGIC",
        b"001M5",
        b"001M7",
    ] {
        assert_eq!(
            gauge.pressure(&frame(invalid)).unwrap_err().port_state,
            PortState::Desynchronized
        );
    }
    let good = b"001M982122V\r";
    for end in 0..good.len() {
        assert_eq!(
            gauge.pressure(&good[..end]).unwrap_err().port_state,
            PortState::Desynchronized
        );
    }
    assert_eq!(
        gauge.pressure(b"001M982122W\r").unwrap_err().port_state,
        PortState::Desynchronized
    );
}

#[test]
fn validates_configured_address() {
    for options in [
        "",
        "address = 0",
        "address = 1000",
        "address = \"1\"",
        "address = 1\nlisten = true",
    ] {
        assert!(Erstevak::from_options(&options.parse().unwrap()).is_err());
    }
}
