use super::*;

fn append_crc(mut frame: Vec<u8>) -> Vec<u8> {
    frame.extend_from_slice(&crc16(&frame).to_le_bytes());
    frame
}

#[test]
fn crc_reference_and_rrg12_manual_vectors() {
    assert_eq!(crc16(b"123456789"), 0x4b37);
    let read = ReadRegisters::new(3, RegisterTable::Holding, 4, 2).unwrap();
    assert_eq!(read.encode(), [3, 3, 0, 4, 0, 2, 0x84, 0x28]);
    assert_eq!(
        read.decode(&[3, 3, 4, 0, 9, 0x80, 4, 0x69, 0xf2]).unwrap(),
        [9, 0x8004]
    );
}

#[test]
fn rejects_bad_ranges_and_decodes_maximum_read() {
    for (slave, start, count) in [
        (0, 0, 1),
        (248, 0, 1),
        (1, 0, 0),
        (1, 0, 126),
        (1, 65535, 2),
    ] {
        assert_eq!(
            ReadRegisters::new(slave, RegisterTable::Input, start, count)
                .unwrap_err()
                .port_state,
            PortState::Synchronized
        );
    }
    let read = ReadRegisters::new(247, RegisterTable::Input, 65535 - 124, 125).unwrap();
    assert_eq!(&read.encode()[..6], &[247, 4, 255, 131, 0, 125]);
    let mut frame = vec![247, 4, 250];
    frame.extend_from_slice(&[0xff; 250]);
    assert_eq!(read.decode(&append_crc(frame)).unwrap(), [65535; 125]);
}

#[test]
fn response_requires_matching_address_function_length_byte_count_and_crc() {
    let read = ReadRegisters::new(1, RegisterTable::Holding, 0, 1).unwrap();
    for prefix in [
        vec![2, 3, 2, 0, 7],
        vec![1, 4, 2, 0, 7],
        vec![1, 3, 1, 0, 7],
        vec![1, 3, 2, 0],
        vec![1, 3, 2, 0, 7, 0],
        vec![1, 0x84, 2],
    ] {
        assert_eq!(
            read.decode(&append_crc(prefix)).unwrap_err().port_state,
            PortState::Desynchronized
        );
    }
    let good = append_crc(vec![1, 3, 2, 0, 7]);
    for end in 0..good.len() {
        assert_eq!(
            read.decode(&good[..end]).unwrap_err().port_state,
            PortState::Desynchronized
        );
    }
    let mut corrupt = good;
    corrupt[6] ^= 1;
    assert_eq!(
        read.decode(&corrupt).unwrap_err().port_state,
        PortState::Desynchronized
    );
}

#[test]
fn only_a_correlated_exception_with_valid_crc_is_complete() {
    let read = ReadRegisters::new(1, RegisterTable::Holding, 0, 1).unwrap();
    let mut frame = append_crc(vec![1, 0x83, 4]);
    let error = read.decode(&frame).unwrap_err();
    assert_eq!(error.port_state, PortState::Synchronized);
    assert!(error.message.contains("exception 0x04"));
    frame[3] ^= 1;
    assert_eq!(
        read.decode(&frame).unwrap_err().port_state,
        PortState::Desynchronized
    );
}

#[test]
fn serial_silence_accounts_for_character_width_and_high_baud() {
    assert_eq!(
        interframe_silence(9600, 8, false, 1).unwrap(),
        Duration::from_nanos(3_645_834)
    );
    assert_eq!(
        interframe_silence(19200, 8, true, 1).unwrap(),
        Duration::from_nanos(2_005_209)
    );
    assert_eq!(
        interframe_silence(19200, 8, true, 1),
        interframe_silence(19200, 8, false, 2)
    );
    assert_eq!(
        interframe_silence(38400, 8, true, 1).unwrap(),
        Duration::from_micros(1750)
    );
    assert!(interframe_silence(0, 8, false, 1).is_err());
    assert!(interframe_silence(9600, 7, false, 1).is_err());
}
