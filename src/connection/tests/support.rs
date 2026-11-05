use super::*;

pub(super) fn observation() -> Observation {
    Observation {
        source: "sample_temperature".to_owned(),
        unit: "degC".to_owned(),
        completed_at: UNIX_EPOCH,
        reading: Ok(Value::Float(20.0)),
    }
}

pub(super) fn loopback_pair() -> (TcpConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();

    (TcpConnection::new(server, 2, 256).unwrap(), client)
}

pub(super) fn wait_for_step(
    connection: &mut TcpConnection,
    ready: impl Fn(&IoStep) -> bool,
) -> IoStep {
    let deadline = Instant::now() + Duration::from_secs(1);

    loop {
        let step = connection.pump().unwrap();

        if ready(&step) {
            return step;
        }

        assert!(Instant::now() < deadline, "TCP test timed out");
        std::thread::yield_now();
    }
}

pub(super) fn collect_until_read_closed(connection: &mut TcpConnection) -> Vec<Incoming> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut incoming = Vec::new();

    loop {
        let step = connection.pump().unwrap();
        incoming.extend(step.incoming);

        if step.read_closed {
            return incoming;
        }

        assert!(Instant::now() < deadline, "TCP EOF test timed out");
        std::thread::yield_now();
    }
}

pub(super) fn power_catalog() -> HashMap<String, ParameterDescription> {
    let description = ParameterDescription {
        id: "power".to_owned(),
        spec: crate::model::Spec::Integer { min: 0, max: 100 },
        unit: "%".to_owned(),
        writable: true,
        default_poll_period: None,
    };

    HashMap::from([(description.id.clone(), description)])
}

pub(super) fn get_catalog() -> HashMap<String, ParameterDescription> {
    let on_demand = ParameterDescription {
        id: "configuration".to_owned(),
        spec: crate::model::Spec::Integer { min: 0, max: 100 },
        unit: "".to_owned(),
        writable: false,
        default_poll_period: None,
    };

    let polled = ParameterDescription {
        id: "sample_temperature".to_owned(),
        spec: crate::model::Spec::Float {
            min: -100.0,
            max: 500.0,
        },
        unit: "degC".to_owned(),
        writable: false,
        default_poll_period: Some(Duration::from_millis(100)),
    };

    HashMap::from([
        (on_demand.id.clone(), on_demand),
        (polled.id.clone(), polled),
    ])
}
