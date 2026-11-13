use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = lab_server::startup::parse_config_path(std::env::args_os().skip(1))?;
    let config = lab_server::config::ServerConfig::load(path)?;

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal_stop = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        signal_stop.store(true, std::sync::atomic::Ordering::Release);
    })?;

    let server = lab_server::startup::start(&config)?;
    println!("TCP server listening on {}", server.local_addr()?);
    if let Some(address) = server.websocket_addr()? {
        println!(
            "WebSocket server listening on ws://{address}{}",
            lab_server::websocket::PATH
        );
    }
    server.run_until(&stop)?;
    Ok(())
}
