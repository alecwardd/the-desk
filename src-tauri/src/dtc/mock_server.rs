use std::io;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

fn build_frame(message_type: u16, payload: &[u8]) -> Vec<u8> {
    let size = (4 + payload.len()) as u16;
    let mut frame = Vec::with_capacity(size as usize);
    frame.extend_from_slice(&size.to_le_bytes());
    frame.extend_from_slice(&message_type.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Run a lightweight mock DTC server for local deterministic tests.
pub async fn run_mock_dtc_server(bind_addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            // encoding response
            let _ = socket.write_all(&build_frame(7, &0i32.to_le_bytes())).await;
            // logon response
            let _ = socket.write_all(&build_frame(2, &[1, 0, 0, 0])).await;
            let mut tick: u32 = 0;
            loop {
                let price = 21000.0 + (tick as f64 * 0.25);
                let volume = 1.0 + ((tick % 5) as f64);
                let mut trade_payload = Vec::new();
                trade_payload.extend_from_slice(&1u32.to_le_bytes()); // symbol_id
                trade_payload.push(if tick.is_multiple_of(2) { 2 } else { 1 }); // at ask / at bid
                trade_payload.extend_from_slice(&price.to_le_bytes());
                trade_payload.extend_from_slice(&volume.to_le_bytes());
                trade_payload.extend_from_slice(&(tick as f64).to_le_bytes());
                if socket
                    .write_all(&build_frame(107, &trade_payload))
                    .await
                    .is_err()
                {
                    break;
                }
                tick = tick.saturating_add(1);
                sleep(Duration::from_millis(250)).await;
            }
        });
    }
}
