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
            // Encoding response: ProtocolVersion(8) | Encoding(0=binary) | "DTC\0"
            let mut enc_resp = Vec::with_capacity(12);
            enc_resp.extend_from_slice(&8i32.to_le_bytes());
            enc_resp.extend_from_slice(&0i32.to_le_bytes());
            enc_resp.extend_from_slice(b"DTC\0");
            let _ = socket.write_all(&build_frame(7, &enc_resp)).await;

            // Logon response: ProtocolVersion(8) | Result(1=success)
            let mut logon_resp = Vec::with_capacity(8);
            logon_resp.extend_from_slice(&8i32.to_le_bytes());
            logon_resp.extend_from_slice(&1i32.to_le_bytes());
            let _ = socket.write_all(&build_frame(2, &logon_resp)).await;
            let mut tick: u32 = 0;
            loop {
                let price = 21000.0 + (tick as f64 * 0.25);
                let volume = 1.0 + ((tick % 5) as f64);
                let side: u16 = if tick.is_multiple_of(2) { 2 } else { 1 }; // AT_ASK / AT_BID

                // s_MarketDataUpdateTrade (type 107, pack(8)):
                //   SymbolID(u32) | AtBidOrAsk(u16) | padding(6) |
                //   Price(f64) | Volume(f64) | DateTime(f64)
                let mut trade_payload = Vec::with_capacity(36);
                trade_payload.extend_from_slice(&1u32.to_le_bytes());
                trade_payload.extend_from_slice(&side.to_le_bytes());
                trade_payload.extend_from_slice(&[0u8; 6]);
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
