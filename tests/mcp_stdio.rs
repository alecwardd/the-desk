use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn mcp_startup_does_not_write_logs_to_stdout() {
    let home = tempfile::tempdir().expect("temp home");
    let exe = env!("CARGO_BIN_EXE_the-desk-mcp");

    let mut child = Command::new(exe)
        .env("USERPROFILE", home.path())
        .env("HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn MCP server");

    std::thread::sleep(Duration::from_millis(750));
    let _ = child.kill();
    let output = child.wait_with_output().expect("collect server output");

    assert!(
        output.stdout.is_empty(),
        "MCP stdout must stay protocol-only; startup produced: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}
