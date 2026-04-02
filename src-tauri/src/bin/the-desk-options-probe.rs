//! One-shot CLI probe for ConvexValue options-chain snapshots.
//!
//! Supports either:
//! - `--input path.json` for offline parsing of a captured chain payload
//! - live API fetches using env-backed credentials configured in `[options]`
//!
//! Examples:
//!   cargo run --bin the-desk-options-probe -- --input sample-chain.json
//!   cargo run --bin the-desk-options-probe -- --root SPX --exp 1 --exp 2 --top 10

use serde_json::json;
use the_desk_backend::options::{
    build_gamma_levels_report, load_options_config, parse_chain_rows, ConvexValueClient,
    OptionsCredentials,
};

fn print_help() {
    eprintln!(
        r#"the-desk-options-probe — Fetch or parse one ConvexValue options-chain snapshot

Usage:
  the-desk-options-probe [OPTIONS]

Options:
  --root SYMBOL         Root symbol to request. Default from [options], fallback SPX.
  --param NAME          Request one chain field. Repeatable.
  --exp N               Expiration selector accepted by ConvexValue. Repeatable.
  --rng VALUE           Range filter around spot (for example 0.10 for +/-10%).
  --top N               Number of gamma concentration levels to print. Default 12.
  --input PATH          Read raw ConvexValue JSON from disk instead of calling the API.
  --help, -h            Show this help.

Config:
  ~/.the-desk/config.toml

Optional [options] block:
  [options]
  enabled = true
  convexvalue_probe_root = "SPX"
  convexvalue_probe_params = ["gxoi", "gxvolm", "gamma", "oi", "volm_bs", "volm", "value_bs"]
  convexvalue_probe_exps = [1, 2, 3]
  convexvalue_probe_range = 0.10
  convexvalue_email_env = "CONVEXVALUE_EMAIL"
  convexvalue_password_env = "CONVEXVALUE_PASSWORD"

Examples:
  the-desk-options-probe --input c:\temp\spx-chain.json
  the-desk-options-probe --root SPX --exp 1 --exp 2 --rng 0.08 --top 8
"#
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_options_config();
    let mut args = std::env::args().skip(1);
    let mut root = config.convexvalue_probe_root.clone();
    let mut params = config.convexvalue_probe_params.clone();
    let mut exps = config.convexvalue_probe_exps.clone();
    let mut range = config.convexvalue_probe_range;
    let mut input_path: Option<String> = None;
    let mut top_n = 12usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = args.next().unwrap_or_default();
            }
            "--param" => {
                let param = args.next().unwrap_or_default();
                if params == config.convexvalue_probe_params {
                    params.clear();
                }
                if !param.trim().is_empty() {
                    params.push(param);
                }
            }
            "--exp" => {
                let exp = args
                    .next()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or("Invalid --exp value")?;
                if exps == config.convexvalue_probe_exps {
                    exps.clear();
                }
                exps.push(exp);
            }
            "--rng" => {
                range = Some(
                    args.next()
                        .and_then(|value| value.parse::<f64>().ok())
                        .ok_or("Invalid --rng value")?,
                );
            }
            "--top" => {
                top_n = args
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .ok_or("Invalid --top value")?;
            }
            "--input" => {
                input_path = args.next();
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {arg}");
                print_help();
                std::process::exit(1);
            }
        }
    }

    if root.trim().is_empty() {
        root = "SPX".to_string();
    }
    if params.is_empty() {
        params = config.convexvalue_probe_params.clone();
    }

    let raw = if let Some(path) = input_path.clone() {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str::<serde_json::Value>(&content)?
    } else {
        let creds = OptionsCredentials::from_env(&config)?;
        let client = ConvexValueClient::new(&config.convexvalue_base_url)?;
        client.login(&creds.email, &creds.password).await?;
        client
            .get_chain(
                &root,
                &params,
                if exps.is_empty() {
                    None
                } else {
                    Some(exps.as_slice())
                },
                range,
            )
            .await?
    };

    let rows = parse_chain_rows(&raw, &params)?;
    let report = build_gamma_levels_report(&root, &params, &rows, Some(top_n));
    let output = json!({
        "source": if input_path.is_some() { "input_file" } else { "convexvalue_live" },
        "root": root,
        "params": params,
        "requestedExpirations": if exps.is_empty() { serde_json::Value::Null } else { json!(exps) },
        "requestedRange": range,
        "report": report,
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
