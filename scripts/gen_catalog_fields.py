#!/usr/bin/env python3
"""Generate curated MarketState field specs for Desk Catalog v0."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MOD = ROOT / "src" / "pipelines" / "mod.rs"
OUT = ROOT / "src" / "catalog" / "market_state_fields.rs"


def snake_to_camel(s: str) -> str:
    parts = s.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


def extract_fields(body: str) -> list[tuple[str, str]]:
    lines = body.splitlines()
    cur: list[str] = []
    fields: list[tuple[str, str]] = []
    for line in lines:
        s = line.strip()
        if s.startswith("///"):
            cur.append(s[3:].strip())
        elif s.startswith("pub "):
            m = re.match(r"pub ([a-zA-Z0-9_]+):", s)
            if m:
                # Empty description when no /// — matches CI drift test (no silent
                # field-name fallback that would disagree with source docs).
                fields.append((m.group(1), " ".join(cur) if cur else ""))
                cur = []
        elif s.startswith("//"):
            cur = []
    return fields


def classify(name: str) -> tuple[str, str, str, str, str]:
    if name in (
        "root_symbol",
        "contract_symbol",
        "contract_month",
        "symbol_resolution_mode",
        "symbol_resolution_source",
        "trading_day",
        "session_type",
        "session_segment",
        "rollover_warning",
        "carry_forward_levels_valid",
        "prior_day_contract_symbol",
    ):
        unit = "Text"
        if name in ("symbol_resolution_mode", "symbol_resolution_source", "session_type", "session_segment"):
            unit = "EnumLabel"
        if name == "carry_forward_levels_valid":
            unit = "Bool"
        scope = "Session" if name in ("session_type", "session_segment") else "CrossSession"
        fresh = "PriorSessionCarry" if name == "prior_day_contract_symbol" else "SessionScoped"
        return "identity", unit, scope, fresh, "R0"

    if name == "dom_summary":
        return "liquidity", "StructuredBlob", "Session", "DelayedDepthOptional", "R1"

    if name == "dom_book_present" or name.startswith(
        (
            "book_velocity_",
            "iceberg_reload_",
            "stop_run_",
            "stop_cluster_",
            "pull_intent_",
            "mm_flow_",
        )
    ):
        unit = "EnumLabel"
        if name == "dom_book_present":
            unit = "Bool"
        elif name.endswith("_price"):
            unit = "PricePoints"
        elif name.endswith("_per_sec") or name.endswith("_rate") or name.endswith("_score"):
            unit = "Ratio"
        return "liquidity", unit, "Session", "DelayedDepthOptional", "R1"

    if name.startswith("tape_") or name == "pace_percentile":
        unit = "TicksPerSec"
        if "volume" in name:
            unit = "ContractsPerSec"
        if "percentile" in name or "coverage" in name or "acceleration" in name:
            unit = "Ratio"
        if name.startswith("tape_valid"):
            unit = "Bool"
        if name.endswith("_ms") or "dwell" in name:
            unit = "Milliseconds"
        return "flow", unit, "Session", "LiveTickAnchored", "R1"

    if name.startswith("leg_") or name.startswith("last_leg_"):
        unit = "Contracts"
        if name in ("leg_status",) or "direction" in name:
            unit = "EnumLabel"
        elif name.endswith("_ms") or "age" in name:
            unit = "Milliseconds"
        elif name.startswith("leg_poc_at") or name.startswith("leg_poc_in") or "overlaps" in name:
            unit = "Bool"
        elif (
            "price" in name
            or name.endswith("_poc")
            or name.endswith("_hvn")
            or name.endswith("_lvn")
            or name.endswith("_va_high")
            or name.endswith("_va_low")
        ):
            unit = "PricePoints"
        elif "volume" in name:
            unit = "Contracts"
        elif "delta" in name:
            unit = "Contracts"
        return "flow", unit, "Session", "LiveTickAnchored", "R1"

    if name.startswith("rvol_"):
        unit = "Ratio"
        if name in ("rvol_classification", "rvol_baseline_status"):
            unit = "EnumLabel"
        if name == "rvol_caveat":
            unit = "Text"
        if name in ("rvol_bucket_index", "rvol_lookback_days_actual"):
            unit = "Count"
        if name == "rvol_expected_volume":
            unit = "Contracts"
        if name == "rvol_percentile":
            unit = "Percent"
        return "volatility", unit, "Rth", "LiveTickAnchored", "R1"

    if any(
        name.startswith(p)
        for p in (
            "absorption",
            "confirmed_",
            "has_recent_",
            "recent_",
            "pinch_",
            "imbalance",
            "avg_trade",
        )
    ):
        unit = "Count"
        if "price" in name:
            unit = "PricePoints"
        if "direction" in name:
            unit = "EnumLabel"
        if "age_ms" in name:
            unit = "Milliseconds"
        if "distance_ticks" in name:
            unit = "Ticks"
        if name.startswith("has_"):
            unit = "Bool"
        if name == "avg_trade_size":
            unit = "Contracts"
        return "flow", unit, "Session", "LiveTickAnchored", "R1"

    if any(name.startswith(p) for p in ("rebid_", "reoffer_", "nearest_zone", "active_zone")):
        unit = "Bool"
        if "count" in name:
            unit = "Count"
        if name.endswith("_ticks"):
            unit = "Ticks"
        if name.endswith("_low") or name.endswith("_high"):
            unit = "PricePoints"
        if "direction" in name or "status" in name:
            unit = "EnumLabel"
        return "response", unit, "Session", "LiveTickAnchored", "R1"

    if name in (
        "day_type",
        "profile_shape",
        "balance_state",
        "single_prints_direction",
        "ib_extension_state",
        "regime",
    ):
        return "location_structure", "EnumLabel", "Rth", "SessionScoped", "R0"

    if name in ("session_delta", "globex_delta", "cumulative_delta"):
        scope = {
            "session_delta": "Segment",
            "globex_delta": "Globex",
            "cumulative_delta": "CrossSession",
        }[name]
        return "flow", "Contracts", scope, "LiveTickAnchored", "R1"

    if name.startswith("inventory") or name == "sessions_in_trend":
        unit = "Count" if name == "sessions_in_trend" else "EnumLabel"
        return "cross_market", unit, "CrossSession", "SessionScoped", "R1"

    unit = "PricePoints"
    if name.endswith("_locked") or name.endswith("_retested") or name in (
        "poor_high",
        "poor_low",
        "excess_high",
        "excess_low",
    ):
        unit = "Bool"
    if "direction" in name:
        unit = "EnumLabel"

    scope = "Session"
    if name.startswith("prior_") or name.startswith("overnight_"):
        scope = "CrossSession"
    if name.startswith(("or_", "ib_", "or5_")) or name in (
        "session_high",
        "session_low",
        "rth_close_price",
        "va_high",
        "va_low",
        "poc",
        "dnva_high",
        "dnva_low",
        "dnp",
    ):
        scope = "Rth"
    if name.startswith("vwap") or name in ("last_price", "bid", "ask"):
        scope = "Session"
    if name.startswith("globex_") or name.startswith("london_"):
        scope = "Globex"

    cost = "R1"
    if name in (
        "last_price",
        "bid",
        "ask",
        "vwap",
        "poc",
        "va_high",
        "va_low",
        "dnp",
        "ib_high",
        "ib_low",
        "or_high",
        "or_low",
        "or5_high",
        "or5_low",
        "or5_mid",
    ) or name.startswith("prior_day"):
        cost = "R0"

    fresh = "PriorSessionCarry" if name.startswith("prior_") else "LiveTickAnchored"
    return "location_structure", unit, scope, fresh, cost


def main() -> None:
    text = MOD.read_text(encoding="utf-8")
    m = re.search(r"pub struct MarketState \{(.*?)\n\}", text, re.S)
    if not m:
        raise SystemExit("MarketState not found")
    fields = extract_fields(m.group(1))

    lines = [
        "//! Curated FieldSpec table for `MarketState` — descriptions come from",
        "//! field doc-comments; unit / session scope / freshness / cost are",
        "//! annotated here so the Desk Catalog cannot drift from runtime state.",
        "//!",
        "//! Regenerate skeleton: `python scripts/gen_catalog_fields.py`",
        "",
        "use super::types::*;",
        "",
        "/// Annotated specs for every `MarketState` field (snake_case rust_field).",
        "pub(crate) fn market_state_field_specs() -> Vec<FieldSpec> {",
        "    vec![",
    ]
    for name, desc in fields:
        domain, unit, scope, fresh, cost = classify(name)
        camel = snake_to_camel(name)
        fid = f"market.{domain}.{camel}"
        desc_esc = desc.replace("\\", "\\\\").replace('"', '\\"')
        lines.extend(
            [
                "        FieldSpec {",
                f'            id: "{fid}",',
                f'            rust_field: "{name}",',
                f'            name: "{camel}",',
                f'            domain_id: "{domain}",',
                f'            description: "{desc_esc}",',
                f"            unit: Unit::{unit},",
                f"            session_scope: SessionScope::{scope},",
                f"            freshness: FreshnessSemantics::{fresh},",
                f"            cost_hint: CostHint::{cost},",
                "        },",
            ]
        )
    lines.extend(["    ]", "}", ""])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {len(fields)} fields -> {OUT}")


if __name__ == "__main__":
    main()
