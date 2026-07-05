---
name: coaching-test
description: Check partner-output grounding and quality. USE WHEN modifying prompts, MCP coaching-related tools, or verifying that trade ideas and opinions are evidence-grounded.
---

# /coaching-test

Validate partner-output behavior without a frontend test suite (React was removed).
The standard is `AGENT.md` "Grounded Partnership": conviction is welcome, hedging is not
— but every opinion, trade idea, and statistic must be grounded and traceable.

## Steps

1. **Manual / agent review:** Walk through orchestrator and risk-coach prompt templates in `agents/` and any coaching strings referenced from MCP or docs.

2. Test each prompt type with sample structured data:

   **Setup Trigger Prompt:**
   - Input: DNVA Reversion setup triggered, price at 21432, DNP at 21448, upper DNVA at 21461
   - Verify: prompt mentions setup name, specific price levels, backtest metrics with `N`, risk state
   - Verify: traces to the specific playbook rule that fired

   **Trade-Idea Proposal:**
   - Input: rebid zone held + retest, delta confirms, setup stats +0.22R avg (N=64, backtest-verified)
   - Verify: the proposal is direct (direction, entry zone, stop, target) — no hedge-speak burying a grounded read
   - Verify: every claim cites its evidence; stats carry `N` + reliability tier + verified provenance
   - Verify: sub-threshold samples (`N < 30`) are framed as directional / candidate-for-backtest, not full conviction

   **Risk Warning Prompt:**
   - Input: trader at 2.5R drawdown, max daily loss is 3R
   - Verify: prompt mentions specific R values, the trader's own rule, and the consequence
   - Verify: factual tone; hard stops and circuit breakers stated as binary — no softening, and no trade idea offered past a triggered stop

   **Trade Management Prompt:**
   - Input: first target hit, trader's plan says trim half
   - Verify: prompt references the specific management rule; any deviation opinion is grounded in data

3. **Graceful degradation:** Confirm raw alerts still surface when the Claude API is unavailable (no UI dependency).

4. **Grounding scan** (read `AGENT.md` "Grounded Partnership" first):
   - Scan for **ungrounded conviction**: directional claims, "I like / I'd pass", or entry/stop/target proposals with no cited structure, flow, rule, or stat
   - Scan for **naked statistics**: any percentage/expectancy without `N` and reliability tier
   - Scan for **over-hedging**: grounded reads wrapped in vague "may be favorable" language — this is a failure too
   - Verify Layer 2 alerts attribute to the trader's rules; agent-originated ideas are labeled as agent ideas

5. Report: grounding status (both failure directions) and any violations with exact phrases.
