---
name: coaching-test
description: Check coaching prompt quality and compliance. USE WHEN modifying prompts, MCP coaching-related tools, or verifying non-advisory phrasing.
---

# /coaching-test

Validate coaching behavior without a frontend test suite (React was removed).

## Steps

1. **Manual / agent review:** Walk through orchestrator and risk-coach prompt templates in `agents/` and any coaching strings referenced from MCP or docs.

2. Test each prompt type with sample structured data:

   **Setup Trigger Prompt:**
   - Input: DNVA Reversion setup triggered, price at 21432, DNP at 21448, upper DNVA at 21461
   - Verify: prompt mentions setup name, specific price levels, backtest metrics, risk state
   - Verify: NO advisory language ("you should", "good trade", "I recommend")
   - Verify: traces to specific playbook rule

   **Risk Warning Prompt:**
   - Input: trader at 2.5R drawdown, max daily loss is 3R
   - Verify: prompt mentions specific R values, trader's own rule, consequence
   - Verify: factual tone, not emotional

   **Trade Management Prompt:**
   - Input: first target hit, trader's plan says trim half
   - Verify: prompt references specific management rule, not generic advice

3. **Graceful degradation:** Confirm raw alerts still surface when the Claude API is unavailable (no UI dependency).

4. Compliance check (read `skills/compliance-research/SKILL.md` first):
   - Scan prompt templates for forbidden phrases
   - Verify attribution to trader's rules

5. Report: compliance status and any violations.
