---
name: coaching-test
description: Test the LLM coaching layer with simulated setup triggers. USE WHEN modifying prompts, testing Claude API integration, or verifying coaching output quality.
---

# /coaching-test

Test coaching prompt generation with simulated scenarios.

## Steps

1. Run coaching prompt tests:
   ```bash
   cd src && npm test -- --grep "coaching"
   ```

2. Test each prompt type with sample data:

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

3. Test graceful degradation:
   - Simulate Claude API timeout
   - Verify: raw alert displayed without coaching prose
   - Verify: no crash, no hang, no blank screen

4. Compliance check (read `skills/compliance-research/SKILL.md` first):
   - Scan all prompt templates for forbidden phrases
   - Verify every prompt includes attribution to trader's rules
   - Flag any language that could be interpreted as financial advice

5. Report:
   - All prompt types tested
   - Compliance status (pass/fail with specific violations)
   - API latency measurements
   - Graceful degradation status
