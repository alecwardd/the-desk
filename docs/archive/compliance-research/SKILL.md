---
name: ComplianceResearch
description: ARCHIVED 2026-07-05. Securities compliance research for a public-product scenario that is not planned. Superseded by AGENT.md "Grounded Partnership".
---

> **ARCHIVED (2026-07-05).** The Desk is a private, single-trader tool with no public
> launch planned, so the coaching-vs-advisory phrasing boundary this skill enforced no
> longer applies. The active doctrine is `AGENT.md` "Grounded Partnership": agents may
> propose trade ideas with entries, stops, and targets — the discipline is evidence
> grounding (playbook rules, structure/flow, backtested stats with `N`), not softened
> phrasing. This file is retained only in case productization is ever revisited; if it
> is, start with fresh counsel — do not treat this research as current.

# Securities Compliance Research

Guide for investigating the regulatory positioning of The Desk as a coaching/discipline tool rather than an investment advisory service.

---

## The Core Question

**Is The Desk a "investment adviser" under SEC/CFTC regulations, or is it a tool/software product?**

The Desk's position: it is a **coaching and discipline tool** that reflects a trader's own rules back to them. It does not provide investment advice, generate signals, or recommend trades.

---

## The Coaching vs. Advisory Distinction

### What The Desk Does (Coaching)

- Stores the trader's own rules in a structured playbook
- Evaluates live market data against the trader's predefined conditions
- Displays alerts that say "your rules say X" — not "you should do X"
- Tracks the trader's risk state against their own limits
- Reviews past sessions against the trader's own plan
- Every prompt traces to a specific rule the trader defined

### What The Desk Does NOT Do (Advisory)

- Generate trade ideas from its own models
- Recommend specific securities, entry prices, or positions
- Provide proprietary market analysis or forecasts
- Give personalized investment advice based on the trader's financial situation
- Hold itself out as an investment adviser
- Charge fees based on assets under management

### Analogies

| Tool | Regulatory Status | Similar Because |
|------|-------------------|-----------------|
| Workout tracking app | Not practicing medicine | Reflects user's own plan, doesn't prescribe treatment |
| Pilot's checklist app | Not an aviation authority | Enforces procedures the pilot already committed to |
| Grammar checker | Not a ghostwriter | Applies rules to user's own content |
| Speedometer | Not a driving instructor | Reports data, user decides how to act |

---

## Regulatory Framework (US)

### SEC (Securities and Exchange Commission)

**Investment Advisers Act of 1940:**
- Defines "investment adviser" as someone who, for compensation, engages in the business of advising others about securities
- Key test: does the person/entity provide advice about specific securities?
- Software that provides tools without making recommendations has generally been treated as a "publisher" or tool

**Relevant precedents to research:**
- [ ] Lowe v. SEC (1985) — publisher vs. adviser distinction
- [ ] SEC no-action letters regarding software tools
- [ ] SEC guidance on robo-advisers (does The Desk fall in this category?)
- [ ] Treatment of charting/technical analysis software

### CFTC (Commodity Futures Trading Commission)

Since The Desk initially focuses on futures (NQ):
- **Commodity Trading Advisor (CTA):** Anyone who advises others about futures trading for compensation
- Key question: does The Desk "advise" on futures, or does it provide tools?
- The coaching framing is critical here — reflecting the trader's own rules should not constitute "advice"

**Relevant considerations:**
- [ ] CTA registration requirements and exemptions
- [ ] CFTC guidance on software tools vs. advisory services
- [ ] NFA (National Futures Association) compliance requirements
- [ ] Treatment of backtesting/journaling tools in futures context

### NFA (National Futures Association)

- Self-regulatory organization for the US derivatives industry
- Requires registration for CTAs
- May have specific guidance on software tools

---

## Research Tasks

When this skill is invoked, spawn a research subagent with these specific queries:

### Priority 1: Core Legal Position

1. **Search:** "software tool vs investment adviser SEC guidance coaching trading"
2. **Search:** "commodity trading advisor exemption software tool CFTC"
3. **Search:** "trading journal software regulatory classification SEC CFTC"
4. **Search:** "Lowe v SEC publisher exemption software"

### Priority 2: Precedent Products

5. **Research how similar products position themselves legally:**
   - TraderSync (journaling) — how do they avoid CTA registration?
   - Trade Ideas (AI signals) — are they registered? How do they position?
   - TrendSpider (automated technical analysis) — regulatory status?
   - Edgewonk (journaling + analytics) — any regulatory disclosures?

### Priority 3: Specific Guidance

6. **Search:** "NFA guidance automated trading tools software registration"
7. **Search:** "SEC no-action letter trading software tool"
8. **Search:** "CFTC CTA exemption technology platform"

### Priority 4: Marketing Language

9. **Research best practices for disclaimer language:**
   - What disclaimers do similar tools use?
   - What language should The Desk avoid in marketing and UI?
   - How should the product be described to avoid implying advisory status?

---

## Key Distinctions in Product Design

### Language That Helps (Use This)

- "Your rules say..."
- "Your playbook indicates..."
- "Based on your predefined conditions..."
- "Your backtest results show..."
- "According to your risk parameters..."
- "The conditions you defined are met"
- "Your plan for this scenario is..."

### Language That Hurts (Never Use This)

- "You should buy/sell..."
- "This is a good trade..."
- "I recommend..."
- "The market is likely to..."
- "This setup has high probability..." (without attribution to user's own backtest)
- "Take this trade..."
- "This is a buying/selling opportunity..."

### UI Design Implications

- Every alert must visually trace to a specific playbook rule
- Include a "Why am I seeing this?" link that shows the exact rule and data
- Never present information in a way that looks like a trade recommendation
- Always frame prompts as "your plan" not "our analysis"
- Include clear disclaimers on the main dashboard and session view

---

## Recommended Actions

1. **Before public launch:** Engage a securities attorney specializing in fintech/trading technology to review:
   - Product positioning and marketing language
   - Terms of service and disclaimers
   - UI copy and prompt templates
   - Whether any registration (CTA, IA, NFA) is required

2. **Budget estimate:** $3,000-$8,000 for initial legal review

3. **Ongoing:** Review any new features (especially Phase 2 options integration) with counsel before release, as adding proprietary analysis could shift the regulatory classification

---

## Status

- [ ] Initial legal research completed
- [ ] Securities attorney engaged
- [ ] Product positioning reviewed by counsel
- [ ] Disclaimer language approved
- [ ] UI copy reviewed for compliance
- [ ] Terms of service drafted and reviewed

---

*This document is a research guide, not legal advice. All conclusions must be validated by a qualified securities attorney.*
