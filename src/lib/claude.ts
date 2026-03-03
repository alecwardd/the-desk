import type { CoachingPrompt, MarketState, RiskState, Setup, SetupAlert, TradeRecord } from "./types";

export interface PromptContext {
  alert: SetupAlert;
  setup: Setup | null;
  risk: RiskState | null;
  notes: string[];
}

export interface ManagementContext {
  setup: Setup;
  trade: TradeRecord | null;
  market: MarketState;
  risk: RiskState | null;
}

export interface BriefingContext {
  market: MarketState;
  setups: Setup[];
  risk: RiskState | null;
  preSessionNote?: string;
  lastBalance?: number;
  openPositions?: Array<{ direction: string; size: number; entryPrice: number }>;
}

/**
 * Generate a coaching prompt from a setup alert. Tries LLM first, falls back
 * to raw deterministic text when the Claude API is unavailable.
 */
export async function generateCoachingPrompt(context: PromptContext): Promise<CoachingPrompt> {
  try {
    const { invoke } = await import("@tauri-apps/api/core");
    const system = buildSystemPrompt();
    const userMessage = buildUserMessage(context);

    const response = await invoke<string>("call_claude_api", {
      messages: [{ role: "user", content: userMessage }],
      system,
    });

    return {
      id: crypto.randomUUID(),
      sessionEventId: crypto.randomUUID(),
      setupId: context.setup?.id ?? context.alert.setupId,
      setupName: context.setup?.name ?? context.alert.setupName,
      message: response,
      priority: "alert",
      source: "llm",
      timestamp: Date.now(),
    };
  } catch {
    return buildRawFallback(context);
  }
}

/**
 * Generate a risk warning prompt when approaching limits.
 */
export async function generateRiskWarning(risk: RiskState): Promise<CoachingPrompt> {
  const pctOfLimit = Math.abs(risk.dailyPnlR / risk.maxDailyLossR) * 100;

  let message: string;
  if (risk.atLimit) {
    message = `Your daily loss limit of ${risk.maxDailyLossR}R has been reached. Your rules say to stop trading for the day.`;
  } else if (pctOfLimit >= 80) {
    message = `You're at ${risk.dailyPnlR.toFixed(1)}R of your ${risk.maxDailyLossR}R daily limit (${pctOfLimit.toFixed(0)}%). Your rules say to reduce size or be highly selective at this point.`;
  } else if (risk.consecutiveLosses >= 3) {
    message = `${risk.consecutiveLosses} consecutive losses. Your playbook recommends stepping away for 15 minutes to reset.`;
  } else {
    message = `Risk check: ${risk.dailyPnlR.toFixed(1)}R daily P&L, ${risk.tradeCount} trades, ${risk.consecutiveLosses} consecutive losses.`;
  }

  return {
    id: crypto.randomUUID(),
    sessionEventId: crypto.randomUUID(),
    setupId: null,
    setupName: "Risk Monitor",
    message,
    priority: "risk_warning",
    source: "raw",
    timestamp: Date.now(),
  };
}

/**
 * Generate a trade management prompt for an open position.
 */
export async function generateManagementPrompt(context: ManagementContext): Promise<CoachingPrompt | null> {
  const { setup, trade, market, risk } = context;

  if (trade && trade.exitTime) return null;

  const parts: string[] = [];

  if (trade) {
    const pnlR = trade.resultR ?? 0;
    const direction = trade.direction === "long" ? "long" : "short";
    const currentPnl = direction === "long"
      ? (market.lastPrice - trade.entryPrice)
      : (trade.entryPrice - market.lastPrice);

    parts.push(
      `You have an open ${direction} from ${trade.entryPrice.toFixed(2)}. Current price: ${market.lastPrice.toFixed(2)}.`
    );

    if (trade.stopPrice) {
      parts.push(`Your stop is at ${trade.stopPrice.toFixed(2)}.`);
    }
    if (trade.targetPrices.length > 0) {
      const targets = trade.targetPrices.map((p) => p.toFixed(2)).join(", ");
      parts.push(`Your targets: ${targets}.`);
    }

    parts.push(`Based on your setup "${setup.name}", check if management rules apply.`);
  } else {
    parts.push(
      `Setup "${setup.name}" conditions are met. If you're in this trade, review your management rules.`
    );
  }

  if (risk) {
    parts.push(`Session risk: ${risk.dailyPnlR.toFixed(1)}R, ${risk.tradeCount} trades.`);
  }

  const message = parts.join(" ");

  return {
    id: crypto.randomUUID(),
    sessionEventId: crypto.randomUUID(),
    setupId: setup.id,
    setupName: setup.name,
    message,
    priority: "info",
    source: "raw",
    timestamp: Date.now(),
  };
}

/**
 * Generate a pre-session briefing narrative using LLM.
 */
export async function generateBriefingSynthesis(context: BriefingContext): Promise<string> {
  try {
    const { invoke } = await import("@tauri-apps/api/core");

    const system = [
      "You are a pre-session briefing assistant for an NQ futures trader.",
      "Summarize the market structure levels and which playbook setups may be relevant today.",
      "Never give trading advice or predict market direction.",
      'Frame as "based on your levels..." and "your setups align with...".',
      "Keep the briefing to 3-5 sentences.",
    ].join(" ");

    const parts: string[] = [];

    const m = context.market;
    if (m.priorDayHigh > 0) {
      parts.push(
        `Prior day: H ${m.priorDayHigh.toFixed(2)}, L ${m.priorDayLow.toFixed(2)}, C ${m.priorDayClose.toFixed(2)}.`
      );
    }
    if (m.overnightHigh > 0) {
      parts.push(
        `Overnight range: ${m.overnightLow.toFixed(2)} - ${m.overnightHigh.toFixed(2)}.`
      );
    }
    if (m.priorVaHigh > 0) {
      parts.push(
        `Prior VA: ${m.priorVaLow.toFixed(2)} - ${m.priorVaHigh.toFixed(2)}, POC ${m.priorPoc.toFixed(2)}.`
      );
    }

    const activeSetups = context.setups.filter((s) => s.active);
    if (activeSetups.length > 0) {
      parts.push(
        `Active setups: ${activeSetups.map((s) => s.name).join(", ")}.`
      );
    }

    if (context.risk) {
      parts.push(
        `Risk state from prior session: ${context.risk.dailyPnlR.toFixed(1)}R P&L.`
      );
    }

    if (context.lastBalance != null && context.lastBalance > 0) {
      parts.push(`Last confirmed balance: $${context.lastBalance.toLocaleString()}.`);
    }
    if (context.openPositions && context.openPositions.length > 0) {
      parts.push(
        `Open positions not from chat: ${context.openPositions.map((p) => `${p.size} ${p.direction} @ $${p.entryPrice}`).join("; ")}.`
      );
    }

    if (context.preSessionNote) {
      parts.push(`Trader's focus note: "${context.preSessionNote}".`);
    }

    parts.push("Provide a concise pre-session briefing based on these levels and the trader's playbook.");

    const response = await invoke<string>("call_claude_api", {
      messages: [{ role: "user", content: parts.join(" ") }],
      system,
      model: "opus",
    });

    return response;
  } catch {
    return buildRawBriefing(context);
  }
}

function buildRawBriefing(context: BriefingContext): string {
  const m = context.market;
  const parts: string[] = [];

  if (m.priorDayHigh > 0) {
    parts.push(
      `Prior day range: ${m.priorDayLow.toFixed(2)} - ${m.priorDayHigh.toFixed(2)}, close ${m.priorDayClose.toFixed(2)}.`
    );
  }
  if (m.overnightHigh > 0) {
    parts.push(
      `Overnight: ${m.overnightLow.toFixed(2)} - ${m.overnightHigh.toFixed(2)}.`
    );
  }
  if (m.priorVaHigh > 0) {
    parts.push(
      `Prior value area: ${m.priorVaLow.toFixed(2)} - ${m.priorVaHigh.toFixed(2)}.`
    );
  }

  const activeSetups = context.setups.filter((s) => s.active);
  if (activeSetups.length > 0) {
    parts.push(`${activeSetups.length} active setup(s) loaded.`);
  }

  return parts.length > 0 ? parts.join(" ") : "No prior-day data available yet.";
}

function buildSystemPrompt(): string {
  return [
    "You are a trading coach that reflects the trader's own playbook rules back to them.",
    "You NEVER give trading advice, predict market direction, or suggest trades.",
    'Frame all observations as "your rules say..." or "your playbook indicates...".',
    "Keep responses concise (2-3 sentences).",
    "Focus on what the trader's predefined conditions show.",
  ].join(" ");
}

function buildUserMessage(context: PromptContext): string {
  const { alert, risk, notes } = context;
  const parts: string[] = [];

  parts.push(
    `Setup "${alert.setupName}" transitioned to ${alert.stateTransition} at price ${alert.currentPrice.toFixed(2)}.`
  );

  if (alert.triggeredConditions.length > 0) {
    parts.push(`Conditions met: ${alert.triggeredConditions.join(", ")}.`);
  }

  if (risk) {
    parts.push(
      `Risk state: ${risk.dailyPnlR.toFixed(2)}R daily P&L, ${risk.tradeCount} trades, ${risk.drawdownR.toFixed(2)}R drawdown.`
    );
    if (risk.atLimit) parts.push("RISK LIMIT REACHED.");
  }

  if (notes.length > 0) {
    parts.push(`Trader note: "${notes[0]}".`);
  }

  parts.push("Provide a coaching observation based on the trader's own playbook rules.");
  return parts.join(" ");
}

function buildRawFallback(context: PromptContext): CoachingPrompt {
  const { alert, setup, risk, notes } = context;
  const header = `Your rules say ${alert.setupName} is in play.`;
  const conditionText =
    alert.triggeredConditions.length > 0
      ? `Conditions met: ${alert.triggeredConditions.join(", ")}.`
      : "Conditions met based on your active playbook rules.";
  const riskText = risk
    ? `Risk state: ${risk.dailyPnlR.toFixed(2)}R, ${risk.tradeCount} trades, drawdown ${risk.drawdownR.toFixed(2)}R.`
    : "Risk state unavailable.";
  const notesText =
    notes.length > 0 ? `Relevant journal note: ${notes[0]}` : "No relevant journal notes.";
  const message = `${header} ${conditionText} ${riskText} ${notesText} Frame decisions through your predefined entry, stop, and target rules.`;

  return {
    id: crypto.randomUUID(),
    sessionEventId: crypto.randomUUID(),
    setupId: setup?.id ?? alert.setupId,
    setupName: setup?.name ?? alert.setupName,
    message,
    priority: "alert",
    source: "raw",
    timestamp: Date.now(),
  };
}
