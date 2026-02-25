import type { CoachingPrompt, RiskState, Setup, SetupAlert } from "./types";

export interface PromptContext {
  alert: SetupAlert;
  setup: Setup | null;
  risk: RiskState | null;
  notes: string[];
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
