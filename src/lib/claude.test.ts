import { beforeEach, describe, expect, it, vi } from "vitest";
import { generateCoachingPrompt } from "./claude";
import type { PromptContext } from "./claude";

const invokeMock = vi.fn();

vi.mock("@tauri-apps/api/core", () => ({
  invoke: invokeMock
}));

function makeContext(): PromptContext {
  return {
    alert: {
      setupId: "setup-1",
      setupName: "VWAP Pullback",
      stateTransition: "conditionsMet",
      triggeredConditions: ["price_vs_vwap", "session_delta"],
      currentPrice: 21000,
      timestamp: Date.now()
    },
    setup: {
      id: "setup-1",
      name: "VWAP Pullback",
      description: "Test setup",
      active: true,
      conditions: ["price_vs_vwap=above"],
      minDelta: 100,
      requireAboveVwap: true,
      duplicateSuppressionMs: 2000
    },
    risk: {
      dailyPnlR: 0,
      tradeCount: 0,
      consecutiveLosses: 0,
      consecutiveWins: 0,
      drawdownR: 0,
      maxDailyLossR: 3,
      atLimit: false
    },
    notes: ["Wait for clear reclaim"]
  };
}

describe("generateCoachingPrompt", () => {
  beforeEach(() => {
    invokeMock.mockReset();
  });

  it("returns LLM prompt when backend command succeeds", async () => {
    invokeMock.mockResolvedValue("Your rules say conditions are aligned.");
    const prompt = await generateCoachingPrompt(makeContext());

    expect(invokeMock).toHaveBeenCalledWith("call_claude_api", expect.any(Object));
    expect(prompt.source).toBe("llm");
    expect(prompt.message).toContain("Your rules say");
  });

  it("falls back to deterministic raw prompt on backend failure", async () => {
    invokeMock.mockRejectedValue(new Error("missing key"));
    const prompt = await generateCoachingPrompt(makeContext());

    expect(prompt.source).toBe("raw");
    expect(prompt.message).toContain("Your rules say");
  });
});

