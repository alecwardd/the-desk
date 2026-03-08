import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DomVisualizer } from "./dom-visualizer";
import type { DomReplayFrame } from "@/lib/types";

const invokeMock = vi.fn<
  (command: string, args?: Record<string, unknown>) => Promise<unknown>
>();
let domReplayHandler: ((frame: DomReplayFrame) => void) | null = null;

vi.mock("@tauri-apps/api/core", () => ({
  invoke: (command: string, args?: Record<string, unknown>) => invokeMock(command, args),
}));

vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn((eventName: string, callback: (event: { payload: DomReplayFrame }) => void) => {
    if (eventName === "dom-replay-frame") {
      domReplayHandler = (frame) => callback({ payload: frame });
    }
    return Promise.resolve(() => {});
  }),
}));

describe("DomVisualizer", () => {
  beforeEach(() => {
    domReplayHandler = null;
    invokeMock.mockReset();
    invokeMock.mockImplementation((command) => {
      if (command === "dom_replay_status") {
        return Promise.resolve({
          isLoaded: false,
          isPlaying: false,
          cursor: 0,
          totalEvents: 0,
          currentTimestampMs: null,
          startMs: null,
          endMs: null,
          speed: 1,
          warning: null,
        });
      }
      if (command === "dom_replay_load") {
        return Promise.resolve({
          tickCount: 10,
          depthBatchCount: 5,
          totalEvents: 15,
          startMs: 1,
          endMs: 2,
          sourceSummary: "ticks: sqlite; depth: sqlite",
          warning: null,
        });
      }
      return Promise.resolve(undefined);
    });
  });

  it("loads a clip via the DOM replay bridge", async () => {
    render(<DomVisualizer />);
    await userEvent.click(screen.getByRole("button", { name: "Load Clip" }));

    await waitFor(() => {
      expect(
        invokeMock.mock.calls.some(([command]) => command === "dom_replay_load")
      ).toBe(true);
    });
  });

  it("renders ladder data from DOM replay frames", async () => {
    render(<DomVisualizer />);

    await act(async () => {
      domReplayHandler?.({
        timestampMs: 1_700_000_000_000,
        eventKind: "depth",
        bestBid: 24660.75,
        bestAsk: 24661,
        bids: [
          {
            price: 24660.75,
            quantity: 12,
            numOrders: 2,
            distanceFromTouchTicks: 0,
          },
        ],
        asks: [
          {
            price: 24661,
            quantity: 18,
            numOrders: 3,
            distanceFromTouchTicks: 0,
          },
        ],
        lastTrade: null,
        recentTape: [],
        volumeProfile: [
          { price: 24660.75, buyVol: 40, sellVol: 10, totalVol: 50 },
        ],
        pullStackDeltas: [],
        cursor: 1,
        totalEvents: 15,
        clipStartMs: 1_700_000_000_000,
        clipEndMs: 1_700_000_100_000,
        warning: null,
      });
    });

    expect(screen.getAllByText("24660.75").length).toBeGreaterThan(0);
    expect(screen.getByText("12")).toBeInTheDocument();
    expect(screen.getByText("18")).toBeInTheDocument();
  });
});
