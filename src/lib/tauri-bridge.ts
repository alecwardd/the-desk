import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type {
  CoachingPrompt,
  MarketState,
  RiskState,
  SessionEventInput,
  SessionEventRecord,
  Setup,
  SetupAlert,
  TradeInput
} from "./types";

type Unlisten = () => void;

export const events = {
  marketState: "market-state",
  setupAlert: "setup-alert",
  coachingPrompt: "coaching-prompt",
  riskState: "risk-state",
  dtcStatus: "dtc-status"
} as const;

export async function subscribe<T>(
  eventName: string,
  handler: (payload: T) => void
): Promise<Unlisten> {
  const unlisten = await listen<T>(eventName, (event) => handler(event.payload));
  return unlisten;
}

export const dtcBridge = {
  connect: (host: string, port: number, symbol: string) =>
    invoke<void>("connect_dtc", { host, port, symbol }),
  disconnect: () => invoke<void>("disconnect_dtc"),
  status: () => invoke<string>("dtc_status"),
  startMockFeed: () => invoke<void>("start_mock_feed"),
};

export const setupBridge = {
  list: () => invoke<Setup[]>("list_setups"),
  create: (setup: Setup) => invoke<Setup>("create_setup", { setup })
};

export const riskBridge = {
  get: () => invoke<RiskState>("get_risk_state")
};

export const sessionBridge = {
  start: () => invoke<string>("start_session"),
  stop: () => invoke<void>("stop_session"),
  addEvent: (event: SessionEventInput) => invoke<void>("add_session_event", { event }),
  addTrade: (trade: TradeInput) => invoke<void>("add_trade", { trade }),
  listEvents: (limit = 200) => invoke<SessionEventRecord[]>("list_session_events", { limit })
};

export const replayBridge = {
  loadRecording: (path: string) => invoke<number>("load_recording", { path }),
  start: (speed = 1) => invoke<void>("start_replay", { speed }),
  pause: () => invoke<void>("pause_replay"),
  seek: (index: number) => invoke<void>("seek_replay", { index }),
  stop: () => invoke<void>("stop_replay")
};

export type StreamPayloads = {
  [events.marketState]: MarketState;
  [events.setupAlert]: SetupAlert;
  [events.coachingPrompt]: CoachingPrompt;
  [events.riskState]: RiskState;
  [events.dtcStatus]: string;
};
