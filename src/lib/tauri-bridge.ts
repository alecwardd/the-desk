import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type {
  AccountStateRecord,
  MemoryBrief,
  CoachingPrompt,
  JournalEntry,
  MarketState,
  RiskConfigRecord,
  RiskState,
  SessionEventInput,
  SessionEventRecord,
  SessionRecord,
  DomReplayFrame,
  DomReplayLoadResult,
  DomReplayStatus,
  Setup,
  SetupAlert,
  TradeInput,
  TradeRecord,
} from "./types";

type Unlisten = () => void;

export const events = {
  marketState: "market-state",
  setupAlert: "setup-alert",
  coachingPrompt: "coaching-prompt",
  riskState: "risk-state",
  /** Live SCID / replay feed connection state */
  feedStatus: "feed-status",
  domReplayFrame: "dom-replay-frame",
} as const;

export async function subscribe<T>(
  eventName: string,
  handler: (payload: T) => void
): Promise<Unlisten> {
  const unlisten = await listen<T>(eventName, (event) => handler(event.payload));
  return unlisten;
}

/** Sierra `.scid` tail via `~/.the-desk/config.toml` (no TCP DTC client). */
export const feedBridge = {
  status: () => invoke<string>("feed_status"),
  startScidFeed: () => invoke<void>("start_scid_feed"),
  stopScidFeed: () => invoke<void>("stop_scid_feed"),
};

/** @deprecated Use `feedBridge` — kept for gradual migration */
export const dtcBridge = feedBridge;

export const setupBridge = {
  list: () => invoke<Setup[]>("list_setups"),
  create: (setup: Setup) => invoke<Setup>("create_setup", { setup }),
  update: (setup: Setup) => invoke<Setup>("update_setup", { setup }),
  delete: (id: string) => invoke<void>("delete_setup", { id }),
  duplicate: (id: string) => invoke<Setup>("duplicate_setup", { id }),
  toggle: (id: string, active: boolean) => invoke<void>("toggle_setup", { id, active }),
  listTemplates: () => invoke<Setup[]>("list_templates"),
};

export const riskBridge = {
  get: () => invoke<RiskState>("get_risk_state"),
  getConfig: () => invoke<RiskConfigRecord>("get_risk_config"),
  saveConfig: (config: RiskConfigRecord) => invoke<void>("save_risk_config", { config }),
  initRiskState: () => invoke<RiskState>("init_risk_state"),
};

export const accountBridge = {
  get: () => invoke<AccountStateRecord | null>("get_account_state"),
  save: (input: Partial<{
    lastBalanceDollars: number;
    openPositions: Array<{ direction: string; size: number; entryPrice: number; instrument?: string; setupId?: string }>;
    lucidDailyLossDollars: number;
    lucidAccountSizeDollars: number;
    profitTargetPerCycle: number;
    positionSizingMethod: string;
    kellyFraction: number;
  }>) =>
    invoke<AccountStateRecord>("save_account_state", { input }),
};

export const sessionBridge = {
  start: (preSessionNote?: string) =>
    invoke<string>("start_session", {
      input: preSessionNote ? { preSessionNote } : undefined,
    }),
  stop: () => invoke<void>("stop_session"),
  list: (limit = 50) => invoke<SessionRecord[]>("list_sessions", { limit }),
  addEvent: (event: SessionEventInput) => invoke<void>("add_session_event", { event }),
  addTrade: (trade: TradeInput) => invoke<void>("add_trade", { trade }),
  listEvents: (limit = 200) =>
    invoke<SessionEventRecord[]>("list_session_events", { limit }),
};

export const tradeBridge = {
  create: (trade: TradeRecord) => invoke<TradeRecord>("create_trade", { trade }),
  close: (id: string, exitPrice: number, resultR: number) =>
    invoke<void>("close_trade", { id, exitPrice, resultR }),
  list: (sessionId: string) => invoke<TradeRecord[]>("list_trades", { sessionId }),
  getOpen: (sessionId: string) => invoke<TradeRecord | null>("get_open_trade", { sessionId }),
  review: (
    id: string,
    planned: boolean,
    rulesFollowed: boolean | null,
    emotionalState: string | null,
    thesis: string | null,
    reviewTags: string[],
    mistakeTags: string[],
    notes: string
  ) =>
    invoke<void>("review_trade", {
      id,
      planned,
      rulesFollowed,
      emotionalState,
      thesis,
      reviewTags,
      mistakeTags,
      notes,
    }),
};

export const journalBridge = {
  save: (entry: JournalEntry) => invoke<void>("save_journal_entry", { entry }),
  getForSession: (sessionId: string) =>
    invoke<JournalEntry[]>("get_journal", { sessionId }),
};

export const memoryBridge = {
  getBrief: (query: {
    intent: string;
    sessionId?: string | null;
    setupId?: string | null;
    sessionType?: string | null;
    sessionSegment?: string | null;
    dayType?: string | null;
    timeBucket?: string | null;
    preSessionNote?: string | null;
    limit?: number;
  }) => invoke<MemoryBrief>("get_memory_brief", { query }),
  getPreSessionBriefing: (preSessionNote?: string) =>
    invoke<MemoryBrief>("get_pre_session_briefing", { preSessionNote }),
};

export const replayBridge = {
  loadRecording: (path: string) => invoke<number>("load_recording", { path }),
  start: (speed = 1) => invoke<void>("start_replay", { speed }),
  pause: () => invoke<void>("pause_replay"),
  seek: (index: number) => invoke<void>("seek_replay", { index }),
  stop: () => invoke<void>("stop_replay"),
};

export const domReplayBridge = {
  load: (startMs: number, endMs: number, levelsPerSide = 12) =>
    invoke<DomReplayLoadResult>("dom_replay_load", { startMs, endMs, levelsPerSide }),
  start: (speed = 1) => invoke<void>("dom_replay_start", { speed }),
  pause: () => invoke<void>("dom_replay_pause"),
  stop: () => invoke<void>("dom_replay_stop"),
  seek: (timestampMs: number) => invoke<void>("dom_replay_seek", { timestampMs }),
  status: () => invoke<DomReplayStatus>("dom_replay_status"),
};

export type StreamPayloads = {
  [events.marketState]: MarketState;
  [events.setupAlert]: SetupAlert;
  [events.coachingPrompt]: CoachingPrompt;
  [events.riskState]: RiskState;
  [events.feedStatus]: string;
  [events.domReplayFrame]: DomReplayFrame;
};
