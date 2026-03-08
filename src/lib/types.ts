export type SetupState =
  | "notActive"
  | "approaching"
  | "conditionsMet"
  | "confirmed"
  | "inTrade"
  | "closed";

export interface Setup {
  id: string;
  name: string;
  description: string;
  active: boolean;
  conditions: string[];
  minDelta?: number;
  requireAboveVwap?: boolean;
  duplicateSuppressionMs?: number;
  entryLogic?: Record<string, unknown>;
  stopLogic?: Record<string, unknown>;
  targets?: Record<string, unknown>[];
  positionSizing?: Record<string, unknown>;
  marketContext?: Record<string, unknown>;
  invalidation?: Record<string, unknown>[];
  backtestResults?: BacktestResults;
  contextBacktestResults?: Record<string, unknown>[];
  discretionaryConditions?: string[];
  templateSource?: string | null;
}

export interface BacktestResults {
  period?: string;
  samples?: number;
  winRate?: number;
  avgWinnerR?: number;
  avgLoserR?: number;
  profitFactor?: number;
  maxConsecutiveLosses?: number;
  maxDrawdownR?: number;
  expectancyR?: number;
  source?: string;
  importedAt?: number;
}

export interface SetupAlert {
  setupId: string;
  setupName: string;
  stateTransition: SetupState;
  triggeredConditions: string[];
  currentPrice: number;
  timestamp: number;
}

export interface MarketState {
  lastPrice: number;
  bid: number;
  ask: number;
  vwap: number;
  vwap1sdUpper: number;
  vwap1sdLower: number;
  vwap2sdUpper: number;
  vwap2sdLower: number;
  vwap3sdUpper: number;
  vwap3sdLower: number;
  vaHigh: number;
  vaLow: number;
  poc: number;
  dnvaHigh: number;
  dnvaLow: number;
  dnp: number;
  sessionDelta: number;
  cumulativeDelta: number;
  priorDayHigh: number;
  priorDayLow: number;
  priorDayClose: number;
  priorVaHigh: number;
  priorVaLow: number;
  priorPoc: number;
  overnightHigh: number;
  overnightLow: number;
  orHigh: number;
  orLow: number;
  ibHigh: number;
  ibLow: number;
}

export interface RiskState {
  dailyPnlR: number;
  tradeCount: number;
  consecutiveLosses: number;
  consecutiveWins: number;
  drawdownR: number;
  maxDailyLossR: number;
  atLimit: boolean;
}

export interface CoachingPrompt {
  id: string;
  sessionEventId: string;
  setupId: string | null;
  setupName: string;
  message: string;
  priority: "info" | "alert" | "warning" | "critical" | "risk_warning";
  source: "llm" | "raw" | "replay";
  timestamp: number;
}

export interface SessionEventInput {
  eventType: string;
  setupId?: string | null;
  data: Record<string, unknown>;
  sessionId?: string | null;
}

export interface SessionEventRecord {
  id: number;
  eventType: string;
  setupId?: string | null;
  data: Record<string, unknown>;
  sessionId?: string | null;
  timestamp?: number | null;
}

export interface TradeInput {
  setupId?: string | null;
  direction: "long" | "short";
  size: number;
  entryPrice: number;
  exitPrice?: number;
  resultR?: number;
}

export interface TradeRecord {
  id: string;
  sessionId?: string | null;
  setupId?: string | null;
  entryTime: number;
  entryPrice: number;
  exitTime?: number | null;
  exitPrice?: number | null;
  direction: string;
  size: number;
  stopPrice?: number | null;
  targetPrices: number[];
  resultR?: number | null;
  planned: boolean;
  rulesFollowed?: boolean | null;
  emotionalState?: string | null;
  notes: string;
  source: string;
}

export interface SessionRecord {
  id: string;
  date: string;
  sessionType: string;
  startTime: number;
  endTime?: number | null;
  recordingPath?: string | null;
  preSessionNote?: string | null;
}

export interface JournalEntry {
  id: string;
  sessionId?: string | null;
  date: string;
  content: string;
  tags: string[];
  setupReferences: string[];
  tradeReferences: string[];
  createdAt: number;
}

export interface RiskConfigRecord {
  rValuePoints: number;
  rValueDollars: number;
  maxDailyLossR: number;
  maxConsecutiveLosses: number;
  maxTradesPerSession?: number | null;
  noTradeZones: unknown[];
  maxDailyLossDollars?: number | null;
}

export interface OpenPosition {
  direction: string;
  size: number;
  entryPrice: number;
  instrument?: string | null;
  setupId?: string | null;
}

export interface AccountStateRecord {
  lastBalanceDollars: number;
  lastBalanceUpdatedAtMs: number;
  openPositions: OpenPosition[];
  lucidDailyLossDollars?: number | null;
  lucidAccountSizeDollars?: number | null;
  profitTargetPerCycle?: number | null;
  positionSizingMethod: string;
  kellyFraction: number;
}

export interface DomLevel {
  price: number;
  quantity: number;
  numOrders: number;
  distanceFromTouchTicks: number;
}

export interface VolumeProfileLevel {
  price: number;
  buyVol: number;
  sellVol: number;
  totalVol: number;
}

export interface PullStackDelta {
  side: "bid" | "ask";
  price: number;
  stackedQuantity: number;
  removedQuantity: number;
  estimatedFilledQuantity: number;
  estimatedPulledQuantity: number;
}

export interface TapePrint {
  timestampMs: number;
  price: number;
  volume: number;
  side: "buy" | "sell" | "unknown";
  bid: number;
  ask: number;
  crossesSpread: boolean;
}

export interface DomReplayFrame {
  timestampMs: number;
  eventKind: "snapshot" | "trade" | "depth";
  bestBid: number | null;
  bestAsk: number | null;
  bids: DomLevel[];
  asks: DomLevel[];
  lastTrade: TapePrint | null;
  recentTape: TapePrint[];
  volumeProfile: VolumeProfileLevel[];
  pullStackDeltas: PullStackDelta[];
  cursor: number;
  totalEvents: number;
  clipStartMs: number;
  clipEndMs: number;
  warning?: string | null;
}

export interface DomReplayStatus {
  isLoaded: boolean;
  isPlaying: boolean;
  cursor: number;
  totalEvents: number;
  currentTimestampMs: number | null;
  startMs: number | null;
  endMs: number | null;
  speed: number;
  warning?: string | null;
}

export interface DomReplayLoadResult {
  tickCount: number;
  depthBatchCount: number;
  totalEvents: number;
  startMs: number;
  endMs: number;
  sourceSummary: string;
  warning?: string | null;
}
