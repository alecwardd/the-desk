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
  priority: "info" | "alert" | "warning" | "critical";
  source: "llm" | "raw";
  timestamp: number;
}

export interface SessionEventInput {
  eventType: string;
  setupId?: string | null;
  data: Record<string, unknown>;
}

export interface SessionEventRecord {
  id: number;
  eventType: string;
  setupId?: string | null;
  data: Record<string, unknown>;
}

export interface TradeInput {
  setupId?: string | null;
  direction: "long" | "short";
  size: number;
  entryPrice: number;
  exitPrice?: number;
  resultR?: number;
}
