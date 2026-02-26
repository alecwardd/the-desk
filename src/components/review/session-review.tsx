import { useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type {
  CoachingPrompt,
  RiskState,
  SessionEventRecord,
  SetupAlert,
  TradeRecord,
} from "../../lib/types";

interface Props {
  prompts: CoachingPrompt[];
  alerts: SetupAlert[];
  riskState: RiskState | null;
  sessionEvents: SessionEventRecord[];
  sessionId?: string | null;
  preSessionNote?: string;
}

const EMOTIONAL_STATES = [
  "Calm",
  "Focused",
  "Anxious",
  "FOMO",
  "Frustrated",
  "Overconfident",
  "Revenge",
  "Bored",
] as const;

function computePromptAdherence(events: SessionEventRecord[]): string {
  const responses = events.filter((e) => e.eventType === "prompt_response");
  const setupResponses = responses.filter(
    (e) => (e.data as Record<string, unknown>).setupId != null
  );
  if (setupResponses.length === 0) return "N/A";

  const tookIt = setupResponses.filter(
    (e) => (e.data as Record<string, unknown>).response === "took_it"
  );
  return `${Math.round((tookIt.length / setupResponses.length) * 100)}%`;
}

interface TradeCardState {
  planned: boolean;
  emotionalState: string;
  notes: string;
}

export function SessionReview({
  prompts,
  alerts,
  riskState,
  sessionEvents,
  sessionId,
  preSessionNote,
}: Props) {
  const llmPrompts = prompts.filter((p) => p.source === "llm");
  const rawPrompts = prompts.filter((p) => p.source === "raw");
  const uniqueSetups = new Set(alerts.map((a) => a.setupId));

  const promptAdherence = useMemo(
    () => computePromptAdherence(sessionEvents),
    [sessionEvents]
  );

  const trades = useMemo(() => {
    return sessionEvents
      .filter((e) => e.eventType === "trade_opened" || e.eventType === "trade_closed")
      .reduce<TradeRecord[]>((acc, e) => {
        const data = e.data as unknown as TradeRecord;
        if (data?.id && !acc.some((t) => t.id === data.id)) acc.push(data);
        return acc;
      }, []);
  }, [sessionEvents]);

  const [tradeEdits, setTradeEdits] = useState<Record<string, TradeCardState>>({});
  const [journalText, setJournalText] = useState("");

  function getTradeEdit(tradeId: string): TradeCardState {
    return tradeEdits[tradeId] ?? { planned: true, emotionalState: "Calm", notes: "" };
  }

  function updateTradeEdit(tradeId: string, patch: Partial<TradeCardState>) {
    setTradeEdits((prev) => ({
      ...prev,
      [tradeId]: { ...getTradeEdit(tradeId), ...patch },
    }));
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Session Review</CardTitle>
        {sessionId && (
          <span className="text-text-muted text-xs">Session: {sessionId.slice(0, 8)}</span>
        )}
      </CardHeader>
      <CardContent className="space-y-4">
        {preSessionNote && (
          <div className="rounded bg-surface-raised p-2">
            <span className="text-text-muted text-xs font-semibold">Pre-session note</span>
            <p className="text-text-secondary mt-0.5 text-sm">{preSessionNote}</p>
          </div>
        )}

        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">Summary</h3>
          <div className="flex flex-wrap gap-3 text-sm">
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">Prompts</Badge>
              <span className="text-text-primary">{prompts.length}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">LLM</Badge>
              <span className="text-text-primary">{llmPrompts.length}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">Raw</Badge>
              <span className="text-text-primary">{rawPrompts.length}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">Alerts</Badge>
              <span className="text-text-primary">{alerts.length}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">Setups</Badge>
              <span className="text-text-primary">{uniqueSetups.size}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Badge variant="secondary">Events</Badge>
              <span className="text-text-primary">{sessionEvents.length}</span>
            </div>
          </div>
        </div>

        <Separator />
        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">Adherence</h3>
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
            <span className="text-text-muted">Prompt Adherence</span>
            <span className="text-text-primary font-mono">{promptAdherence}</span>
            <span className="text-text-muted">Rules Adherence</span>
            <span className="text-text-primary font-mono">
              {trades.length > 0
                ? `${Math.round(
                    (trades.filter((t) => t.rulesFollowed === true).length / trades.length) * 100
                  )}%`
                : "N/A"}
            </span>
          </div>
        </div>

        {riskState && (
          <>
            <Separator />
            <div>
              <h3 className="text-text-primary mb-2 text-sm font-semibold">Risk</h3>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                <span className="text-text-muted">Daily P&L</span>
                <span className="text-text-primary">{riskState.dailyPnlR.toFixed(2)}R</span>
                <span className="text-text-muted">Trades</span>
                <span className="text-text-primary">{riskState.tradeCount}</span>
                <span className="text-text-muted">Consecutive losses</span>
                <span className="text-text-primary">{riskState.consecutiveLosses}</span>
                <span className="text-text-muted">Drawdown</span>
                <span className="text-text-primary">{riskState.drawdownR.toFixed(2)}R</span>
              </div>
              {riskState.atLimit && (
                <p className="mt-2 text-sm font-semibold text-critical">Risk limit reached</p>
              )}
            </div>
          </>
        )}

        {trades.length > 0 && (
          <>
            <Separator />
            <div>
              <h3 className="text-text-primary mb-2 text-sm font-semibold">Trades</h3>
              <div className="space-y-2">
                {trades.map((trade) => {
                  const edit = getTradeEdit(trade.id);
                  const resultColor =
                    trade.resultR != null
                      ? trade.resultR >= 0
                        ? "text-positive"
                        : "text-critical"
                      : "text-text-muted";

                  return (
                    <div
                      key={trade.id}
                      className="rounded border border-border-subtle bg-surface p-2.5"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Badge
                            variant={trade.direction === "long" ? "default" : "destructive"}
                          >
                            {trade.direction.toUpperCase()}
                          </Badge>
                          <span className="text-text-primary text-sm font-mono">
                            {trade.entryPrice}
                          </span>
                          {trade.exitPrice != null && (
                            <>
                              <span className="text-text-muted text-xs">→</span>
                              <span className="text-text-primary text-sm font-mono">
                                {trade.exitPrice}
                              </span>
                            </>
                          )}
                        </div>
                        <span className={`text-sm font-semibold font-mono ${resultColor}`}>
                          {trade.resultR != null ? `${trade.resultR > 0 ? "+" : ""}${trade.resultR.toFixed(2)}R` : "Open"}
                        </span>
                      </div>

                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <Button
                          variant={edit.planned ? "default" : "outline"}
                          size="xs"
                          onClick={() => updateTradeEdit(trade.id, { planned: true })}
                        >
                          Planned
                        </Button>
                        <Button
                          variant={!edit.planned ? "destructive" : "outline"}
                          size="xs"
                          onClick={() => updateTradeEdit(trade.id, { planned: false })}
                        >
                          Unplanned
                        </Button>

                        <Select
                          value={edit.emotionalState}
                          onValueChange={(v) =>
                            updateTradeEdit(trade.id, { emotionalState: v })
                          }
                        >
                          <SelectTrigger size="sm" className="h-6 w-32 text-xs">
                            <SelectValue placeholder="Emotional state" />
                          </SelectTrigger>
                          <SelectContent>
                            {EMOTIONAL_STATES.map((state) => (
                              <SelectItem key={state} value={state}>
                                {state}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      <Input
                        placeholder="Trade notes..."
                        className="mt-2 h-7 text-xs"
                        value={edit.notes}
                        onChange={(e) =>
                          updateTradeEdit(trade.id, { notes: e.target.value })
                        }
                      />
                    </div>
                  );
                })}
              </div>
            </div>
          </>
        )}

        {prompts.length > 0 && (
          <>
            <Separator />
            <div>
              <h3 className="text-text-primary mb-2 text-sm font-semibold">Prompt Log</h3>
              <div className="max-h-[200px] space-y-0 overflow-y-auto">
                {prompts.slice(0, 20).map((p) => (
                  <div key={p.id} className="border-b border-border-subtle py-1">
                    <strong className="text-text-primary text-sm">{p.setupName}</strong>
                    <span className="text-text-muted text-sm"> [{p.source}]</span>
                    <br />
                    <small className="text-text-secondary">
                      {p.message.slice(0, 120)}
                      {p.message.length > 120 ? "…" : ""}
                    </small>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}

        <Separator />
        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">Journal Entry</h3>
          <Textarea
            placeholder="What did you learn today? What would you do differently?"
            value={journalText}
            onChange={(e) => setJournalText(e.target.value)}
            className="min-h-24 text-sm"
          />
        </div>
      </CardContent>
    </Card>
  );
}
