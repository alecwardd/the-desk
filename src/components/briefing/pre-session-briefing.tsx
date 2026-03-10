import { useCallback, useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import type {
  AccountStateRecord,
  MarketState,
  MemoryBrief,
  RiskState,
  Setup,
} from "../../lib/types";
import { accountBridge, memoryBridge } from "../../lib/tauri-bridge";
import { generateBriefingSynthesis } from "../../lib/claude";

interface Props {
  marketState: MarketState | null;
  setups: Setup[];
  riskState?: RiskState | null;
  onStartSession: (focusNote?: string) => void;
}

export function PreSessionBriefing({ marketState, setups, riskState, onStartSession }: Props) {
  const [focusNote, setFocusNote] = useState("");
  const [briefingNarrative, setBriefingNarrative] = useState<string | null>(null);
  const [loadingBriefing, setLoadingBriefing] = useState(false);
  const [accountState, setAccountState] = useState<AccountStateRecord | null>(null);
  const [currentBalance, setCurrentBalance] = useState("");
  const [memoryBrief, setMemoryBrief] = useState<MemoryBrief | null>(null);
  const activeSetups = setups.filter((s) => s.active);

  useEffect(() => {
    accountBridge.get().then((s) => {
      if (s) setAccountState(s);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (!marketState || briefingNarrative) return;
    setLoadingBriefing(true);
    generateBriefingSynthesis({
      market: marketState,
      setups,
      risk: riskState ?? null,
      preSessionNote: focusNote || undefined,
      lastBalance: accountState?.lastBalanceDollars,
      openPositions: accountState?.openPositions,
    })
      .then(setBriefingNarrative)
      .catch(() => setBriefingNarrative(null))
      .finally(() => setLoadingBriefing(false));
  }, [marketState]);

  useEffect(() => {
    memoryBridge
      .getPreSessionBriefing(focusNote || undefined)
      .then(setMemoryBrief)
      .catch(() => setMemoryBrief(null));
  }, [focusNote]);

  const handleStartSession = useCallback(async () => {
    const balanceNum = currentBalance ? parseFloat(currentBalance) : accountState?.lastBalanceDollars;
    if (balanceNum != null && balanceNum > 0) {
      try {
        await accountBridge.save({ lastBalanceDollars: balanceNum });
      } catch {
        // non-Tauri mode or bridge unavailable
      }
    }
    onStartSession(focusNote || undefined);
  }, [currentBalance, accountState, focusNote, onStartSession]);

  return (
    <Card className="w-full max-w-lg">
      <CardHeader>
        <CardTitle>Pre-Session Briefing</CardTitle>
      </CardHeader>
      <CardContent className="space-y-5">
        {briefingNarrative && (
          <div className="rounded-md bg-surface p-3">
            <p className="text-text-secondary text-sm italic">{briefingNarrative}</p>
          </div>
        )}
        {loadingBriefing && (
          <p className="text-text-muted text-xs">Generating briefing...</p>
        )}
        {memoryBrief && (
          <div className="rounded-md border border-border-subtle bg-surface p-3 space-y-2">
            <div>
              <h3 className="text-text-primary text-sm font-semibold">Carry-Forward Memory</h3>
              <p className="text-text-muted text-xs">
                {memoryBrief.patterns.length} patterns, {memoryBrief.insights.length} insights,{" "}
                {memoryBrief.followups.length} follow-ups
              </p>
            </div>
            {memoryBrief.patterns.slice(0, 2).map((pattern) => (
              <p key={pattern.id} className="text-text-secondary text-sm">
                {pattern.description}
              </p>
            ))}
            {memoryBrief.insights.slice(0, 2).map((insight) => (
              <p key={insight.id} className="text-text-secondary text-sm">
                {insight.summary}
                <span className="text-text-muted"> [{insight.status}]</span>
              </p>
            ))}
            {memoryBrief.followups.slice(0, 2).map((followup) => (
              <p key={followup.id} className="text-text-secondary text-sm">
                Follow-up: {followup.title}
              </p>
            ))}
          </div>
        )}

        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">Account Check</h3>
          {accountState && accountState.lastBalanceDollars > 0 ? (
            <p className="text-text-secondary text-sm mb-2">
              Last balance: ${accountState.lastBalanceDollars.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              {accountState.lastBalanceUpdatedAtMs > 0 && (
                <span className="text-text-muted ml-1">
                  (updated {new Date(accountState.lastBalanceUpdatedAtMs).toLocaleDateString()})
                </span>
              )}
            </p>
          ) : (
            <p className="text-text-muted text-sm mb-2">No previous balance recorded.</p>
          )}
          <div className="space-y-1">
            <label className="text-text-secondary text-xs" htmlFor="current-balance">
              Current account balance ($)
            </label>
            <Input
              id="current-balance"
              type="number"
              min={0}
              step={100}
              placeholder={accountState?.lastBalanceDollars?.toString() ?? "e.g. 50000"}
              value={currentBalance}
              onChange={(e) => setCurrentBalance(e.target.value)}
              className="h-8"
            />
          </div>
        </div>

        <Separator />

        {riskState && (
          <div>
            <h3 className="text-text-primary mb-2 text-sm font-semibold">Risk State</h3>
            <div className="flex gap-3 text-sm">
              <Badge variant="outline">
                P&L: {riskState.dailyPnlR.toFixed(1)}R
              </Badge>
              <Badge variant="outline">
                Trades: {riskState.tradeCount}
              </Badge>
              <Badge variant="outline">
                Drawdown: {riskState.drawdownR.toFixed(1)}R
              </Badge>
              {riskState.atLimit && (
                <Badge variant="destructive">AT LIMIT</Badge>
              )}
            </div>
          </div>
        )}

        <Separator />

        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">Key Levels</h3>
          {marketState ? (
            <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
              <span className="text-text-muted">Prior High</span>
              <span className="text-text-primary">{marketState.priorDayHigh.toFixed(2)}</span>
              <span className="text-text-muted">Prior Low</span>
              <span className="text-text-primary">{marketState.priorDayLow.toFixed(2)}</span>
              <span className="text-text-muted">Prior Close</span>
              <span className="text-text-primary">{marketState.priorDayClose.toFixed(2)}</span>
              {marketState.priorVaHigh > 0 && (
                <>
                  <span className="text-text-muted">Prior VA</span>
                  <span className="text-text-primary">
                    {marketState.priorVaLow.toFixed(2)} – {marketState.priorVaHigh.toFixed(2)}
                  </span>
                  <span className="text-text-muted">Prior POC</span>
                  <span className="text-text-primary">{marketState.priorPoc.toFixed(2)}</span>
                </>
              )}
              <span className="text-text-muted">ON High</span>
              <span className="text-text-primary">{marketState.overnightHigh.toFixed(2)}</span>
              <span className="text-text-muted">ON Low</span>
              <span className="text-text-primary">{marketState.overnightLow.toFixed(2)}</span>
              <span className="text-text-muted">VWAP</span>
              <span className="text-text-primary">{marketState.vwap.toFixed(2)}</span>
              <span className="text-text-muted">VA</span>
              <span className="text-text-primary">
                {marketState.vaLow.toFixed(2)} – {marketState.vaHigh.toFixed(2)}
              </span>
              <span className="text-text-muted">POC</span>
              <span className="text-text-primary">{marketState.poc.toFixed(2)}</span>
            </div>
          ) : (
            <p className="text-text-muted text-sm">Waiting for market data…</p>
          )}
        </div>

        <Separator />

        <div>
          <h3 className="text-text-primary mb-2 text-sm font-semibold">
            Active Setups ({activeSetups.length})
          </h3>
          {activeSetups.length > 0 ? (
            <ul className="list-inside list-disc space-y-1 text-sm text-text-secondary">
              {activeSetups.map((s) => (
                <li key={s.id}>
                  {s.name}
                  {s.description ? ` — ${s.description}` : ""}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-text-muted text-sm">
              No active setups. Add one in the Playbook Builder.
            </p>
          )}
        </div>

        <div className="space-y-2">
          <label className="text-text-secondary text-sm" htmlFor="focus-note">
            Session focus note
          </label>
          <Textarea
            id="focus-note"
            placeholder="What are you focusing on today?"
            value={focusNote}
            onChange={(e) => setFocusNote(e.target.value)}
          />
        </div>

        <Button onClick={handleStartSession} className="w-full">
          Start Session
        </Button>
      </CardContent>
    </Card>
  );
}
