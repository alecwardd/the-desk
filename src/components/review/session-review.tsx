import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import type {
  CoachingPrompt,
  RiskState,
  SessionEventRecord,
  SetupAlert
} from "../../lib/types";

interface Props {
  prompts: CoachingPrompt[];
  alerts: SetupAlert[];
  riskState: RiskState | null;
  sessionEvents: SessionEventRecord[];
}

export function SessionReview({ prompts, alerts, riskState, sessionEvents }: Props) {
  const llmPrompts = prompts.filter((p) => p.source === "llm");
  const rawPrompts = prompts.filter((p) => p.source === "raw");
  const uniqueSetups = new Set(alerts.map((a) => a.setupId));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Session Review</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
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

        {prompts.length > 0 && (
          <>
            <Separator />
            <div>
              <h3 className="text-text-primary mb-2 text-sm font-semibold">Prompt Log</h3>
              <div className="max-h-[200px] space-y-0 overflow-y-auto">
                {prompts.slice(0, 20).map((p) => (
                  <div
                    key={p.id}
                    className="border-b border-border-subtle py-1"
                  >
                    <strong className="text-text-primary text-sm">{p.setupName}</strong>
                    <span className="text-text-muted text-sm"> [{p.source}]</span>
                    <br />
                    <small className="text-text-secondary">{p.message.slice(0, 120)}…</small>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
