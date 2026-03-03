import { Badge } from "@/components/ui/badge";
import type { RiskConfigRecord, RiskState } from "../../lib/types";

interface Props {
  riskState: RiskState | null;
  riskConfig?: RiskConfigRecord | null;
  connection: string;
}

export function RiskBar({ riskState, riskConfig, connection }: Props) {
  const isConnected = connection === "connected";
  const dollarDisplay =
    riskState &&
    riskConfig &&
    (riskConfig.maxDailyLossDollars ?? riskConfig.rValueDollars * riskState.maxDailyLossR) > 0;
  const usedDollars =
    riskState && riskState.dailyPnlR < 0 && riskConfig
      ? Math.abs(riskState.dailyPnlR) * riskConfig.rValueDollars
      : 0;
  const limitDollars =
    riskConfig?.maxDailyLossDollars ??
    (riskConfig && riskState ? riskConfig.rValueDollars * riskState.maxDailyLossR : 0);

  return (
    <header className="flex items-center justify-between px-4 border-b border-border">
      <strong className="text-text-primary text-lg tracking-tight">The Desk</strong>

      <div className="flex items-center gap-3">
        <span className="text-text-secondary text-sm">DTC:</span>
        <Badge variant={isConnected ? "default" : "destructive"}>
          {connection}
        </Badge>
      </div>

      <span className="text-text-secondary text-sm">
        {riskState ? (
          <>
            <span className="text-text-muted">P&L </span>
            <span className="font-bold text-text-primary">
              {riskState.dailyPnlR.toFixed(2)}R
            </span>
            {dollarDisplay && limitDollars > 0 && (
              <span className="text-text-muted ml-1">
                (~${usedDollars.toFixed(0)} of ${limitDollars.toFixed(0)})
              </span>
            )}
            <span className="text-text-muted mx-2">|</span>
            <span className="text-text-muted">Trades </span>
            <span className="font-bold text-text-primary">{riskState.tradeCount}</span>
            <span className="text-text-muted mx-2">|</span>
            <span className="text-text-muted">Drawdown </span>
            <span className="font-bold text-text-primary">
              {riskState.drawdownR.toFixed(2)}R
            </span>
          </>
        ) : (
          "Risk state unavailable"
        )}
      </span>
    </header>
  );
}
