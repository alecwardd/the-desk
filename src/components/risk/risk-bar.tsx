import { Badge } from "@/components/ui/badge";
import type { RiskState } from "../../lib/types";

interface Props {
  riskState: RiskState | null;
  connection: string;
}

export function RiskBar({ riskState, connection }: Props) {
  const isConnected = connection === "connected";

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
