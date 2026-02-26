import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import type { MarketState, SetupAlert } from "../../lib/types";

interface Props {
  marketState: MarketState | null;
  setupAlerts: SetupAlert[];
}

function fmt(n: number) {
  return n > 0 ? n.toFixed(2) : "—";
}

export function MarketSidebar({ marketState, setupAlerts }: Props) {
  return (
    <aside aria-label="Market state">
      <Card className="h-full">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Market State</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          {!marketState ? (
            <p className="text-text-muted">Waiting for market data.</p>
          ) : (
            <div className="space-y-1">
              <Row label="Last" value={fmt(marketState.lastPrice)} bold />
              <Row label="VWAP" value={fmt(marketState.vwap)} bold />
              <Row
                label="+1σ / −1σ"
                value={`${fmt(marketState.vwap1sdUpper)} / ${fmt(marketState.vwap1sdLower)}`}
              />
              <Row
                label="+2σ / −2σ"
                value={`${fmt(marketState.vwap2sdUpper)} / ${fmt(marketState.vwap2sdLower)}`}
              />

              <Separator className="my-1" />

              <Row label="VA" value={`${fmt(marketState.vaLow)} – ${fmt(marketState.vaHigh)}`} bold />
              <Row label="POC" value={fmt(marketState.poc)} />
              <Row label="DNVA" value={`${fmt(marketState.dnvaLow)} – ${fmt(marketState.dnvaHigh)}`} />
              <Row label="DNP" value={fmt(marketState.dnp)} />
              <Row label="Delta" value={marketState.sessionDelta.toFixed(0)} bold />

              {marketState.priorVaHigh > 0 && (
                <>
                  <Separator className="my-1" />
                  <Row label="Prior VA" value={`${fmt(marketState.priorVaLow)} – ${fmt(marketState.priorVaHigh)}`} />
                  <Row label="Prior POC" value={fmt(marketState.priorPoc)} />
                </>
              )}

              <Separator className="my-1" />

              <Row label="OR" value={`${fmt(marketState.orLow)} – ${fmt(marketState.orHigh)}`} />
              <Row label="IB" value={`${fmt(marketState.ibLow)} – ${fmt(marketState.ibHigh)}`} />
            </div>
          )}

          <Separator />

          <div>
            <h3 className="text-text-secondary font-semibold mb-1">Setup States</h3>
            {setupAlerts.length === 0 ? (
              <p className="text-text-muted text-xs">No alerts yet.</p>
            ) : (
              setupAlerts.slice(0, 5).map((alert) => (
                <div
                  key={`${alert.setupId}-${alert.timestamp}`}
                  className="flex justify-between text-xs py-0.5"
                >
                  <span className="text-text-muted">{alert.setupName}</span>
                  <span className={stateColor(alert.stateTransition)}>
                    {alert.stateTransition}
                  </span>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>
    </aside>
  );
}

function Row({ label, value, bold }: { label: string; value: string; bold?: boolean }) {
  return (
    <div className="flex justify-between">
      <span className="text-text-muted">{label}</span>
      <span className={bold ? "text-text-primary font-bold" : "text-text-primary"}>
        {value}
      </span>
    </div>
  );
}

function stateColor(state: string): string {
  switch (state) {
    case "conditionsMet":
      return "text-positive font-semibold";
    case "approaching":
      return "text-warning";
    case "inTrade":
      return "text-info font-semibold";
    default:
      return "text-text-primary";
  }
}
