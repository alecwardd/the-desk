import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import type { MarketState, SetupAlert } from "../../lib/types";

interface Props {
  marketState: MarketState | null;
  setupAlerts: SetupAlert[];
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
              <div className="flex justify-between">
                <span className="text-text-muted">Last</span>
                <span className="text-text-primary font-bold">
                  {marketState.lastPrice.toFixed(2)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">VWAP</span>
                <span className="text-text-primary font-bold">
                  {marketState.vwap.toFixed(2)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">VA</span>
                <span className="text-text-primary font-bold">
                  {marketState.vaLow.toFixed(2)} – {marketState.vaHigh.toFixed(2)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">DNVA</span>
                <span className="text-text-primary font-bold">
                  {marketState.dnvaLow.toFixed(2)} – {marketState.dnvaHigh.toFixed(2)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">Delta</span>
                <span className="text-text-primary font-bold">
                  {marketState.cumulativeDelta.toFixed(0)}
                </span>
              </div>
            </div>
          )}

          <Separator />

          <div>
            <h3 className="text-text-secondary font-semibold mb-1">Recent Setup States</h3>
            {setupAlerts.slice(0, 5).map((alert) => (
              <div
                key={`${alert.setupId}-${alert.timestamp}`}
                className="flex justify-between text-xs py-0.5"
              >
                <span className="text-text-muted">{alert.setupName}</span>
                <span className="text-text-primary">{alert.stateTransition}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </aside>
  );
}
