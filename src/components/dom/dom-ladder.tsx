import type { DomReplayFrame, PullStackDelta } from "@/lib/types";
import { cn } from "@/lib/utils";

interface DomLadderProps {
  frame: DomReplayFrame | null;
}

function deltaHighlight(price: number, deltas: PullStackDelta[]) {
  const match = deltas.find((delta) => delta.price === price);
  if (!match) return null;
  if (match.stackedQuantity > match.removedQuantity) return "stack";
  if (match.removedQuantity > 0) return "pull";
  return null;
}

export function DomLadder({ frame }: DomLadderProps) {
  const bidMap = new Map(frame?.bids.map((level) => [level.price, level]) ?? []);
  const askMap = new Map(frame?.asks.map((level) => [level.price, level]) ?? []);
  const maxQty = Math.max(
    1,
    ...Array.from(bidMap.values(), (level) => level.quantity),
    ...Array.from(askMap.values(), (level) => level.quantity)
  );

  const rows = Array.from(new Set([...bidMap.keys(), ...askMap.keys()])).sort((a, b) => b - a);
  const lastTradePrice = frame?.lastTrade?.price ?? null;

  return (
    <div className="rounded-xl border border-border bg-[#090d14] min-h-0 overflow-hidden">
      <div className="grid grid-cols-[1fr_88px_1fr] border-b border-border bg-[#0d1320] px-3 py-2 text-[11px] uppercase tracking-[0.18em] text-text-secondary">
        <div>Bid</div>
        <div className="text-center">Price</div>
        <div className="text-right">Ask</div>
      </div>
      <div className="max-h-[70vh] overflow-auto font-mono text-sm">
        {rows.length === 0 ? (
          <div className="px-4 py-10 text-center text-text-secondary">
            Load a replay clip to render the ladder.
          </div>
        ) : (
          rows.map((price) => {
            const bid = bidMap.get(price);
            const ask = askMap.get(price);
            const highlight = deltaHighlight(price, frame?.pullStackDeltas ?? []);
            const isLastTrade = lastTradePrice === price;

            return (
              <div
                key={price}
                className={cn(
                  "grid grid-cols-[1fr_88px_1fr] items-center border-b border-[#121926] px-2 py-0.5",
                  isLastTrade && "bg-[rgba(33,150,243,0.22)]",
                  highlight === "stack" && "bg-[rgba(76,175,80,0.08)]",
                  highlight === "pull" && "bg-[rgba(255,152,0,0.09)]"
                )}
              >
                <div className="relative h-6 overflow-hidden rounded-sm">
                  {bid && (
                    <div
                      className="absolute inset-y-0 right-0 bg-[#2196f3]/45"
                      style={{ width: `${(bid.quantity / maxQty) * 100}%` }}
                    />
                  )}
                  <div className="relative z-10 flex h-full items-center justify-end pr-2 text-[#77c3ff]">
                    {bid?.quantity ?? ""}
                  </div>
                </div>
                <div
                  className={cn(
                    "text-center text-text-primary",
                    frame?.bestBid === price && "text-[#64b5f6]",
                    frame?.bestAsk === price && "text-[#ff6b6b]",
                    isLastTrade &&
                      (frame?.lastTrade?.side === "buy" ? "text-[#4caf50]" : "text-[#f44336]")
                  )}
                >
                  {price.toFixed(2)}
                </div>
                <div className="relative h-6 overflow-hidden rounded-sm">
                  {ask && (
                    <div
                      className="absolute inset-y-0 left-0 bg-[#f44336]/45"
                      style={{ width: `${(ask.quantity / maxQty) * 100}%` }}
                    />
                  )}
                  <div className="relative z-10 flex h-full items-center justify-start pl-2 text-[#ff8a80]">
                    {ask?.quantity ?? ""}
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
