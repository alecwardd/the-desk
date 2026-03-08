import type { TapePrint } from "@/lib/types";
import { cn } from "@/lib/utils";

interface TapeStripProps {
  prints: TapePrint[];
}

function formatTime(timestampMs: number) {
  return new Date(timestampMs).toLocaleTimeString([], {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

export function TapeStrip({ prints }: TapeStripProps) {
  return (
    <div className="rounded-xl border border-border bg-[#0a0f16] min-h-0 overflow-hidden">
      <div className="grid grid-cols-[92px_60px_44px] border-b border-border bg-[#101824] px-3 py-2 text-[11px] uppercase tracking-[0.18em] text-text-secondary">
        <div>Time</div>
        <div className="text-right">Price</div>
        <div className="text-right">Vol</div>
      </div>
      <div className="max-h-[70vh] overflow-auto font-mono text-xs">
        {prints.length === 0 ? (
          <div className="px-4 py-10 text-center text-text-secondary">
            Tape prints appear during replay.
          </div>
        ) : (
          prints
            .slice()
            .reverse()
            .map((print) => (
              <div
                key={`${print.timestampMs}-${print.price}-${print.volume}`}
                className={cn(
                  "grid grid-cols-[92px_60px_44px] border-b border-[#121926] px-3 py-1",
                  print.side === "buy" && "text-[#4caf50]",
                  print.side === "sell" && "text-[#ff6b6b]",
                  print.crossesSpread && "bg-[rgba(255,255,255,0.03)]",
                  print.volume > 1 && "font-semibold"
                )}
              >
                <div>{formatTime(print.timestampMs)}</div>
                <div className="text-right">{print.price.toFixed(2)}</div>
                <div className="text-right">{print.volume.toFixed(0)}</div>
              </div>
            ))
        )}
      </div>
    </div>
  );
}
