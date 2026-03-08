import type { TapePrint, VolumeProfileLevel } from "@/lib/types";
import { cn } from "@/lib/utils";

interface VolumeProfileProps {
  levels: VolumeProfileLevel[];
  lastTrade: TapePrint | null;
}

export function VolumeProfile({ levels, lastTrade }: VolumeProfileProps) {
  const maxVol = Math.max(1, ...levels.map((level) => level.totalVol));
  const poc = levels.reduce<VolumeProfileLevel | null>((best, level) => {
    if (!best || level.totalVol > best.totalVol) return level;
    return best;
  }, null);

  return (
    <div className="rounded-xl border border-border bg-[#0b1119] min-h-0 overflow-hidden">
      <div className="border-b border-border bg-[#101824] px-3 py-2 text-[11px] uppercase tracking-[0.18em] text-text-secondary">
        Session Profile
      </div>
      <div className="max-h-[70vh] overflow-auto font-mono text-xs">
        {levels.length === 0 ? (
          <div className="px-4 py-10 text-center text-text-secondary">
            Volume profile appears after loading a replay.
          </div>
        ) : (
          levels.map((level) => {
            const buyWidth = (level.buyVol / maxVol) * 100;
            const sellWidth = (level.sellVol / maxVol) * 100;
            const isPoc = poc?.price === level.price;
            const isLastTrade = lastTrade?.price === level.price;
            return (
              <div
                key={level.price}
                className={cn(
                  "grid grid-cols-[64px_1fr_54px] items-center gap-2 border-b border-[#121926] px-2 py-0.5",
                  isPoc && "bg-[rgba(255,193,7,0.08)]",
                  isLastTrade && "bg-[rgba(33,150,243,0.08)]"
                )}
              >
                <div className={cn("text-right", level.buyVol >= level.sellVol ? "text-[#4caf50]" : "text-[#f44336]")}>
                  {(level.buyVol - level.sellVol).toFixed(0)}
                </div>
                <div className="relative h-5 overflow-hidden rounded-sm bg-[#121a24]">
                  <div className="absolute inset-y-0 left-0 bg-[#4caf50]/55" style={{ width: `${buyWidth}%` }} />
                  <div
                    className="absolute inset-y-0 bg-[#f44336]/45"
                    style={{ left: `${buyWidth}%`, width: `${sellWidth}%` }}
                  />
                </div>
                <div className="text-right text-text-primary">{level.price.toFixed(2)}</div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
