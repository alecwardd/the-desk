import { useEffect, useState } from "react";
import { DomLadder } from "./dom-ladder";
import { TapeStrip } from "./tape-strip";
import { VolumeProfile } from "./volume-profile";
import { useDomReplay } from "@/hooks/use-dom-replay";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

const SPEEDS = [0.1, 0.25, 0.5, 1, 2, 5, 10];

function toLocalInputValue(timestampMs: number) {
  const date = new Date(timestampMs);
  const offset = date.getTimezoneOffset() * 60_000;
  return new Date(timestampMs - offset).toISOString().slice(0, 16);
}

function fromLocalInputValue(value: string) {
  return new Date(value).getTime();
}

function formatPlaybackTime(timestampMs: number | null | undefined) {
  if (!timestampMs) return "--:--:--.---";
  return new Date(timestampMs).toLocaleTimeString([], {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

export function DomVisualizer() {
  const now = Date.now();
  const { currentFrame, status, loadResult, error, load, play, pause, stop, seek } = useDomReplay();
  const [startValue, setStartValue] = useState(toLocalInputValue(now - 30 * 60 * 1000));
  const [endValue, setEndValue] = useState(toLocalInputValue(now));
  const [speed, setSpeed] = useState(1);
  const [isSeeking, setIsSeeking] = useState(false);

  useEffect(() => {
    if (status?.speed && !status.isPlaying) {
      setSpeed(status.speed);
    }
  }, [status?.speed, status?.isPlaying]);

  const rangeStart = status?.startMs ?? fromLocalInputValue(startValue);
  const rangeEnd = status?.endMs ?? fromLocalInputValue(endValue);
  const currentTs = currentFrame?.timestampMs ?? status?.currentTimestampMs ?? rangeStart;
  const progress = rangeEnd > rangeStart ? ((currentTs - rangeStart) / (rangeEnd - rangeStart)) * 100 : 0;

  async function handleLoad() {
    await load(fromLocalInputValue(startValue), fromLocalInputValue(endValue), 12);
  }

  return (
    <div className="flex flex-col gap-3 p-3 min-h-0">
      <Card className="border-border bg-card/80">
        <CardHeader className="pb-0">
          <CardTitle>Historical DOM Replay</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 pt-4">
          <div className="grid grid-cols-[1fr_1fr_auto] gap-3">
            <div className="flex flex-col gap-1">
              <label className="text-xs uppercase tracking-[0.18em] text-text-secondary">Start</label>
              <Input type="datetime-local" value={startValue} onChange={(event) => setStartValue(event.target.value)} />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs uppercase tracking-[0.18em] text-text-secondary">End</label>
              <Input type="datetime-local" value={endValue} onChange={(event) => setEndValue(event.target.value)} />
            </div>
            <div className="flex items-end">
              <Button onClick={() => void handleLoad()}>Load Clip</Button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button onClick={() => void play(speed)} disabled={!status?.isLoaded || status.isPlaying}>
              Play
            </Button>
            <Button variant="secondary" onClick={() => void pause()} disabled={!status?.isPlaying}>
              Pause
            </Button>
            <Button variant="outline" onClick={() => void stop()} disabled={!status?.isLoaded}>
              Stop
            </Button>
            <div className="ml-2 flex flex-wrap gap-1">
              {SPEEDS.map((value) => (
                <Button
                  key={value}
                  variant={speed === value ? "default" : "ghost"}
                  size="sm"
                  onClick={() => setSpeed(value)}
                >
                  {value}x
                </Button>
              ))}
            </div>
          </div>

          <div className="space-y-1">
            <div className="flex items-center justify-between text-xs text-text-secondary">
              <span>
                {status?.cursor ?? 0}/{status?.totalEvents ?? 0} events
              </span>
              <span>{formatPlaybackTime(currentTs)}</span>
            </div>
            <input
              aria-label="DOM replay scrubber"
              className="w-full accent-[#2196f3]"
              type="range"
              min={0}
              max={1000}
              value={Math.max(0, Math.min(1000, Math.round(progress * 10)))}
              onMouseDown={() => setIsSeeking(true)}
              onMouseUp={(event) => {
                const ratio = Number((event.target as HTMLInputElement).value) / 1000;
                const ts = rangeStart + (rangeEnd - rangeStart) * ratio;
                setIsSeeking(false);
                void seek(ts);
              }}
              onChange={(event) => {
                if (!isSeeking) {
                  const ratio = Number(event.target.value) / 1000;
                  const ts = rangeStart + (rangeEnd - rangeStart) * ratio;
                  void seek(ts);
                }
              }}
            />
          </div>

          <div className="flex flex-wrap gap-4 text-sm text-text-secondary">
            <span>{loadResult ? `Sources: ${loadResult.sourceSummary}` : "Load a clip to inspect DOM history"}</span>
            <span>{currentFrame?.warning ?? status?.warning ?? "Reconstructed from historical depth and tape data"}</span>
            {error && <span className="text-[#ff8a80]">{error}</span>}
          </div>
        </CardContent>
      </Card>

      <div className="grid min-h-0 flex-1 grid-cols-[260px_minmax(420px,1fr)_220px] gap-3">
        <VolumeProfile levels={currentFrame?.volumeProfile ?? []} lastTrade={currentFrame?.lastTrade ?? null} />
        <DomLadder frame={currentFrame} />
        <TapeStrip prints={currentFrame?.recentTape ?? []} />
      </div>
    </div>
  );
}
