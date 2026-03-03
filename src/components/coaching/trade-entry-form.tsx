import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { RiskState } from "../../lib/types";

interface Props {
  defaultDirection?: "long" | "short";
  defaultPrice?: number;
  riskState?: RiskState | null;
  onSubmit: (direction: "long" | "short", size: number, entryPrice: number) => void;
  onCancel: () => void;
}

export function TradeEntryForm({
  defaultDirection = "long",
  defaultPrice,
  riskState,
  onSubmit,
  onCancel,
}: Props) {
  const [direction, setDirection] = useState<"long" | "short">(defaultDirection);
  const [size, setSize] = useState(1);
  const [entryPrice, setEntryPrice] = useState(defaultPrice ?? 0);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (entryPrice <= 0) return;
    if (riskState?.atLimit) return;
    onSubmit(direction, size, entryPrice);
  }

  const atLimit = riskState?.atLimit === true;
  const remainingR = riskState
    ? riskState.maxDailyLossR + riskState.dailyPnlR
    : null;

  return (
    <form
      onSubmit={handleSubmit}
      className="mt-2 flex flex-wrap items-end gap-2 rounded border border-border-subtle bg-surface p-2"
    >
      <div className="flex flex-col gap-1">
        <span className="text-text-muted text-xs">Direction</span>
        <Select
          value={direction}
          onValueChange={(v) => setDirection(v as "long" | "short")}
        >
          <SelectTrigger size="sm" className="w-24">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="long">Long</SelectItem>
            <SelectItem value="short">Short</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-1">
        <span className="text-text-muted text-xs">Size</span>
        <Input
          type="number"
          min={1}
          step={1}
          value={size}
          onChange={(e) => setSize(Math.max(1, Number(e.target.value)))}
          className="h-8 w-20"
        />
      </div>

      <div className="flex flex-col gap-1">
        <span className="text-text-muted text-xs">Entry Price</span>
        <Input
          type="number"
          min={0}
          step={0.25}
          value={entryPrice || ""}
          onChange={(e) => setEntryPrice(Number(e.target.value))}
          placeholder="0.00"
          className="h-8 w-28"
        />
      </div>

      {atLimit && (
        <p className="text-warning text-xs">Daily limit reached. Do not log new trades.</p>
      )}
      {remainingR != null && remainingR < 0 && !atLimit && (
        <p className="text-text-muted text-xs">Remaining: {remainingR.toFixed(1)}R</p>
      )}
      <div className="flex gap-1.5">
        <Button type="submit" size="sm" disabled={entryPrice <= 0 || atLimit}>
          Log Trade
        </Button>
        <Button type="button" variant="ghost" size="sm" onClick={onCancel}>
          Cancel
        </Button>
      </div>
    </form>
  );
}
