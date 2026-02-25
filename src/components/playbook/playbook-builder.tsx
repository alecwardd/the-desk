import { useState } from "react";
import type { Setup } from "../../lib/types";
import { setupBridge } from "../../lib/tauri-bridge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface Props {
  onCreated: (setup: Setup) => void;
}

const AVAILABLE_CONDITIONS = [
  { value: "price_vs_vwap=above", label: "Price above VWAP" },
  { value: "price_vs_vwap=below", label: "Price below VWAP" },
  { value: "session_delta=positive", label: "Session delta positive" },
  { value: "session_delta=negative", label: "Session delta negative" },
  { value: "price_near_poc", label: "Price near POC" },
  { value: "price_in_va", label: "Price inside Value Area" },
] as const;

export function PlaybookBuilder({ onCreated }: Props) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [selectedConditions, setSelectedConditions] = useState<string[]>([]);
  const [minDelta, setMinDelta] = useState(0);
  const [duplicateSuppressionMs, setDuplicateSuppressionMs] = useState(2000);

  function toggleCondition(value: string) {
    setSelectedConditions((prev) =>
      prev.includes(value) ? prev.filter((c) => c !== value) : [...prev, value]
    );
  }

  async function handleCreate() {
    if (!name.trim()) return;

    const setup: Setup = {
      id: crypto.randomUUID(),
      name,
      description,
      active: true,
      conditions: selectedConditions,
      minDelta,
      requireAboveVwap: false,
      duplicateSuppressionMs,
    };
    try {
      const created = await setupBridge.create(setup);
      onCreated(created);
    } catch {
      onCreated(setup);
    }
    setName("");
    setDescription("");
    setSelectedConditions([]);
    setMinDelta(0);
    setDuplicateSuppressionMs(2000);
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Playbook Builder</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Setup name
          </label>
          <Input value={name} onChange={(e) => setName(e.target.value)} />
        </div>

        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Description
          </label>
          <Textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
        </div>

        <fieldset className="rounded-md border border-border-subtle p-3">
          <legend className="px-1 text-sm font-medium text-text-secondary">
            Conditions
          </legend>
          <div className="flex flex-col gap-1">
            {AVAILABLE_CONDITIONS.map((cond) => (
              <label
                key={cond.value}
                className="flex items-center gap-2 text-sm text-text-primary"
              >
                <input
                  type="checkbox"
                  checked={selectedConditions.includes(cond.value)}
                  onChange={() => toggleCondition(cond.value)}
                  className="accent-info"
                />
                {cond.label}
              </label>
            ))}
          </div>
        </fieldset>

        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Min absolute delta
          </label>
          <Input
            type="number"
            value={minDelta}
            onChange={(e) => setMinDelta(Number(e.target.value))}
            min={0}
            step={10}
          />
        </div>

        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Alert cooldown (ms)
          </label>
          <Input
            type="number"
            value={duplicateSuppressionMs}
            onChange={(e) =>
              setDuplicateSuppressionMs(Number(e.target.value))
            }
            min={250}
            step={250}
          />
        </div>

        <Button onClick={handleCreate}>Create Setup</Button>
      </CardContent>
    </Card>
  );
}
