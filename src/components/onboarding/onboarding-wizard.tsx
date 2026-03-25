import { useState } from "react";
import { feedBridge, setupBridge } from "../../lib/tauri-bridge";
import type { Setup } from "../../lib/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface Props {
  onComplete: (setups: Setup[]) => void;
  onSkip: () => void;
}

type Step = "connection" | "risk" | "playbook" | "done";

const STEPS: { key: Step; label: string }[] = [
  { key: "connection", label: "1. Connection" },
  { key: "risk", label: "2. Risk" },
  { key: "playbook", label: "3. Playbook" },
  { key: "done", label: "4. Ready" },
];

export function OnboardingWizard({ onComplete, onSkip }: Props) {
  const [step, setStep] = useState<Step>("connection");
  const [connectionStatus, setConnectionStatus] = useState<string | null>(null);
  const [setupName, setSetupName] = useState("");
  const [createdSetups, setCreatedSetups] = useState<Setup[]>([]);

  async function startScidFeed() {
    setConnectionStatus("starting SCID tail...");
    try {
      await feedBridge.startScidFeed();
      setConnectionStatus("SCID feed started — ensure Sierra Chart is writing the .scid file");
    } catch {
      setConnectionStatus("failed — check ~/.the-desk/config.toml sierra_data_dir and .scid path");
    }
  }

  async function addSetup() {
    if (!setupName.trim()) return;
    const setup: Setup = {
      id: crypto.randomUUID(),
      name: setupName,
      description: "",
      active: true,
      conditions: ["price_vs_vwap=above", "session_delta=positive"],
      minDelta: 0,
      requireAboveVwap: true,
      duplicateSuppressionMs: 2000,
    };
    try {
      const created = await setupBridge.create(setup);
      setCreatedSetups((prev) => [...prev, created]);
    } catch {
      setCreatedSetups((prev) => [...prev, setup]);
    }
    setSetupName("");
  }

  function finish() {
    onComplete(createdSetups);
  }

  return (
    <Card className="mx-auto max-w-lg">
      <CardHeader>
        <CardTitle>Welcome to The Desk</CardTitle>
        <div className="flex gap-2 pt-2">
          {STEPS.map((s) => (
            <Badge
              key={s.key}
              variant={s.key === step ? "default" : "outline"}
            >
              {s.label}
            </Badge>
          ))}
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {step === "connection" && (
          <div className="flex flex-col gap-4">
            <h3 className="text-base font-semibold text-text-primary">
              Step 1: Sierra data files
            </h3>
            <p className="text-sm text-text-secondary">
              The Desk reads live ticks from Sierra Chart&apos;s <code className="text-xs">.scid</code> file
              (and optional <code className="text-xs">MarketDepthData/*.depth</code>). Configure{" "}
              <code className="text-xs">sierra_data_dir</code> in{" "}
              <code className="text-xs">~/.the-desk/config.toml</code>, then start the tail.
            </p>

            <div className="flex gap-2">
              <Button onClick={startScidFeed}>Start SCID feed</Button>
            </div>

            {connectionStatus && (
              <p className="text-sm text-text-secondary">{connectionStatus}</p>
            )}

            <div className="flex gap-2 pt-2">
              <Button onClick={() => setStep("risk")}>Next</Button>
              <Button variant="ghost" onClick={onSkip}>
                Skip onboarding
              </Button>
            </div>
          </div>
        )}

        {step === "risk" && (
          <div className="flex flex-col gap-4">
            <h3 className="text-base font-semibold text-text-primary">
              Step 2: Risk Defaults
            </h3>
            <p className="text-sm text-text-secondary">
              Default risk limits: 3R max daily loss, 8 trades per session.
              These can be adjusted in settings later.
            </p>
            <div className="flex gap-2">
              <Button variant="outline" onClick={() => setStep("connection")}>
                Back
              </Button>
              <Button onClick={() => setStep("playbook")}>Next</Button>
            </div>
          </div>
        )}

        {step === "playbook" && (
          <div className="flex flex-col gap-4">
            <h3 className="text-base font-semibold text-text-primary">
              Step 3: Your First Setup
            </h3>
            <p className="text-sm text-text-secondary">
              Create at least one playbook setup. This tells The Desk what
              conditions to watch for.
            </p>

            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Setup name
              </label>
              <Input
                value={setupName}
                onChange={(e) => setSetupName(e.target.value)}
                placeholder="e.g. VWAP Pullback Long"
              />
            </div>

            <Button variant="outline" onClick={addSetup}>
              Add Setup
            </Button>

            {createdSetups.length > 0 && (
              <ul className="list-disc pl-5 text-sm text-text-primary">
                {createdSetups.map((s) => (
                  <li key={s.id}>{s.name}</li>
                ))}
              </ul>
            )}

            <div className="flex gap-2 pt-2">
              <Button variant="outline" onClick={() => setStep("risk")}>
                Back
              </Button>
              <Button onClick={() => setStep("done")}>Finish</Button>
            </div>
          </div>
        )}

        {step === "done" && (
          <div className="flex flex-col gap-4">
            <h3 className="text-base font-semibold text-text-primary">
              Ready to trade
            </h3>
            <p className="text-sm text-text-secondary">
              The Desk will watch your playbook rules and provide coaching
              prompts when conditions are met. Remember: it reflects your rules
              — it never gives trading advice.
            </p>
            <Button onClick={finish}>Start Session</Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
