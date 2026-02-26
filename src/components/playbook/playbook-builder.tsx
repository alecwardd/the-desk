import { useState } from "react";
import type { Setup, BacktestResults } from "../../lib/types";
import { setupBridge } from "../../lib/tauri-bridge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface Props {
  onCreated: (setup: Setup) => void;
  templates?: Setup[];
}

type WizardStep = "basics" | "conditions" | "stops" | "backtest" | "review";

const STEPS: { key: WizardStep; label: string }[] = [
  { key: "basics", label: "Basics" },
  { key: "conditions", label: "Entry" },
  { key: "stops", label: "Stop & Targets" },
  { key: "backtest", label: "Backtest" },
  { key: "review", label: "Review" },
];

const AVAILABLE_CONDITIONS = [
  { value: "price_vs_vwap=above", label: "Price above VWAP" },
  { value: "price_vs_vwap=below", label: "Price below VWAP" },
  { value: "session_delta=positive", label: "Session delta positive" },
  { value: "session_delta=negative", label: "Session delta negative" },
  { value: "price_near_poc", label: "Price near POC" },
  { value: "price_in_va", label: "Price inside Value Area" },
  { value: "price_in_dnva", label: "Price inside DNVA" },
  { value: "price_vs_dnva_high=above", label: "Price above DNVA High" },
  { value: "price_vs_dnva_low=below", label: "Price below DNVA Low" },
  { value: "price_vs_dnp=above", label: "Price above DNP" },
  { value: "price_vs_dnp=below", label: "Price below DNP" },
  { value: "price_vs_or_high=above", label: "Price above OR High" },
  { value: "price_vs_or_low=below", label: "Price below OR Low" },
  { value: "price_vs_ib_high=above", label: "Price above IB High" },
  { value: "price_vs_ib_low=below", label: "Price below IB Low" },
  { value: "price_vs_prior_high=above", label: "Price above Prior High" },
  { value: "price_vs_prior_low=below", label: "Price below Prior Low" },
  { value: "price_vs_overnight_high=above", label: "Price above ON High" },
  { value: "price_vs_overnight_low=below", label: "Price below ON Low" },
] as const;

const STOP_TYPES = [
  { value: "fixed_points", label: "Fixed Points" },
  { value: "structural", label: "Structural" },
  { value: "atr_based", label: "ATR Based" },
] as const;

const MARKET_CONTEXTS = [
  { value: "any", label: "Any" },
  { value: "trend", label: "Trend" },
  { value: "range", label: "Range" },
] as const;

interface TargetDef {
  points: string;
  description: string;
}

const EMPTY_TARGET: TargetDef = { points: "", description: "" };

function defaultBacktest(): BacktestResults {
  return {
    winRate: undefined,
    avgWinnerR: undefined,
    avgLoserR: undefined,
    profitFactor: undefined,
    samples: undefined,
    maxDrawdownR: undefined,
  };
}

export function PlaybookBuilder({ onCreated, templates }: Props) {
  const [step, setStep] = useState<WizardStep>("basics");

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [marketContext, setMarketContext] = useState("any");

  const [selectedConditions, setSelectedConditions] = useState<string[]>([]);
  const [minDelta, setMinDelta] = useState(0);
  const [discretionary, setDiscretionary] = useState<string[]>([""]);

  const [stopType, setStopType] = useState("fixed_points");
  const [stopValue, setStopValue] = useState("");
  const [targets, setTargets] = useState<TargetDef[]>([{ ...EMPTY_TARGET }]);

  const [backtest, setBacktest] = useState<BacktestResults>(defaultBacktest());

  const [templateId, setTemplateId] = useState<string | undefined>(undefined);
  const [saving, setSaving] = useState(false);

  function loadTemplate(id: string) {
    const tpl = templates?.find((t) => t.id === id);
    if (!tpl) return;
    setTemplateId(id);
    setName(tpl.name + " (copy)");
    setDescription(tpl.description);
    setSelectedConditions([...tpl.conditions]);
    setMinDelta(tpl.minDelta ?? 0);
    setDiscretionary(
      tpl.discretionaryConditions?.length ? [...tpl.discretionaryConditions] : [""]
    );
    if (tpl.marketContext) {
      setMarketContext((tpl.marketContext as { type?: string }).type ?? "any");
    }
    if (tpl.stopLogic) {
      const sl = tpl.stopLogic as { type?: string; value?: number };
      setStopType(sl.type ?? "fixed_points");
      setStopValue(sl.value?.toString() ?? "");
    }
    if (tpl.targets?.length) {
      setTargets(
        tpl.targets.map((t) => ({
          points: ((t as { points?: number }).points ?? "").toString(),
          description: ((t as { description?: string }).description ?? ""),
        }))
      );
    }
    if (tpl.backtestResults) {
      setBacktest({ ...tpl.backtestResults });
    }
  }

  function toggleCondition(value: string) {
    setSelectedConditions((prev) =>
      prev.includes(value) ? prev.filter((c) => c !== value) : [...prev, value]
    );
  }

  function updateTarget(idx: number, field: keyof TargetDef, val: string) {
    setTargets((prev) =>
      prev.map((t, i) => (i === idx ? { ...t, [field]: val } : t))
    );
  }

  function addTarget() {
    if (targets.length < 3) setTargets((prev) => [...prev, { ...EMPTY_TARGET }]);
  }

  function removeTarget(idx: number) {
    setTargets((prev) => prev.filter((_, i) => i !== idx));
  }

  function updateDiscretionary(idx: number, val: string) {
    setDiscretionary((prev) => prev.map((d, i) => (i === idx ? val : d)));
  }

  function addDiscretionary() {
    setDiscretionary((prev) => [...prev, ""]);
  }

  function removeDiscretionary(idx: number) {
    setDiscretionary((prev) => prev.filter((_, i) => i !== idx));
  }

  function updateBacktest(field: keyof BacktestResults, val: string) {
    const num = val === "" ? undefined : Number(val);
    setBacktest((prev) => ({ ...prev, [field]: num }));
  }

  const stepIdx = STEPS.findIndex((s) => s.key === step);

  function goNext() {
    if (stepIdx < STEPS.length - 1) setStep(STEPS[stepIdx + 1].key);
  }

  function goBack() {
    if (stepIdx > 0) setStep(STEPS[stepIdx - 1].key);
  }

  function canProceedFromBasics() {
    return name.trim().length > 0;
  }

  async function handleCreate() {
    if (!name.trim()) return;
    setSaving(true);

    const filteredDiscretionary = discretionary.filter((d) => d.trim());
    const filteredTargets = targets
      .filter((t) => t.points || t.description)
      .map((t) => ({
        points: t.points ? Number(t.points) : undefined,
        description: t.description,
      }));

    const hasBacktest = backtest.winRate !== undefined || backtest.samples !== undefined;

    const setup: Setup = {
      id: crypto.randomUUID(),
      name: name.trim(),
      description,
      active: true,
      conditions: selectedConditions,
      minDelta: minDelta || undefined,
      marketContext: { type: marketContext },
      stopLogic: { type: stopType, value: stopValue ? Number(stopValue) : undefined },
      targets: filteredTargets.length > 0 ? filteredTargets : undefined,
      backtestResults: hasBacktest ? backtest : undefined,
      discretionaryConditions: filteredDiscretionary.length > 0 ? filteredDiscretionary : undefined,
      templateSource: templateId ?? null,
    };

    try {
      const created = await setupBridge.create(setup);
      onCreated(created);
    } catch {
      onCreated(setup);
    }

    resetForm();
  }

  function resetForm() {
    setStep("basics");
    setName("");
    setDescription("");
    setMarketContext("any");
    setSelectedConditions([]);
    setMinDelta(0);
    setDiscretionary([""]);
    setStopType("fixed_points");
    setStopValue("");
    setTargets([{ ...EMPTY_TARGET }]);
    setBacktest(defaultBacktest());
    setTemplateId(undefined);
    setSaving(false);
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Playbook Builder</CardTitle>
        <div className="flex gap-1.5 pt-2">
          {STEPS.map((s, i) => (
            <Badge
              key={s.key}
              variant={s.key === step ? "default" : i < stepIdx ? "secondary" : "outline"}
              className="cursor-pointer"
              onClick={() => {
                if (s.key === "basics" || canProceedFromBasics()) setStep(s.key);
              }}
            >
              {i + 1}. {s.label}
            </Badge>
          ))}
        </div>
      </CardHeader>

      <CardContent className="flex flex-col gap-4">
        {/* Template selector */}
        {templates && templates.length > 0 && step === "basics" && (
          <div className="flex flex-col gap-1.5">
            <label className="text-sm font-medium text-text-secondary">
              Start from Template
            </label>
            <Select value={templateId} onValueChange={loadTemplate}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Choose a template..." />
              </SelectTrigger>
              <SelectContent>
                {templates.map((t) => (
                  <SelectItem key={t.id} value={t.id}>
                    {t.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        {/* Step 1: Basics */}
        {step === "basics" && (
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Setup name <span className="text-destructive">*</span>
              </label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. VWAP Pullback Long"
              />
            </div>

            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Description
              </label>
              <Textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Describe when and how this setup triggers..."
                rows={3}
              />
            </div>

            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Market context
              </label>
              <Select value={marketContext} onValueChange={setMarketContext}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {MARKET_CONTEXTS.map((mc) => (
                    <SelectItem key={mc.value} value={mc.value}>
                      {mc.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {/* Step 2: Entry Conditions */}
        {step === "conditions" && (
          <div className="flex flex-col gap-4">
            <fieldset className="rounded-md border border-border-subtle p-3">
              <legend className="px-1 text-sm font-medium text-text-secondary">
                Market Conditions
              </legend>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1">
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

            <Separator />

            <fieldset className="rounded-md border border-border-subtle p-3">
              <legend className="px-1 text-sm font-medium text-text-secondary">
                Discretionary Conditions
              </legend>
              <p className="mb-2 text-xs text-muted-foreground">
                Free-text conditions evaluated by the coaching layer.
              </p>
              <div className="flex flex-col gap-2">
                {discretionary.map((d, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <Input
                      value={d}
                      onChange={(e) => updateDiscretionary(i, e.target.value)}
                      placeholder="e.g. Trend day structure confirmed"
                    />
                    {discretionary.length > 1 && (
                      <Button
                        variant="ghost"
                        size="xs"
                        onClick={() => removeDiscretionary(i)}
                      >
                        Remove
                      </Button>
                    )}
                  </div>
                ))}
                <Button variant="outline" size="sm" onClick={addDiscretionary}>
                  + Add condition
                </Button>
              </div>
            </fieldset>
          </div>
        )}

        {/* Step 3: Stop & Targets */}
        {step === "stops" && (
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Stop type
              </label>
              <Select value={stopType} onValueChange={setStopType}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STOP_TYPES.map((st) => (
                    <SelectItem key={st.value} value={st.value}>
                      {st.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium text-text-secondary">
                Stop value (points)
              </label>
              <Input
                type="number"
                value={stopValue}
                onChange={(e) => setStopValue(e.target.value)}
                placeholder="e.g. 8"
                min={0}
                step={0.25}
              />
            </div>

            <Separator />

            <fieldset className="rounded-md border border-border-subtle p-3">
              <legend className="px-1 text-sm font-medium text-text-secondary">
                Targets (up to 3)
              </legend>
              <div className="flex flex-col gap-3">
                {targets.map((t, i) => (
                  <div key={i} className="flex flex-col gap-1.5 rounded border border-border-subtle p-2">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-medium text-text-secondary">
                        Target {i + 1}
                      </span>
                      {targets.length > 1 && (
                        <Button variant="ghost" size="xs" onClick={() => removeTarget(i)}>
                          Remove
                        </Button>
                      )}
                    </div>
                    <Input
                      type="number"
                      value={t.points}
                      onChange={(e) => updateTarget(i, "points", e.target.value)}
                      placeholder="Points (e.g. 12)"
                      min={0}
                      step={0.25}
                    />
                    <Input
                      value={t.description}
                      onChange={(e) => updateTarget(i, "description", e.target.value)}
                      placeholder="Description (e.g. Prior day high)"
                    />
                  </div>
                ))}
                {targets.length < 3 && (
                  <Button variant="outline" size="sm" onClick={addTarget}>
                    + Add target
                  </Button>
                )}
              </div>
            </fieldset>
          </div>
        )}

        {/* Step 4: Backtest Results */}
        {step === "backtest" && (
          <div className="flex flex-col gap-4">
            <p className="text-sm text-text-secondary">
              Optional. Enter backtest results from your external testing tool.
            </p>

            <div className="grid grid-cols-2 gap-4">
              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Win rate %
                </label>
                <Input
                  type="number"
                  value={backtest.winRate ?? ""}
                  onChange={(e) => updateBacktest("winRate", e.target.value)}
                  placeholder="e.g. 55"
                  min={0}
                  max={100}
                  step={0.1}
                />
              </div>

              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Avg winner (R)
                </label>
                <Input
                  type="number"
                  value={backtest.avgWinnerR ?? ""}
                  onChange={(e) => updateBacktest("avgWinnerR", e.target.value)}
                  placeholder="e.g. 1.8"
                  min={0}
                  step={0.1}
                />
              </div>

              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Avg loser (R)
                </label>
                <Input
                  type="number"
                  value={backtest.avgLoserR ?? ""}
                  onChange={(e) => updateBacktest("avgLoserR", e.target.value)}
                  placeholder="e.g. -1.0"
                  step={0.1}
                />
              </div>

              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Profit factor
                </label>
                <Input
                  type="number"
                  value={backtest.profitFactor ?? ""}
                  onChange={(e) => updateBacktest("profitFactor", e.target.value)}
                  placeholder="e.g. 1.6"
                  min={0}
                  step={0.1}
                />
              </div>

              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Sample size
                </label>
                <Input
                  type="number"
                  value={backtest.samples ?? ""}
                  onChange={(e) => updateBacktest("samples", e.target.value)}
                  placeholder="e.g. 50"
                  min={0}
                  step={1}
                />
              </div>

              <div className="flex flex-col gap-1.5">
                <label className="text-sm font-medium text-text-secondary">
                  Max drawdown (R)
                </label>
                <Input
                  type="number"
                  value={backtest.maxDrawdownR ?? ""}
                  onChange={(e) => updateBacktest("maxDrawdownR", e.target.value)}
                  placeholder="e.g. 4.5"
                  min={0}
                  step={0.1}
                />
              </div>
            </div>
          </div>
        )}

        {/* Step 5: Review & Save */}
        {step === "review" && (
          <div className="flex flex-col gap-3">
            <div>
              <span className="text-xs font-medium text-text-secondary">Name</span>
              <p className="text-sm text-text-primary">{name || "—"}</p>
            </div>
            <div>
              <span className="text-xs font-medium text-text-secondary">Description</span>
              <p className="text-sm text-text-primary">{description || "—"}</p>
            </div>
            <div>
              <span className="text-xs font-medium text-text-secondary">Market context</span>
              <p className="text-sm text-text-primary capitalize">{marketContext}</p>
            </div>

            <Separator />

            <div>
              <span className="text-xs font-medium text-text-secondary">Conditions</span>
              {selectedConditions.length > 0 ? (
                <div className="mt-1 flex flex-wrap gap-1">
                  {selectedConditions.map((c) => {
                    const label = AVAILABLE_CONDITIONS.find((ac) => ac.value === c)?.label ?? c;
                    return (
                      <Badge key={c} variant="secondary">
                        {label}
                      </Badge>
                    );
                  })}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">None selected</p>
              )}
            </div>

            {minDelta > 0 && (
              <div>
                <span className="text-xs font-medium text-text-secondary">Min delta</span>
                <p className="text-sm text-text-primary">{minDelta}</p>
              </div>
            )}

            {discretionary.filter((d) => d.trim()).length > 0 && (
              <div>
                <span className="text-xs font-medium text-text-secondary">Discretionary</span>
                <ul className="mt-1 list-disc pl-4 text-sm text-text-primary">
                  {discretionary.filter((d) => d.trim()).map((d, i) => (
                    <li key={i}>{d}</li>
                  ))}
                </ul>
              </div>
            )}

            <Separator />

            <div>
              <span className="text-xs font-medium text-text-secondary">Stop</span>
              <p className="text-sm text-text-primary">
                {STOP_TYPES.find((s) => s.value === stopType)?.label ?? stopType}
                {stopValue && ` — ${stopValue} pts`}
              </p>
            </div>

            {targets.filter((t) => t.points || t.description).length > 0 && (
              <div>
                <span className="text-xs font-medium text-text-secondary">Targets</span>
                <ul className="mt-1 list-disc pl-4 text-sm text-text-primary">
                  {targets
                    .filter((t) => t.points || t.description)
                    .map((t, i) => (
                      <li key={i}>
                        {t.points ? `${t.points} pts` : ""}
                        {t.points && t.description ? " — " : ""}
                        {t.description}
                      </li>
                    ))}
                </ul>
              </div>
            )}

            {(backtest.winRate !== undefined || backtest.samples !== undefined) && (
              <>
                <Separator />
                <div>
                  <span className="text-xs font-medium text-text-secondary">Backtest</span>
                  <div className="mt-1 grid grid-cols-3 gap-2 text-sm text-text-primary">
                    {backtest.winRate !== undefined && (
                      <span>WR: {backtest.winRate}%</span>
                    )}
                    {backtest.avgWinnerR !== undefined && (
                      <span>Avg W: {backtest.avgWinnerR}R</span>
                    )}
                    {backtest.avgLoserR !== undefined && (
                      <span>Avg L: {backtest.avgLoserR}R</span>
                    )}
                    {backtest.profitFactor !== undefined && (
                      <span>PF: {backtest.profitFactor}</span>
                    )}
                    {backtest.samples !== undefined && (
                      <span>N: {backtest.samples}</span>
                    )}
                    {backtest.maxDrawdownR !== undefined && (
                      <span>MDD: {backtest.maxDrawdownR}R</span>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        )}
      </CardContent>

      <CardFooter className="flex justify-between gap-2">
        <div>
          {stepIdx > 0 && (
            <Button variant="outline" onClick={goBack}>
              Back
            </Button>
          )}
        </div>
        <div className="flex gap-2">
          {step === "review" ? (
            <Button onClick={handleCreate} disabled={!canProceedFromBasics() || saving}>
              {saving ? "Creating..." : "Create Setup"}
            </Button>
          ) : (
            <Button
              onClick={goNext}
              disabled={step === "basics" && !canProceedFromBasics()}
            >
              Next
            </Button>
          )}
        </div>
      </CardFooter>
    </Card>
  );
}
