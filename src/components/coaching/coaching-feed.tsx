import { useCallback, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import type { CoachingPrompt, RiskState } from "../../lib/types";
import { TradeEntryForm } from "./trade-entry-form";

type ResponseType = "took_it" | "watching" | "passed";

interface ResponseState {
  type: ResponseType;
  passNote?: string;
}

interface Props {
  prompts: CoachingPrompt[];
  riskState?: RiskState | null;
  onRespond: (prompt: CoachingPrompt, response: ResponseType) => void;
  onTookIt: (
    prompt: CoachingPrompt,
    direction: "long" | "short",
    size: number,
    entryPrice: number
  ) => void;
}

const priorityStyles: Record<CoachingPrompt["priority"], string> = {
  alert: "border-l-4 border-l-positive bg-prompt-alert",
  warning: "border-l-4 border-l-warning bg-prompt-warning",
  critical: "border-l-4 border-l-critical bg-prompt-critical",
  info: "border-l-4 border-l-info bg-prompt-info",
  risk_warning: "border-l-4 border-l-warning bg-prompt-risk-warning",
};

function promptCardClass(response: ResponseState | undefined): string {
  if (!response) return "";
  if (response.type === "watching") return "ring-1 ring-info/40 animate-pulse-border";
  return "opacity-60";
}

export function CoachingFeed({ prompts, riskState, onRespond, onTookIt }: Props) {
  const [responses, setResponses] = useState<Record<string, ResponseState>>({});
  const [expandedTrade, setExpandedTrade] = useState<string | null>(null);
  const [expandedPass, setExpandedPass] = useState<string | null>(null);

  const handleRespond = useCallback(
    (prompt: CoachingPrompt, type: ResponseType) => {
      if (type === "took_it") {
        setExpandedTrade(prompt.id);
        setExpandedPass(null);
        return;
      }
      if (type === "passed") {
        setExpandedPass(prompt.id);
        setExpandedTrade(null);
        setResponses((prev) => ({ ...prev, [prompt.id]: { type: "passed" } }));
        onRespond(prompt, "passed");
        return;
      }
      setExpandedTrade(null);
      setExpandedPass(null);
      setResponses((prev) => ({ ...prev, [prompt.id]: { type } }));
      onRespond(prompt, type);
    },
    [onRespond]
  );

  const handleTradeSubmit = useCallback(
    (prompt: CoachingPrompt, direction: "long" | "short", size: number, entryPrice: number) => {
      setResponses((prev) => ({ ...prev, [prompt.id]: { type: "took_it" } }));
      setExpandedTrade(null);
      onTookIt(prompt, direction, size, entryPrice);
      onRespond(prompt, "took_it");
    },
    [onRespond, onTookIt]
  );

  const handlePassNote = useCallback(
    (promptId: string, note: string) => {
      setResponses((prev) => ({
        ...prev,
        [promptId]: { type: "passed", passNote: note },
      }));
    },
    []
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Coaching Feed</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {prompts.length === 0 ? (
          <p className="text-text-muted text-sm">No prompts yet. The Desk is watching.</p>
        ) : (
          prompts.map((prompt) => {
            const response = responses[prompt.id];
            const isTradeExpanded = expandedTrade === prompt.id;
            const isPassExpanded = expandedPass === prompt.id;

            return (
              <article
                key={prompt.id}
                className={`rounded-md p-3 transition-all ${priorityStyles[prompt.priority]} ${promptCardClass(response)}`}
              >
                <strong className="text-text-primary text-sm">{prompt.setupName}</strong>
                <p className="text-text-secondary mt-1 text-sm">{prompt.message}</p>

                {!response && (
                  <div className="mt-2 flex flex-col gap-2">
                    {riskState?.atLimit && (
                      <p className="text-warning text-xs">
                        Your daily loss limit has been reached. Your rules say to stop trading for the day.
                      </p>
                    )}
                    {riskState && !riskState.atLimit && riskState.dailyPnlR <= -0.8 * riskState.maxDailyLossR && (
                      <p className="text-warning text-xs">
                        Approaching limit: {riskState.dailyPnlR.toFixed(1)}R of -{riskState.maxDailyLossR}R. Proceed with caution.
                      </p>
                    )}
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleRespond(prompt, "took_it")}
                        disabled={riskState?.atLimit === true}
                      >
                        Took it (1)
                      </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleRespond(prompt, "watching")}
                    >
                      Watching (2)
                    </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleRespond(prompt, "passed")}
                      >
                        Passed (3)
                      </Button>
                    </div>
                  </div>
                )}

                {response && (
                  <div className="mt-1.5">
                    <span className="text-text-muted text-xs uppercase tracking-wide">
                      {response.type.replace("_", " ")}
                    </span>
                  </div>
                )}

                {isTradeExpanded && (
                  <TradeEntryForm
                    riskState={riskState}
                    onSubmit={(dir, sz, px) => handleTradeSubmit(prompt, dir, sz, px)}
                    onCancel={() => setExpandedTrade(null)}
                  />
                )}

                {isPassExpanded && !response?.passNote && (
                  <div className="mt-2 flex items-center gap-2">
                    <Input
                      placeholder="Why did you pass? (optional)"
                      className="h-8 text-sm"
                      onKeyDown={(e) => {
                        if (e.key === "Enter") {
                          handlePassNote(prompt.id, e.currentTarget.value);
                          setExpandedPass(null);
                        }
                        if (e.key === "Escape") setExpandedPass(null);
                      }}
                      autoFocus
                    />
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setExpandedPass(null)}
                    >
                      Skip
                    </Button>
                  </div>
                )}

                {response?.passNote && (
                  <p className="text-text-muted mt-1 text-xs italic">
                    Note: {response.passNote}
                  </p>
                )}
              </article>
            );
          })
        )}
      </CardContent>
    </Card>
  );
}
