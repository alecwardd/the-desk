import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { CoachingPrompt } from "../../lib/types";

interface Props {
  prompts: CoachingPrompt[];
  onRespond: (prompt: CoachingPrompt, response: "took_it" | "watching" | "passed") => void;
}

const priorityStyles: Record<CoachingPrompt["priority"], string> = {
  alert: "border-l-4 border-l-positive bg-prompt-alert",
  warning: "border-l-4 border-l-warning bg-prompt-warning",
  critical: "border-l-4 border-l-critical bg-prompt-critical",
  info: "border-l-4 border-l-info bg-prompt-info",
};

export function CoachingFeed({ prompts, onRespond }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Coaching Feed</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {prompts.length === 0 ? (
          <p className="text-text-muted text-sm">No prompts yet. The Desk is watching.</p>
        ) : (
          prompts.map((prompt) => (
            <article
              key={prompt.id}
              className={`rounded-md p-3 ${priorityStyles[prompt.priority]}`}
            >
              <strong className="text-text-primary text-sm">{prompt.setupName}</strong>
              <p className="text-text-secondary mt-1 text-sm">{prompt.message}</p>
              <div className="mt-2 flex gap-2">
                <Button variant="outline" size="sm" onClick={() => onRespond(prompt, "took_it")}>
                  Took it (1)
                </Button>
                <Button variant="outline" size="sm" onClick={() => onRespond(prompt, "watching")}>
                  Watching (2)
                </Button>
                <Button variant="outline" size="sm" onClick={() => onRespond(prompt, "passed")}>
                  Passed (3)
                </Button>
              </div>
            </article>
          ))
        )}
      </CardContent>
    </Card>
  );
}
