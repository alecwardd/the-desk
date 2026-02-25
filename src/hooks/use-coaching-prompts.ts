import { useEffect, useState } from "react";
import { events, subscribe } from "../lib/tauri-bridge";
import type { CoachingPrompt } from "../lib/types";

export function useCoachingPrompts() {
  const [prompts, setPrompts] = useState<CoachingPrompt[]>([]);

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<CoachingPrompt>(events.coachingPrompt, (prompt) => {
      setPrompts((existing) => [prompt, ...existing].slice(0, 200));
    })
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode.
      });
    return () => cleanup?.();
  }, []);

  return prompts;
}
