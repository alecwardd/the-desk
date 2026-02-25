import { useEffect, useState } from "react";
import { events, subscribe } from "../lib/tauri-bridge";
import type { RiskState } from "../lib/types";

export function useRiskState() {
  const [riskState, setRiskState] = useState<RiskState | null>(null);

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<RiskState>(events.riskState, setRiskState)
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode.
      });
    return () => cleanup?.();
  }, []);

  return riskState;
}
