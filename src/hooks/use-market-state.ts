import { useEffect, useState } from "react";
import { events, subscribe } from "../lib/tauri-bridge";
import type { MarketState } from "../lib/types";

export function useMarketState() {
  const [marketState, setMarketState] = useState<MarketState | null>(null);

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<MarketState>(events.marketState, setMarketState)
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode: no event channel available.
      });
    return () => cleanup?.();
  }, []);

  return marketState;
}
