import { useEffect, useState } from "react";
import { events, subscribe } from "../lib/tauri-bridge";
import type { SetupAlert } from "../lib/types";

export function useSetupAlerts() {
  const [setupAlerts, setSetupAlerts] = useState<SetupAlert[]>([]);

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<SetupAlert>(events.setupAlert, (nextAlert) => {
      setSetupAlerts((existing) => [nextAlert, ...existing].slice(0, 100));
    })
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode.
      });
    return () => cleanup?.();
  }, []);

  return setupAlerts;
}
