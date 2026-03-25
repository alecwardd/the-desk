import { useEffect, useState } from "react";
import { events, feedBridge, subscribe } from "../lib/tauri-bridge";

export function useConnection() {
  const [status, setStatus] = useState("disconnected");

  useEffect(() => {
    feedBridge.status().then(setStatus).catch(() => {});
    let cleanup: (() => void) | undefined;
    subscribe<string>(events.feedStatus, setStatus)
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode.
      });
    return () => cleanup?.();
  }, []);

  return status;
}
