import { useEffect, useState } from "react";
import { events, subscribe } from "../lib/tauri-bridge";

export function useConnection() {
  const [status, setStatus] = useState("disconnected");

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<string>(events.dtcStatus, setStatus)
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
