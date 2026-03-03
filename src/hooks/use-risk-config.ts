import { useEffect, useState } from "react";
import { riskBridge } from "../lib/tauri-bridge";
import type { RiskConfigRecord } from "../lib/types";

export function useRiskConfig() {
  const [riskConfig, setRiskConfig] = useState<RiskConfigRecord | null>(null);

  useEffect(() => {
    riskBridge
      .getConfig()
      .then(setRiskConfig)
      .catch(() => {
        // Non-Tauri web mode
      });
  }, []);

  return riskConfig;
}
