import { useEffect, useMemo, useRef, useState } from "react";
import { PreSessionBriefing } from "./components/briefing/pre-session-briefing";
import { CoachingFeed } from "./components/coaching/coaching-feed";
import { MarketSidebar } from "./components/dashboard/market-sidebar";
import { OnboardingWizard } from "./components/onboarding/onboarding-wizard";
import { PlaybookBuilder } from "./components/playbook/playbook-builder";
import { SetupList } from "./components/playbook/setup-list";
import { ReplayControls } from "./components/replay/replay-controls";
import { SessionReview } from "./components/review/session-review";
import { RiskBar } from "./components/risk/risk-bar";
import { useCoachingPrompts } from "./hooks/use-coaching-prompts";
import { useConnection } from "./hooks/use-connection";
import { useMarketState } from "./hooks/use-market-state";
import { useRiskConfig } from "./hooks/use-risk-config";
import { useRiskState } from "./hooks/use-risk-state";
import { useSetupAlerts } from "./hooks/use-setup-alerts";
import { generateCoachingPrompt, generateRiskWarning } from "./lib/claude";
import { sessionBridge, setupBridge, tradeBridge } from "./lib/tauri-bridge";
import type { CoachingPrompt, SessionEventRecord, Setup, TradeRecord } from "./lib/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";

type View = "dashboard" | "briefing" | "review" | "settings" | "playbook";

export default function App() {
  const connection = useConnection();
  const marketState = useMarketState();
  const riskState = useRiskState();
  const riskConfig = useRiskConfig();
  const setupAlerts = useSetupAlerts();
  const streamPrompts = useCoachingPrompts();
  const [localPrompts, setLocalPrompts] = useState<CoachingPrompt[]>([]);
  const [setups, setSetups] = useState<Setup[]>([]);
  const [templates, setTemplates] = useState<Setup[]>([]);
  const [showOnboarding, setShowOnboarding] = useState(true);
  const [quickNote, setQuickNote] = useState("");
  const [view, setView] = useState<View>("dashboard");
  const [sessionEvents, setSessionEvents] = useState<SessionEventRecord[]>([]);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [preSessionNote, setPreSessionNote] = useState("");
  const [appStatus, setAppStatus] = useState<string | null>(null);
  const [editingSetup, setEditingSetup] = useState<Setup | null>(null);

  const prompts = useMemo(
    () => [...localPrompts, ...streamPrompts],
    [localPrompts, streamPrompts]
  );

  const hasActiveSetups = setups.some((s) => s.active);

  useEffect(() => {
    refreshSetups();
    setupBridge
      .listTemplates()
      .then(setTemplates)
      .catch(() => {});
    sessionBridge
      .listEvents(300)
      .then(setSessionEvents)
      .catch(() => {});
  }, []);

  function refreshSetups() {
    setupBridge
      .list()
      .then(setSetups)
      .catch(() => setAppStatus("Setups unavailable; working in local-only mode"));
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      const target = event.target as HTMLElement;
      if (target.tagName === "INPUT" || target.tagName === "TEXTAREA") return;

      if (event.key === "n" || event.key === "N") {
        event.preventDefault();
        const noteInput = document.getElementById("quick-note") as HTMLInputElement | null;
        noteInput?.focus();
      }
      if (event.key === "1" || event.key === "2" || event.key === "3") {
        const latest = prompts[0];
        if (!latest) return;
        if (event.key === "1") void handlePromptResponse(latest, "took_it");
        else if (event.key === "2") void handlePromptResponse(latest, "watching");
        else void handlePromptResponse(latest, "passed");
      }
      if (event.ctrlKey && event.key.toLowerCase() === "e") {
        event.preventDefault();
        if (window.confirm("End session now?")) {
          void sessionBridge.stop();
          setView("review");
        }
      }
      if (event.key === "?") {
        event.preventDefault();
        setShowShortcuts((prev) => !prev);
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [prompts]);

  const [showShortcuts, setShowShortcuts] = useState(false);

  useEffect(() => {
    const latestAlert = setupAlerts[0];
    if (!latestAlert || latestAlert.stateTransition !== "conditionsMet") return;

    const matchingSetup = setups.find((setup) => setup.id === latestAlert.setupId) ?? null;
    const noteList = quickNote ? [quickNote] : [];
    generateCoachingPrompt({
      alert: latestAlert,
      setup: matchingSetup,
      risk: riskState,
      notes: noteList,
    })
      .then((prompt) => {
        setLocalPrompts((existing) => [prompt, ...existing].slice(0, 200));
      })
      .catch(() => {});
  }, [setups, setupAlerts, riskState, quickNote]);

  const lastRiskWarningKey = useRef<string | null>(null);
  useEffect(() => {
    if (!riskState) return;
    let key: string | null = null;
    if (riskState.atLimit) key = "at_limit";
    else if (riskState.dailyPnlR <= -0.8 * riskState.maxDailyLossR && riskState.maxDailyLossR > 0)
      key = "near_limit";
    else if (riskState.consecutiveLosses >= 3) key = "consecutive_losses";
    if (!key || key === lastRiskWarningKey.current) return;
    lastRiskWarningKey.current = key;
    generateRiskWarning(riskState)
      .then((prompt) => {
        setLocalPrompts((existing) => [prompt, ...existing].slice(0, 200));
      })
      .catch(() => {});
  }, [riskState]);
  useEffect(() => {
    if (riskState && !riskState.atLimit) {
      const nearLimit =
        riskState.dailyPnlR <= -0.8 * riskState.maxDailyLossR && riskState.maxDailyLossR > 0;
      if (!nearLimit && riskState.consecutiveLosses < 3) {
        lastRiskWarningKey.current = null;
      }
    }
  }, [riskState]);

  async function handlePromptResponse(
    prompt: CoachingPrompt,
    response: "took_it" | "watching" | "passed"
  ) {
    await sessionBridge.addEvent({
      eventType: "prompt_response",
      setupId: prompt.setupId,
      data: { promptEventId: prompt.sessionEventId, response, note: null },
      sessionId,
    });
    const latest = await sessionBridge.listEvents(300).catch(() => null);
    if (latest) setSessionEvents(latest);
  }

  async function handleTookIt(
    prompt: CoachingPrompt,
    direction: "long" | "short",
    size: number,
    entryPrice: number
  ) {
    await handlePromptResponse(prompt, "took_it");

    if (sessionId) {
      const trade: TradeRecord = {
        id: crypto.randomUUID(),
        sessionId,
        setupId: prompt.setupId,
        entryTime: Date.now(),
        entryPrice,
        exitTime: null,
        exitPrice: null,
        direction,
        size,
        stopPrice: null,
        targetPrices: [],
        resultR: null,
        planned: true,
        rulesFollowed: null,
        emotionalState: null,
        notes: "",
        source: "manual",
      };
      try {
        await tradeBridge.create(trade);
      } catch {
        // Fall back to legacy addTrade
        await sessionBridge.addTrade({
          setupId: prompt.setupId,
          direction,
          size,
          entryPrice,
        });
      }
    }
  }

  function handleOnboardingComplete(newSetups: Setup[]) {
    setSetups((prev) => [...newSetups, ...prev]);
    setShowOnboarding(false);
    setView("briefing");
  }

  async function handleStartSession(focusNote?: string) {
    try {
      const sid = await sessionBridge.start();
      setSessionId(sid);
      if (focusNote) {
        setPreSessionNote(focusNote);
        await sessionBridge.addEvent({
          eventType: "focus_note",
          data: { note: focusNote },
          sessionId: sid,
        });
      }
      setView("dashboard");
      setAppStatus(null);
    } catch {
      setAppStatus("Session start failed; check backend connection");
      setView("dashboard");
    }
  }

  const noSetupBanner = !hasActiveSetups && !showOnboarding ? (
    <Card className="mx-3 mt-2 border-warning/30 shrink-0">
      <CardContent className="py-3 flex items-center justify-between">
        <p className="text-text-secondary text-sm">
          No active setups — The Desk is watching but won't alert on setups.
        </p>
        <Button variant="outline" size="sm" onClick={() => setView("playbook")}>
          Add a Setup
        </Button>
      </CardContent>
    </Card>
  ) : null;

  const statusBanner = (appStatus || connection !== "connected") ? (
    <Card className="mx-3 mt-2 shrink-0">
      <CardContent className="py-3">
        {appStatus && <p className="text-text-secondary text-sm">{appStatus}</p>}
        {connection !== "connected" && (
          <p className="text-text-secondary text-sm">
            Data feed not connected. Start a feed in Onboarding or Replay controls.
          </p>
        )}
      </CardContent>
    </Card>
  ) : null;

  const shortcutModal = showShortcuts ? (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
      onClick={() => setShowShortcuts(false)}
    >
      <Card className="w-80" onClick={(e) => e.stopPropagation()}>
        <CardContent className="p-4 space-y-2">
          <h3 className="text-text-primary font-semibold mb-3">Keyboard Shortcuts</h3>
          {[
            ["N", "Quick note"],
            ["1", "Took it (respond to latest prompt)"],
            ["2", "Watching"],
            ["3", "Passed"],
            ["Ctrl+E", "End session"],
            ["?", "Toggle this help"],
          ].map(([key, desc]) => (
            <div key={key} className="flex justify-between text-sm">
              <kbd className="bg-surface px-2 py-0.5 rounded text-text-primary font-mono">
                {key}
              </kbd>
              <span className="text-text-secondary">{desc}</span>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  ) : null;

  return (
    <div className="grid grid-rows-[48px_1fr_40px] h-screen">
      <RiskBar riskState={riskState} riskConfig={riskConfig} connection={connection} />

      {showOnboarding ? (
        <div className="flex flex-col overflow-auto">
          {statusBanner}
          <main className="flex justify-center p-3 flex-1">
            <OnboardingWizard
              onComplete={handleOnboardingComplete}
              onSkip={() => setShowOnboarding(false)}
            />
          </main>
        </div>
      ) : view === "briefing" ? (
        <div className="flex flex-col overflow-auto">
          {statusBanner}
          {noSetupBanner}
          <main className="flex justify-center p-3 flex-1">
            <PreSessionBriefing
              marketState={marketState}
              setups={setups}
              riskState={riskState}
              onStartSession={handleStartSession}
            />
          </main>
        </div>
      ) : view === "review" ? (
        <div className="flex flex-col overflow-auto">
          {statusBanner}
          <main className="flex flex-col items-center gap-3 p-3 flex-1">
            <SessionReview
              prompts={prompts}
              alerts={setupAlerts}
              riskState={riskState}
              sessionEvents={sessionEvents}
              sessionId={sessionId}
              preSessionNote={preSessionNote}
            />
            <Button variant="outline" onClick={() => setView("dashboard")}>
              Back to Dashboard
            </Button>
          </main>
        </div>
      ) : view === "playbook" ? (
        <div className="flex flex-col overflow-auto">
          {statusBanner}
          <main className="grid grid-cols-[1fr_1fr] gap-3 p-3 flex-1 min-h-0 overflow-auto">
            <SetupList
              setups={setups}
              onUpdate={refreshSetups}
              onEdit={(setup) => {
                setEditingSetup(setup);
              }}
            />
            <PlaybookBuilder
              onCreated={(setup) => {
                setSetups((existing) => [setup, ...existing]);
                refreshSetups();
              }}
              templates={templates}
            />
          </main>
        </div>
      ) : view === "settings" ? (
        <div className="flex flex-col overflow-auto">
          {statusBanner}
          <main className="flex justify-center p-3 flex-1">
            <SettingsPanel />
          </main>
        </div>
      ) : (
        <div className="flex flex-col overflow-hidden">
          {statusBanner}
          {noSetupBanner}
          <main className="grid grid-cols-[240px_1fr_300px] gap-3 p-3 flex-1 min-h-0 overflow-auto">
            <MarketSidebar marketState={marketState} setupAlerts={setupAlerts} />
            <CoachingFeed
              prompts={prompts}
              riskState={riskState}
              onRespond={handlePromptResponse}
              onTookIt={handleTookIt}
            />
            <section className="flex flex-col gap-3 overflow-y-auto">
              <PlaybookBuilder
                onCreated={(setup) => {
                  setSetups((existing) => [setup, ...existing]);
                  refreshSetups();
                }}
                templates={templates}
              />
              <ReplayControls
                onStartReplay={() => {
                  setView("dashboard");
                  setAppStatus(null);
                }}
                onStopReplay={() => {
                  setView("review");
                }}
              />
            </section>
          </main>
        </div>
      )}

      <footer className="border-t border-border px-3 py-2 flex items-center gap-2">
        <Input
          id="quick-note"
          placeholder="Quick note (N)"
          value={quickNote}
          onChange={(event) => setQuickNote(event.target.value)}
          className="max-w-xs"
        />
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" onClick={() => setView("briefing")}>
            Briefing
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setView("dashboard")}>
            Dashboard
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setView("playbook")}>
            Playbook
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setView("review")}>
            Review
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setView("settings")}>
            Settings
          </Button>
        </div>
      </footer>

      {shortcutModal}
    </div>
  );
}

function SettingsPanel() {
  const [riskConfig, setRiskConfig] = useState({
    rValuePoints: 8,
    rValueDollars: 40,
    maxDailyLossR: 3,
    maxConsecutiveLosses: 3,
    maxTradesPerSession: 8 as number | null,
    noTradeZones: [] as unknown[],
    maxDailyLossDollars: null as number | null,
  });
  const [dtcHost, setDtcHost] = useState("127.0.0.1");
  const [dtcPort, setDtcPort] = useState(11099);
  const [symbol, setSymbol] = useState("NQ");
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    import("./lib/tauri-bridge").then(({ riskBridge }) => {
      riskBridge.getConfig().then((config) => {
        setRiskConfig({
          rValuePoints: config.rValuePoints,
          rValueDollars: config.rValueDollars,
          maxDailyLossR: config.maxDailyLossR,
          maxConsecutiveLosses: config.maxConsecutiveLosses,
          maxTradesPerSession: config.maxTradesPerSession ?? null,
          noTradeZones: config.noTradeZones,
          maxDailyLossDollars: config.maxDailyLossDollars ?? null,
        });
      }).catch(() => {});
    });
  }, []);

  async function handleSave() {
    try {
      const { riskBridge } = await import("./lib/tauri-bridge");
      await riskBridge.saveConfig(riskConfig);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch {
      // swallow in local-only mode
    }
  }

  return (
    <div className="w-full max-w-lg space-y-4">
      <Card>
        <CardContent className="p-4 space-y-3">
          <h3 className="text-text-primary font-semibold">Connection</h3>
          <div className="grid grid-cols-3 gap-2">
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Host</label>
              <Input value={dtcHost} onChange={(e) => setDtcHost(e.target.value)} />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Port</label>
              <Input type="number" value={dtcPort} onChange={(e) => setDtcPort(Number(e.target.value))} />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Symbol</label>
              <Input value={symbol} onChange={(e) => setSymbol(e.target.value)} />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="p-4 space-y-3">
          <h3 className="text-text-primary font-semibold">Risk Configuration</h3>
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">R-value (points)</label>
              <Input
                type="number"
                value={riskConfig.rValuePoints}
                onChange={(e) => setRiskConfig({ ...riskConfig, rValuePoints: Number(e.target.value) })}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">R-value (dollars)</label>
              <Input
                type="number"
                value={riskConfig.rValueDollars}
                onChange={(e) => setRiskConfig({ ...riskConfig, rValueDollars: Number(e.target.value) })}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Max daily loss (R)</label>
              <Input
                type="number"
                value={riskConfig.maxDailyLossR}
                onChange={(e) => setRiskConfig({ ...riskConfig, maxDailyLossR: Number(e.target.value) })}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Max daily loss ($) e.g. Lucid 750</label>
              <Input
                type="number"
                value={riskConfig.maxDailyLossDollars ?? ""}
                placeholder="Optional"
                onChange={(e) => {
                  const v = e.target.value ? Number(e.target.value) : null;
                  setRiskConfig({ ...riskConfig, maxDailyLossDollars: v });
                }}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Max consecutive losses</label>
              <Input
                type="number"
                value={riskConfig.maxConsecutiveLosses}
                onChange={(e) => setRiskConfig({ ...riskConfig, maxConsecutiveLosses: Number(e.target.value) })}
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-xs text-text-secondary">Max trades/session</label>
              <Input
                type="number"
                value={riskConfig.maxTradesPerSession ?? ""}
                onChange={(e) => {
                  const v = e.target.value ? Number(e.target.value) : null;
                  setRiskConfig({ ...riskConfig, maxTradesPerSession: v });
                }}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Button onClick={handleSave} className="w-full">
        {saved ? "Saved" : "Save Settings"}
      </Button>
    </div>
  );
}
