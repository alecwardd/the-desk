import { useEffect, useMemo, useState } from "react";
import { PreSessionBriefing } from "./components/briefing/pre-session-briefing";
import { CoachingFeed } from "./components/coaching/coaching-feed";
import { MarketSidebar } from "./components/dashboard/market-sidebar";
import { OnboardingWizard } from "./components/onboarding/onboarding-wizard";
import { PlaybookBuilder } from "./components/playbook/playbook-builder";
import { ReplayControls } from "./components/replay/replay-controls";
import { SessionReview } from "./components/review/session-review";
import { RiskBar } from "./components/risk/risk-bar";
import { useCoachingPrompts } from "./hooks/use-coaching-prompts";
import { useConnection } from "./hooks/use-connection";
import { useMarketState } from "./hooks/use-market-state";
import { useRiskState } from "./hooks/use-risk-state";
import { useSetupAlerts } from "./hooks/use-setup-alerts";
import { generateCoachingPrompt } from "./lib/claude";
import { sessionBridge, setupBridge } from "./lib/tauri-bridge";
import type { CoachingPrompt, SessionEventRecord, Setup } from "./lib/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";

type View = "dashboard" | "briefing" | "review";

export default function App() {
  const connection = useConnection();
  const marketState = useMarketState();
  const riskState = useRiskState();
  const setupAlerts = useSetupAlerts();
  const streamPrompts = useCoachingPrompts();
  const [localPrompts, setLocalPrompts] = useState<CoachingPrompt[]>([]);
  const [setups, setSetups] = useState<Setup[]>([]);
  const [showOnboarding, setShowOnboarding] = useState(true);
  const [quickNote, setQuickNote] = useState("");
  const [view, setView] = useState<View>("dashboard");
  const [sessionEvents, setSessionEvents] = useState<SessionEventRecord[]>([]);
  const [appStatus, setAppStatus] = useState<string | null>(null);

  const prompts = useMemo(
    () => [...localPrompts, ...streamPrompts],
    [localPrompts, streamPrompts]
  );

  useEffect(() => {
    setupBridge
      .list()
      .then((loaded) => setSetups(loaded))
      .catch(() => setAppStatus("Setups unavailable; working in local-only mode"));
    sessionBridge
      .listEvents(300)
      .then((events) => setSessionEvents(events))
      .catch(() => {});
  }, []);

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
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [prompts]);

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

  async function handlePromptResponse(
    prompt: CoachingPrompt,
    response: "took_it" | "watching" | "passed"
  ) {
    await sessionBridge.addEvent({
      eventType: "prompt_response",
      setupId: prompt.setupId,
      data: { promptEventId: prompt.sessionEventId, response, note: null },
    });
    const latest = await sessionBridge.listEvents(300).catch(() => null);
    if (latest) setSessionEvents(latest);
  }

  function handleOnboardingComplete(newSetups: Setup[]) {
    setSetups((prev) => [...newSetups, ...prev]);
    setShowOnboarding(false);
    setView("briefing");
  }

  async function handleStartSession(focusNote?: string) {
    try {
      await sessionBridge.start();
      if (focusNote) {
        await sessionBridge.addEvent({
          eventType: "focus_note",
          data: { note: focusNote },
        });
      }
      setView("dashboard");
      setAppStatus(null);
    } catch {
      setAppStatus("Session start failed; check backend connection");
      setView("dashboard");
    }
  }

  const statusBanner = (appStatus || connection !== "connected") ? (
    <Card className="mx-3 mt-3 shrink-0">
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

  return (
    <div className="grid grid-rows-[48px_1fr_40px] h-screen">
      <RiskBar riskState={riskState} connection={connection} />

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
          <main className="flex justify-center p-3 flex-1">
            <PreSessionBriefing
              marketState={marketState}
              setups={setups}
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
            />
            <Button variant="outline" onClick={() => setView("dashboard")}>
              Back to Dashboard
            </Button>
          </main>
        </div>
      ) : (
        <div className="flex flex-col overflow-hidden">
          {statusBanner}
          <main className="grid grid-cols-[240px_1fr_300px] gap-3 p-3 flex-1 min-h-0 overflow-auto">
            <MarketSidebar marketState={marketState} setupAlerts={setupAlerts} />
            <CoachingFeed prompts={prompts} onRespond={handlePromptResponse} />
            <section className="flex flex-col gap-3 overflow-y-auto">
              <PlaybookBuilder
                onCreated={(setup) => setSetups((existing) => [setup, ...existing])}
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
          <Button variant="ghost" size="sm" onClick={() => setView("review")}>
            Review
          </Button>
        </div>
      </footer>
    </div>
  );
}
