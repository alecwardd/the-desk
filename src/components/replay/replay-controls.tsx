import { useState } from "react";
import { feedBridge, replayBridge, sessionBridge } from "../../lib/tauri-bridge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface Props {
  onStartReplay: () => void;
  onStopReplay: () => void;
}

export function ReplayControls({ onStartReplay, onStopReplay }: Props) {
  const [speed, setSpeed] = useState("1x");
  const [isPlaying, setIsPlaying] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [recordingPath, setRecordingPath] = useState("");

  async function handleStartScid() {
    setStatus("Starting SCID feed...");
    try {
      await feedBridge.startScidFeed();
      setStatus("SCID tail running");
      setIsPlaying(true);
      onStartReplay();
    } catch {
      setStatus("Failed to start SCID feed — check config.toml and .scid path");
    }
  }

  async function handleStop() {
    try {
      await feedBridge.stopScidFeed();
      setIsPlaying(false);
      setStatus("SCID feed stopped");
      onStopReplay();
    } catch {
      setStatus("Failed to stop feed");
    }
  }

  async function handleLoadRecording() {
    if (!recordingPath.trim()) {
      setStatus("Provide a recording path first");
      return;
    }
    try {
      const count = await replayBridge.loadRecording(recordingPath.trim());
      setStatus(`Loaded ${count} replay events`);
    } catch (e) {
      setStatus(`Replay load failed: ${e}`);
    }
  }

  async function handleStartReplay() {
    try {
      const numericSpeed = Number(speed.replace("x", ""));
      await replayBridge.start(numericSpeed);
      setIsPlaying(true);
      setStatus(`Replay running at ${speed}`);
      onStartReplay();
    } catch (e) {
      setStatus(`Replay start failed: ${e}`);
    }
  }

  async function handlePauseReplay() {
    try {
      await replayBridge.pause();
      setIsPlaying(false);
      setStatus("Replay paused");
    } catch (e) {
      setStatus(`Replay pause failed: ${e}`);
    }
  }

  async function handleStartSession() {
    try {
      const id = await sessionBridge.start();
      setStatus(`Session ${id.slice(0, 8)} started`);
    } catch (e) {
      setStatus(`Session start failed: ${e}`);
    }
  }

  async function handleStopSession() {
    try {
      await sessionBridge.stop();
      setStatus("Session saved");
    } catch (e) {
      setStatus(`Session stop failed: ${e}`);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Data Feed & Session</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex gap-2">
          {!isPlaying ? (
            <Button onClick={handleStartScid}>Start SCID Feed</Button>
          ) : (
            <Button variant="destructive" onClick={handleStop}>
              Stop Feed
            </Button>
          )}
        </div>

        <div className="flex gap-2">
          <Button variant="outline" onClick={handleStartSession}>
            Start Session
          </Button>
          <Button variant="outline" onClick={handleStopSession}>
            End Session
          </Button>
        </div>

        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Playback Speed
          </label>
          <select
            value={speed}
            onChange={(e) => setSpeed(e.target.value)}
            className="h-9 rounded-md border border-border-subtle bg-surface px-3 text-sm text-text-primary focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="1x">1x</option>
            <option value="2x">2x</option>
            <option value="4x">4x</option>
            <option value="8x">8x</option>
          </select>
        </div>

        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium text-text-secondary">
            Recording Path
          </label>
          <Input
            value={recordingPath}
            onChange={(e) => setRecordingPath(e.target.value)}
            placeholder="C:\Users\...\session_xxx.desk"
          />
        </div>

        <div className="flex gap-2">
          <Button variant="outline" onClick={handleLoadRecording}>
            Load Recording
          </Button>
          {!isPlaying ? (
            <Button onClick={handleStartReplay}>Start Replay</Button>
          ) : (
            <Button variant="secondary" onClick={handlePauseReplay}>
              Pause Replay
            </Button>
          )}
        </div>

        {status && (
          <p className="text-sm text-text-secondary">{status}</p>
        )}
      </CardContent>
    </Card>
  );
}
