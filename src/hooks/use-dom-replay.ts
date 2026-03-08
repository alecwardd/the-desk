import { useEffect, useState } from "react";
import { domReplayBridge, events, subscribe } from "../lib/tauri-bridge";
import type { DomReplayFrame, DomReplayLoadResult, DomReplayStatus } from "../lib/types";

export function useDomReplay() {
  const [currentFrame, setCurrentFrame] = useState<DomReplayFrame | null>(null);
  const [status, setStatus] = useState<DomReplayStatus | null>(null);
  const [loadResult, setLoadResult] = useState<DomReplayLoadResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function refreshStatus() {
    try {
      const next = await domReplayBridge.status();
      setStatus(next);
    } catch {
      // Non-Tauri web mode.
    }
  }

  useEffect(() => {
    let cleanup: (() => void) | undefined;
    subscribe<DomReplayFrame>(events.domReplayFrame, (frame) => {
      setCurrentFrame(frame);
      setError(null);
    })
      .then((unlisten) => {
        cleanup = unlisten;
      })
      .catch(() => {
        // Non-Tauri web mode.
      });
    return () => cleanup?.();
  }, []);

  useEffect(() => {
    void refreshStatus();
    const timer = window.setInterval(() => {
      void refreshStatus();
    }, 500);
    return () => window.clearInterval(timer);
  }, []);

  async function load(startMs: number, endMs: number, levelsPerSide = 12) {
    try {
      const result = await domReplayBridge.load(startMs, endMs, levelsPerSide);
      setLoadResult(result);
      setError(null);
      await refreshStatus();
      return result;
    } catch (err) {
      const message = String(err);
      setError(message);
      throw err;
    }
  }

  async function play(speed: number) {
    try {
      await domReplayBridge.start(speed);
      setError(null);
      await refreshStatus();
    } catch (err) {
      const message = String(err);
      setError(message);
      throw err;
    }
  }

  async function pause() {
    try {
      await domReplayBridge.pause();
      setError(null);
      await refreshStatus();
    } catch (err) {
      const message = String(err);
      setError(message);
      throw err;
    }
  }

  async function stop() {
    try {
      await domReplayBridge.stop();
      setError(null);
      await refreshStatus();
    } catch (err) {
      const message = String(err);
      setError(message);
      throw err;
    }
  }

  async function seek(timestampMs: number) {
    try {
      await domReplayBridge.seek(timestampMs);
      setError(null);
      await refreshStatus();
    } catch (err) {
      const message = String(err);
      setError(message);
      throw err;
    }
  }

  return {
    currentFrame,
    status,
    loadResult,
    error,
    load,
    play,
    pause,
    stop,
    seek,
    refreshStatus,
  };
}
