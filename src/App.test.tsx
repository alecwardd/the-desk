import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";
import App from "./App";

vi.mock("@tauri-apps/api/core", () => ({
  invoke: vi.fn(() => Promise.resolve([])),
}));

vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn(() => Promise.resolve(() => {})),
}));

describe("App", () => {
  it("renders onboarding on first load", async () => {
    await act(async () => {
      render(<App />);
    });
    expect(screen.getByText("The Desk")).toBeInTheDocument();
    expect(screen.getByText("Welcome to The Desk")).toBeInTheDocument();
    expect(screen.getByText("Step 1: Data Connection")).toBeInTheDocument();
  });

  it("shows dashboard after skipping onboarding", async () => {
    await act(async () => {
      render(<App />);
    });
    const skipButton = screen.getByText("Skip onboarding");
    await userEvent.click(skipButton);
    expect(screen.getByText("Coaching Feed")).toBeInTheDocument();
    expect(screen.getByText("Market State")).toBeInTheDocument();
  });
});
