import { useState } from "react";
import type { Setup } from "../../lib/types";
import { setupBridge } from "../../lib/tauri-bridge";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface Props {
  setups: Setup[];
  onUpdate: () => void;
  onEdit: (setup: Setup) => void;
}

export function SetupList({ setups, onUpdate, onEdit }: Props) {
  const [showActiveOnly, setShowActiveOnly] = useState(false);

  const filtered = showActiveOnly ? setups.filter((s) => s.active) : setups;

  async function handleToggle(setup: Setup) {
    try {
      await setupBridge.toggle(setup.id, !setup.active);
      onUpdate();
    } catch {
      /* bridge unavailable — ignore */
    }
  }

  async function handleDelete(id: string) {
    try {
      await setupBridge.delete(id);
      onUpdate();
    } catch {
      /* bridge unavailable */
    }
  }

  async function handleDuplicate(id: string) {
    try {
      await setupBridge.duplicate(id);
      onUpdate();
    } catch {
      /* bridge unavailable */
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-text-primary">Setups</h2>
        <div className="flex gap-2">
          <Button
            variant={showActiveOnly ? "default" : "outline"}
            size="sm"
            onClick={() => setShowActiveOnly(true)}
          >
            Active Only
          </Button>
          <Button
            variant={!showActiveOnly ? "default" : "outline"}
            size="sm"
            onClick={() => setShowActiveOnly(false)}
          >
            All
          </Button>
        </div>
      </div>

      {filtered.length === 0 && (
        <p className="py-6 text-center text-sm text-muted-foreground">
          {showActiveOnly
            ? "No active setups. Toggle the filter to see all."
            : "No setups yet. Create one with the Playbook Builder."}
        </p>
      )}

      <div className="flex flex-col gap-3">
        {filtered.map((setup) => (
          <Card key={setup.id}>
            <CardHeader className="pb-0">
              <div className="flex items-start justify-between">
                <div className="flex flex-col gap-1">
                  <CardTitle className="text-sm">{setup.name}</CardTitle>
                  {setup.description && (
                    <p className="line-clamp-2 text-xs text-muted-foreground">
                      {setup.description}
                    </p>
                  )}
                </div>
                <div className="flex items-center gap-1.5">
                  <Badge variant={setup.active ? "default" : "outline"}>
                    {setup.active ? "Active" : "Inactive"}
                  </Badge>
                  {setup.backtestResults?.winRate !== undefined && (
                    <Badge variant="secondary">
                      WR {setup.backtestResults.winRate}%
                    </Badge>
                  )}
                  {setup.backtestResults?.samples !== undefined && (
                    <Badge variant="secondary">
                      N={setup.backtestResults.samples}
                    </Badge>
                  )}
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="xs"
                  onClick={() => handleToggle(setup)}
                >
                  {setup.active ? "Deactivate" : "Activate"}
                </Button>
                <Button
                  variant="outline"
                  size="xs"
                  onClick={() => onEdit(setup)}
                >
                  Edit
                </Button>
                <Button
                  variant="outline"
                  size="xs"
                  onClick={() => handleDuplicate(setup.id)}
                >
                  Duplicate
                </Button>
                <Button
                  variant="destructive"
                  size="xs"
                  onClick={() => handleDelete(setup.id)}
                >
                  Delete
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
