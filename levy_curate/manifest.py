"""An append-only record of what a deposit run actually did.

Written incrementally *during* the loop, not after, so a run that dies partway
still leaves an accurate account of which datasets exist and which files landed.
This is what turns resuming a failed batch from "edit the [N:] slice and hope"
into "diff the manifest against the dataset's real file list".
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class Manifest:
    """A JSON run log keyed on dataset title."""

    def __init__(self, path: str | Path, batch: str = ""):
        self.path = Path(path)
        self.data: dict[str, Any] = {
            "batch": batch,
            "started": _now(),
            "target": "",
            "datasets": {},
        }
        if self.path.exists():
            self.data.update(json.loads(self.path.read_text()))

    # -- recording ----------------------------------------------------

    def start(self, title: str, n_files: int) -> None:
        self.data["datasets"].setdefault(title, {})
        self.data["datasets"][title].update(
            {"status": "in_progress", "n_files": n_files, "started": _now()}
        )
        self.save()

    def succeeded(self, title: str, pid: str, n_files: int) -> None:
        self.data["datasets"][title] = {
            "status": "uploaded",
            "pid": pid,
            "n_files": n_files,
            "finished": _now(),
        }
        self.save()

    def failed(self, title: str, error: str, pid: str | None = None) -> None:
        entry = self.data["datasets"].setdefault(title, {})
        entry.update({"status": "failed", "error": str(error)[:2000], "finished": _now()})
        if pid:
            entry["pid"] = pid
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=False))

    # -- querying -----------------------------------------------------

    def status_of(self, title: str) -> str:
        return self.data["datasets"].get(title, {}).get("status", "pending")

    def is_done(self, title: str) -> bool:
        return self.status_of(title) == "uploaded"

    def pending(self, titles: list[str]) -> list[str]:
        """Titles not yet successfully uploaded -- the resume list."""
        return [t for t in titles if not self.is_done(t)]

    def pids(self) -> dict[str, str]:
        """{dataset_title: pid} for everything uploaded, like upload_pids.json."""
        return {
            t: e["pid"]
            for t, e in self.data["datasets"].items()
            if e.get("status") == "uploaded" and e.get("pid")
        }

    def report(self) -> str:
        rows = self.data["datasets"]
        if not rows:
            return "manifest is empty -- nothing deposited yet"
        width = max(len(t) for t in rows)
        lines = [f"manifest: {self.path}"]
        for title, e in rows.items():
            lines.append(
                f"  {title:<{width}}  {e.get('status','?'):<12} "
                f"{e.get('n_files','?'):>6} files  {e.get('pid','')}"
            )
        return "\n".join(lines)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
