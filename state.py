"""Persistence helpers for tracking parser progress across stages."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Dict

logger = logging.getLogger(__name__)


class StateManager:
    """Simple JSON-backed state store."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = Lock()
        self._data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            logger.debug("State file %s does not exist, starting fresh", self.path)
            return
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                self._data = json.load(fh)
            logger.debug("Loaded state from %s: %s", self.path, self._data)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to read state file %s: %s", self.path, exc)
            self._data = {}

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(self._data, fh, ensure_ascii=False, indent=2)
        tmp_path.replace(self.path)
        logger.debug("State written to %s", self.path)

    def get_stage_state(self, stage: str) -> dict[str, Any]:
        return dict(self._data.get(stage, {}))

    def update_stage_state(self, stage: str, **updates: Any) -> None:
        with self._lock:
            stage_state = self._data.setdefault(stage, {})
            stage_state.update(updates)
            self._flush()

    def reset_stage(self, stage: str) -> None:
        with self._lock:
            if stage in self._data:
                del self._data[stage]
                self._flush()

    def snapshot(self) -> dict[str, Any]:
        return dict(self._data)
