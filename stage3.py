"""Implementation of stage 3 of the Drom parser."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from .llm import BaseLLMClient
from .stage1 import fetch_html
from .state import StateManager

logger = logging.getLogger(__name__)

STAGE3_STATE_KEY = "stage3"
STAGE3_OUTPUT = "stage3_results.xlsx"


class Stage3Processor:
    def __init__(
        self,
        base_df: pd.DataFrame,
        output_path: Path,
        state_manager: StateManager,
        llm_client: BaseLLMClient,
    ) -> None:
        self.base_df = base_df.copy()
        self.output_path = output_path
        self.state_manager = state_manager
        self.llm_client = llm_client
        self.dataframe = self._load_existing()

    def _load_existing(self) -> pd.DataFrame:
        df = self.base_df.copy()
        if self.output_path.exists():
            existing = pd.read_excel(self.output_path)
            existing = existing.reindex(range(len(existing)))
        else:
            existing = None
        if existing is not None and "configuration_specs" in existing.columns:
            df["configuration_specs"] = existing["configuration_specs"].reindex(
                df.index, fill_value="[]"
            )
        else:
            df["configuration_specs"] = "[]"
        return df

    def save(self) -> None:
        self.dataframe.to_excel(self.output_path, index=False)
        logger.debug("Stage 3 results saved to %s", self.output_path)

    def process(self) -> None:
        state = self.state_manager.get_stage_state(STAGE3_STATE_KEY)
        row_index = int(state.get("row_index", 0))
        total_rows = len(self.dataframe)
        for idx in range(row_index, total_rows):
            row = self.dataframe.iloc[idx]
            configurations_raw = row.get("configurations") or "[]"
            try:
                configurations: list[dict[str, Any]] = json.loads(configurations_raw)
            except json.JSONDecodeError:
                logger.warning("Row %d has invalid configurations JSON, skipping", idx)
                configurations = []
            if not configurations:
                logger.info("Stage 3: no configurations for row %d", idx)
                self.dataframe.at[idx, "configuration_specs"] = json.dumps([], ensure_ascii=False)
                self.save()
                self.state_manager.update_stage_state(STAGE3_STATE_KEY, row_index=idx + 1)
                continue
            try:
                logger.info("Stage 3: processing row %d/%d", idx + 1, total_rows)
                specs = self._fetch_specs(configurations)
                self.dataframe.at[idx, "configuration_specs"] = json.dumps(specs, ensure_ascii=False)
                self.save()
                self.state_manager.update_stage_state(STAGE3_STATE_KEY, row_index=idx + 1)
            except Exception as exc:  # pragma: no cover - runtime error reporting
                logger.exception("Failed to process row %d: %s", idx, exc)
                break

    def _fetch_specs(self, configurations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for config in configurations:
            url = config.get("url")
            name = config.get("name", "")
            if not url:
                continue
            try:
                html = fetch_html(url)
                soup = BeautifulSoup(html, "html.parser")
                target = soup.select_one("body div.b-left-side")
                fragment = target.decode() if target is not None else soup.body.decode() if soup.body else html
                specs_html = self.llm_client.extract_stage3_specs(fragment)
                results.append({"name": name, "url": url, "specs_html": specs_html})
            except Exception as exc:
                logger.exception("Failed to extract specs for %s: %s", url, exc)
                results.append({"name": name, "url": url, "specs_html": ""})
        return results


def run_stage3(
    data_dir: Path,
    state_manager: StateManager,
    llm_client: BaseLLMClient,
) -> None:
    stage2_path = data_dir / "stage2_results.xlsx"
    if not stage2_path.exists():
        raise FileNotFoundError("Stage 2 results not found. Run stage 2 first.")
    base_df = pd.read_excel(stage2_path)
    output_path = data_dir / STAGE3_OUTPUT
    processor = Stage3Processor(base_df, output_path, state_manager, llm_client)
    processor.process()
