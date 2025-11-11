"""Implementation of stage 1 of the Drom parser."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .llm import BaseLLMClient
from .state import StateManager

logger = logging.getLogger(__name__)

STAGE1_STATE_KEY = "stage1"
STAGE1_OUTPUT = "stage1_results.xlsx"
STAGE1_COLUMNS = [
    "brand",
    "model",
    "body_code",
    "years",
    "generation",
    "type",
    "url",
    "region",
    "entry_url",
]


class Stage1Processor:
    def __init__(
        self,
        entry_points: Iterable[str],
        output_path: Path,
        state_manager: StateManager,
        llm_client: BaseLLMClient,
    ) -> None:
        self.entry_points = list(entry_points)
        self.output_path = output_path
        self.state_manager = state_manager
        self.llm_client = llm_client
        self.dataframe = self._load_existing()
        self._existing_keys = set(
            zip(self.dataframe.get("entry_url", []), self.dataframe.get("url", []))
        )

    def _load_existing(self) -> pd.DataFrame:
        if self.output_path.exists():
            df = pd.read_excel(self.output_path)
            missing = [col for col in STAGE1_COLUMNS if col not in df.columns]
            for col in missing:
                df[col] = ""
            return df[STAGE1_COLUMNS]
        return pd.DataFrame(columns=STAGE1_COLUMNS)

    def save(self) -> None:
        self.dataframe.to_excel(self.output_path, index=False)
        logger.debug("Stage 1 results saved to %s", self.output_path)

    def process(self) -> None:
        state = self.state_manager.get_stage_state(STAGE1_STATE_KEY)
        entry_index = int(state.get("entry_index", 0))
        block_index = int(state.get("block_index", 0))

        for idx in range(entry_index, len(self.entry_points)):
            entry_url = self.entry_points[idx]
            try:
                logger.info("Processing entry %s (%d/%d)", entry_url, idx + 1, len(self.entry_points))
                html = fetch_html(entry_url)
                soup = BeautifulSoup(html, "html.parser")
                blocks = soup.find_all("div", class_="css-18bfsxm e1ei9t6a4")
                if not blocks:
                    logger.warning("No blocks found for %s", entry_url)
                start_block = block_index if idx == entry_index else 0
                for b_idx in range(start_block, len(blocks)):
                    block_html = blocks[b_idx].decode()
                    records = self._extract_records(block_html, entry_url)
                    self._append_records(records)
                    self.save()
                    self.state_manager.update_stage_state(
                        STAGE1_STATE_KEY,
                        entry_index=idx,
                        block_index=b_idx + 1,
                    )
                block_index = 0
                self.state_manager.update_stage_state(
                    STAGE1_STATE_KEY,
                    entry_index=idx + 1,
                    block_index=0,
                )
            except Exception as exc:  # pragma: no cover - runtime error reporting
                logger.exception("Failed to process entry %s: %s", entry_url, exc)
                break

    def _extract_records(self, block_html: str, entry_url: str) -> list[dict[str, str]]:
        try:
            records = self.llm_client.extract_stage1_data(block_html, entry_url)
        except Exception as exc:
            logger.exception("LLM extraction failed, falling back to empty list: %s", exc)
            records = []
        cleaned: list[dict[str, str]] = []
        for record in records:
            record.setdefault("brand", "")
            record.setdefault("model", "")
            record.setdefault("body_code", "")
            record.setdefault("years", "")
            record.setdefault("generation", "")
            record.setdefault("type", "")
            record.setdefault("url", "")
            record.setdefault("region", "")
            record["entry_url"] = entry_url
            cleaned.append(record)
        return cleaned

    def _append_records(self, records: list[dict[str, str]]) -> None:
        new_records = []
        for record in records:
            key = (record.get("entry_url", ""), record.get("url", ""))
            if key in self._existing_keys:
                continue
            self._existing_keys.add(key)
            new_records.append(record)
        if not new_records:
            return
        df = pd.DataFrame(new_records, columns=STAGE1_COLUMNS)
        self.dataframe = pd.concat([self.dataframe, df], ignore_index=True)


def run_stage1(
    entry_points_path: Path,
    data_dir: Path,
    state_manager: StateManager,
    llm_client: BaseLLMClient,
) -> None:
    if not entry_points_path.exists():
        raise FileNotFoundError(f"Entry points file not found: {entry_points_path}")
    with entry_points_path.open("r", encoding="utf-8") as fh:
        entry_points = [line.strip() for line in fh if line.strip()]
    output_path = data_dir / STAGE1_OUTPUT
    processor = Stage1Processor(entry_points, output_path, state_manager, llm_client)
    processor.process()


def fetch_html(url: str, timeout: int = 30) -> str:
    logger.debug("Fetching %s", url)
    response = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; DromParser/1.0; +https://www.drom.ru/)",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding
    return response.text
