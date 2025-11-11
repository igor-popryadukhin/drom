"""Implementation of stage 2 of the Drom parser."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .stage1 import fetch_html
from .state import StateManager

logger = logging.getLogger(__name__)

STAGE2_STATE_KEY = "stage2"
STAGE2_OUTPUT = "stage2_results.xlsx"
IMAGE_MAIN_CLASS = "b-image b-image_type_centred-image b-image_theme_cat-resp-main"
IMAGE_THUMB_CLASS = "b-line__thumb b-image"
COMPLETION_ROW_CLASS = "y7l57t2"


class Stage2Processor:
    def __init__(self, base_df: pd.DataFrame, output_path: Path, state_manager: StateManager) -> None:
        self.base_df = base_df.copy()
        self.output_path = output_path
        self.state_manager = state_manager
        self.dataframe = self._load_existing()

    def _load_existing(self) -> pd.DataFrame:
        df = self.base_df.copy()
        if self.output_path.exists():
            existing = pd.read_excel(self.output_path)
            existing = existing.reindex(range(len(existing)))
        else:
            existing = None
        for column, default in (
            ("main_image_url", ""),
            ("image_urls", "[]"),
            ("configurations", "[]"),
        ):
            if existing is not None and column in existing.columns:
                values = existing[column]
                df[column] = values.reindex(df.index, fill_value=default)
            else:
                df[column] = default
        return df

    def save(self) -> None:
        self.dataframe.to_excel(self.output_path, index=False)
        logger.debug("Stage 2 results saved to %s", self.output_path)

    def process(self) -> None:
        state = self.state_manager.get_stage_state(STAGE2_STATE_KEY)
        row_index = int(state.get("row_index", 0))
        total_rows = len(self.dataframe)
        for idx in range(row_index, total_rows):
            row = self.dataframe.iloc[idx]
            url = row.get("url")
            if not url:
                logger.warning("Row %d has no URL, skipping", idx)
                continue
            try:
                logger.info("Stage 2: processing %s (%d/%d)", url, idx + 1, total_rows)
                html = fetch_html(url)
                soup = BeautifulSoup(html, "html.parser")
                main_image = extract_main_image(soup, base_url=url)
                thumb_images = extract_additional_images(soup, base_url=url)
                configurations = extract_configurations(soup, base_url=url)
                self.dataframe.at[idx, "main_image_url"] = main_image or ""
                self.dataframe.at[idx, "image_urls"] = json.dumps(thumb_images, ensure_ascii=False)
                self.dataframe.at[idx, "configurations"] = json.dumps(configurations, ensure_ascii=False)
                self.save()
                self.state_manager.update_stage_state(STAGE2_STATE_KEY, row_index=idx + 1)
            except Exception as exc:  # pragma: no cover - runtime error reporting
                logger.exception("Failed to process %s: %s", url, exc)
                break


def run_stage2(data_dir: Path, state_manager: StateManager) -> None:
    stage1_path = data_dir / "stage1_results.xlsx"
    if not stage1_path.exists():
        raise FileNotFoundError("Stage 1 results not found. Run stage 1 first.")
    base_df = pd.read_excel(stage1_path)
    output_path = data_dir / STAGE2_OUTPUT
    processor = Stage2Processor(base_df, output_path, state_manager)
    processor.process()


def extract_main_image(soup: BeautifulSoup, base_url: str) -> str | None:
    anchor = soup.find("a", class_=IMAGE_MAIN_CLASS, href=True)
    if anchor is None:
        return None
    return urljoin(base_url, anchor["href"])


def extract_additional_images(soup: BeautifulSoup, base_url: str) -> list[str]:
    anchors = soup.find_all("a", class_=IMAGE_THUMB_CLASS, href=True)
    return [urljoin(base_url, anchor["href"]) for anchor in anchors]


def extract_configurations(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    configurations: list[dict[str, Any]] = []
    for row in soup.find_all("tr", class_=COMPLETION_ROW_CLASS):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        link = cells[1].find("a", href=True)
        if not link:
            continue
        name = link.get_text(strip=True)
        href = urljoin(base_url, link["href"])
        configurations.append({"name": name, "url": href})
    return configurations
