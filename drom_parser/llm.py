"""LLM client abstractions used by the parser."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

STAGE1_PROMPT_TEMPLATE = (
    "Из этого HTML-фрагмента извлеки данные и сформируй JSON-массив. Формат объекта: {\n"
    '  "brand": "",\n'
    '  "model": "",\n'
    '  "body_code": "",\n'
    '  "years": "",\n'
    '  "generation": "",\n'
    '  "type": "",\n'
    '  "url": "",\n'
    '  "region": ""\n'
    "} Требования: - Вернуть строго валидный JSON. - Не добавлять комментарии, описания, текст до или после JSON.\n"
    "- Если данных нет — вернуть пустой массив [].\n"
    "- Все строки оставить как в исходном тексте (ничего не сокращать и не интерпретировать).\n"
    "- URL должен быть полным.\n"
    "- Не писать объяснений. В ответе должно быть только содержимое JSON.\n\n"
    "ФРАГМЕНТ HTML\n\n{fragment}"
)

STAGE3_PROMPT_TEMPLATE = (
    "Из следующего HTML блока выдели HTML с техническими характеристиками,"
    " сохранив исходную разметку. Верни JSON объект вида {\"specs_html\": \"...\"}"
    " без лишнего текста. Если данных нет — верни {\"specs_html\": \"\"}."
    "\n\nHTML:\n{fragment}"
)


class BaseLLMClient(ABC):
    """Abstract interface for LLM interactions."""

    @abstractmethod
    def extract_stage1_data(self, html_fragment: str, base_url: str) -> list[dict[str, Any]]:
        """Extract structured data for stage 1."""

    @abstractmethod
    def extract_stage3_specs(self, html_fragment: str) -> str:
        """Extract technical specification HTML for stage 3."""


@dataclass
class HTTPClientConfig:
    endpoint: str
    api_key: Optional[str] = None
    model: Optional[str] = None
    timeout: int = 60


class HTTPJSONLLMClient(BaseLLMClient):
    """LLM client that sends prompts to an HTTP endpoint and expects JSON responses."""

    def __init__(self, config: HTTPClientConfig):
        self.config = config

    def _post(self, payload: dict[str, Any]) -> Any:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        response = requests.post(
            self.config.endpoint,
            json=payload,
            timeout=self.config.timeout,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()

    def _build_payload(self, prompt: str) -> dict[str, Any]:
        if self.config.model:
            return {"model": self.config.model, "input": prompt}
        return {"input": prompt}

    def extract_stage1_data(self, html_fragment: str, base_url: str) -> list[dict[str, Any]]:
        prompt = STAGE1_PROMPT_TEMPLATE.format(fragment=html_fragment)
        result = self._post(self._build_payload(prompt))
        if isinstance(result, dict) and "output" in result:
            result = result["output"]
        if isinstance(result, str):
            result = json.loads(result)
        if not isinstance(result, list):
            raise ValueError("Unexpected LLM response format for stage 1")
        items: list[dict[str, Any]] = []
        for item in result:
            if not isinstance(item, dict):
                logger.debug("Skipping non-dict item from LLM: %r", item)
                continue
            if item.get("url"):
                item["url"] = _join_url(base_url, item["url"])
            items.append(item)
        return items

    def extract_stage3_specs(self, html_fragment: str) -> str:
        prompt = STAGE3_PROMPT_TEMPLATE.format(fragment=html_fragment)
        result = self._post(self._build_payload(prompt))
        if isinstance(result, dict):
            if "output" in result:
                result = result["output"]
            if "specs_html" in result:
                return result["specs_html"] or ""
        if isinstance(result, str):
            parsed = json.loads(result)
            if isinstance(parsed, dict) and "specs_html" in parsed:
                return parsed["specs_html"] or ""
        raise ValueError("Unexpected LLM response format for stage 3")


class RuleBasedLLMClient(BaseLLMClient):
    """Fallback client that approximates the LLM behaviour with deterministic parsing."""

    def extract_stage1_data(self, html_fragment: str, base_url: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html_fragment, "html.parser")
        items: list[dict[str, Any]] = []
        for block in soup.select("div") or [soup]:
            anchor = block.find("a", href=True)
            if not anchor:
                continue
            url = _join_url(base_url, anchor["href"])
            text = anchor.get_text(" ", strip=True)
            if not text:
                continue
            parts = text.split()
            brand = parts[0] if parts else ""
            model = " ".join(parts[1:]) if len(parts) > 1 else ""
            items.append(
                {
                    "brand": brand,
                    "model": model,
                    "body_code": "",
                    "years": "",
                    "generation": "",
                    "type": "",
                    "url": url,
                    "region": "",
                }
            )
        return items

    def extract_stage3_specs(self, html_fragment: str) -> str:
        soup = BeautifulSoup(html_fragment, "html.parser")
        specs_container = soup.find(class_="b-left-side")
        if specs_container is None:
            specs_container = soup
        return specs_container.decode()


def build_llm_client(
    endpoint: Optional[str],
    api_key: Optional[str],
    model: Optional[str],
) -> BaseLLMClient:
    """Factory that selects the appropriate LLM client implementation."""

    if endpoint:
        config = HTTPClientConfig(endpoint=endpoint, api_key=api_key, model=model)
        logger.info("Using HTTP LLM client with endpoint %s", endpoint)
        return HTTPJSONLLMClient(config)

    logger.info("Using rule-based fallback LLM client")
    return RuleBasedLLMClient()


def _join_url(base_url: str, relative: str) -> str:
    if not relative:
        return relative
    return urljoin(base_url.rstrip("/") + "/", relative)
