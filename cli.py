"""Command-line interface for the Drom parser."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

from .llm import build_llm_client
from .stage1 import run_stage1
from .stage2 import run_stage2
from .stage3 import run_stage3
from .state import StateManager

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Drom multi-stage parser")
    parser.add_argument("stage", choices=["1", "2", "3", "all"], help="Stage to run")
    parser.add_argument(
        "--entry-points",
        type=Path,
        default=Path("entry-points.txt"),
        help="Path to the entry points file",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory where intermediate results will be stored",
    )
    parser.add_argument(
        "--llm-endpoint",
        type=str,
        default=None,
        help="Optional HTTP endpoint for the LLM provider",
    )
    parser.add_argument(
        "--llm-api-key",
        type=str,
        default=None,
        help="Optional API key for the LLM provider",
    )
    parser.add_argument(
        "--llm-model",
        type=str,
        default=None,
        help="Optional model identifier for the LLM provider",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    data_dir: Path = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    state_path = data_dir / "state.json"
    state_manager = StateManager(state_path)

    llm_client = build_llm_client(
        endpoint=args.llm_endpoint,
        api_key=args.llm_api_key,
        model=args.llm_model,
    )

    if args.stage in {"1", "all"}:
        run_stage1(
            entry_points_path=args.entry_points,
            data_dir=data_dir,
            state_manager=state_manager,
            llm_client=llm_client,
        )
    if args.stage in {"2", "all"}:
        run_stage2(
            data_dir=data_dir,
            state_manager=state_manager,
        )
    if args.stage in {"3", "all"}:
        run_stage3(
            data_dir=data_dir,
            state_manager=state_manager,
            llm_client=llm_client,
        )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
