"""CLI entry point that iterates through the inventory and runs the workflow."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from hifld_next_datasets.core import load_inventory
from hifld_next_datasets.workflow import run_dataset_workflow

LOGGER = logging.getLogger("hifld_next_datasets.scraper")

async def _run(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO if not args.quiet else logging.WARNING, format="%(levelname)s: %(message)s")
    rows = load_inventory(args.inventory)
    selection = rows[args.offset :] if args.limit <= 0 else rows[args.offset : args.offset + args.limit]
    # offset is 0-based: 0 = first data row, 30 = 31st data row (file line 32)
    first_row_one_based = args.offset + 1
    LOGGER.info(
        "Processing %d rows starting at index %d (1-based row %d)",
        len(selection),
        args.offset,
        first_row_one_based,
    )

    if args.overwrite and args.offset == 0 and args.output.exists():
        args.output.write_text("", encoding="utf-8")
    if not args.output.parent.exists():
        args.output.parent.mkdir(parents=True, exist_ok=True)

    try:
        dataset_results = await run_dataset_workflow(
            selection,
            args.model or os.environ.get("HIFLD_AGNO_MODEL"),
            output_path=args.output,
            max_concurrent=args.max_concurrent,
        )
    except Exception as exc:
        LOGGER.exception("Workflow run failed: %s", exc)
        return

    LOGGER.info("Workflow completed %d datasets. Results incrementally appended to %s", len(dataset_results), args.output)


def main() -> None:
    load_dotenv(override=True)
    parser = argparse.ArgumentParser(description="Identify source URLs for each dataset in the inventory via a workflow")
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("HIFLD_Open_Inventory_12112025.csv"),
        help="Path to the inventory CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("dataset-source-hints.jsonl"),
        help="File to store per-dataset JSON results",
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows to process (0=all, default)")
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="0-based row index to start from (0=first data row, 1=second, 30=31st data row)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=os.environ.get("HIFLD_AGNO_MODEL", "gpt-5.1-codex-mini"),
        help="Optional Agno model identifier",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Maximum number of agents running at once (default: 10)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output file when starting from offset 0; if not set and file exists, append",
    )
    parser.add_argument("--quiet", action="store_true", help="Only emit warnings and errors")
    args = parser.parse_args()

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
