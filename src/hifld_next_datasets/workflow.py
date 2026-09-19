"""Workflow primitives that execute agents in parallel for the inventory."""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from pathlib import Path

from agno.workflow.parallel import Parallel
from agno.workflow.step import Step
from agno.workflow.types import StepOutput, StepInput
from agno.workflow.workflow import Workflow

from hifld_next_datasets.core import DatasetSources, arun_agent_for_row

LOGGER = logging.getLogger("hifld_next_datasets.workflow")


def _build_step(
    row: dict[str, str],
    model: str | None,
    output_path: Path | None = None,
    file_lock: asyncio.Lock | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> Step:
    has_run = False

    async def executor(step_input: StepInput) -> StepOutput:  # noqa: D401
        nonlocal has_run
        if has_run:
            return StepOutput(step_name="dataset-source-lookup", success=True, stop=True)

        dataset_name = row.get("title") or row.get("filename") or row.get("path") or "unnamed dataset"
        LOGGER.info("Workflow looking up %s", dataset_name)

        try:
            run_agent = arun_agent_for_row(row, model)
            if semaphore is not None:
                async with semaphore:
                    dataset_sources = await run_agent
            else:
                dataset_sources = await run_agent
            LOGGER.info(
                "Agent finished %s: found %d files, %d candidates.\n%s",
                dataset_name,
                len(dataset_sources.files),
                len(dataset_sources.candidates),
                dataset_sources.model_dump_json(indent=2)
            )

            if output_path and file_lock:
                async with file_lock:
                    with output_path.open("a", encoding="utf-8") as fh:
                        fh.write(dataset_sources.model_dump_json())
                        fh.write("\n")

        except Exception as exc:  # pragma: no cover - resilient fallback
            LOGGER.exception("Failed to run agent for %s", dataset_name)
            dataset_sources = DatasetSources(
                dataset=dataset_name,
                publisher=row.get("publisher") or row.get("agency"),
                summary=f"Workflow failure: {exc}",
                query="",
                files=[],
                candidates=[],
            )

        has_run = True
        return StepOutput(step_name=dataset_name, content=dataset_sources, success=True)

    return Step(name="dataset-source-lookup", executor=executor)


def _extract_dataset_outputs(parallel_output: StepOutput) -> Iterable[DatasetSources]:
    if not parallel_output.steps:
        return []

    results: list[DatasetSources] = []
    for step in parallel_output.steps:
        if not step.content:
            continue
        if isinstance(step.content, DatasetSources):
            results.append(step.content)
        elif isinstance(step.content, dict):
            results.append(DatasetSources.model_validate(step.content))
    return results


def _find_parallel_output(step_results: list[StepOutput | list[StepOutput]]) -> StepOutput | None:
    for entry in step_results:
        candidates = entry if isinstance(entry, list) else [entry]
        for candidate in candidates:
            if getattr(candidate, "step_type", None) == "Parallel":
                return candidate
    return None


async def run_dataset_workflow(
    rows: list[dict[str, str]],
    model: str | None = None,
    output_path: Path | None = None,
    max_concurrent: int = 10,
) -> list[DatasetSources]:
    if not rows:
        return []

    file_lock = asyncio.Lock() if output_path else None
    semaphore = asyncio.Semaphore(max_concurrent)
    steps = [
        _build_step(row, model, output_path, file_lock, semaphore)
        for row in rows
    ]
    parallel_step = Parallel(*steps, name="dataset-parallel")
    workflow = Workflow(name="HIFLD dataset lookup", steps=[parallel_step])
    run_output = await workflow.arun()
    parallel_output = _find_parallel_output(run_output.step_results or [])
    if not parallel_output:
        return []

    return list(_extract_dataset_outputs(parallel_output))
