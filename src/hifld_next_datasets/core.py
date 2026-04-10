"""Shared logic for the HIFLD dataset scraper workflow."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Sequence

import polars as pl
import httpx
from agno.agent import Agent
from agno.models.openrouter import OpenRouter
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.duckduckgo import DuckDuckGoTools
from pydantic import BaseModel, Field, field_validator

LOGGER = logging.getLogger("hifld_next_datasets.core")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPTS_DIR = PROJECT_ROOT / "prompts"


def _load_markdown(name: str) -> str:
    path = PROMPTS_DIR / name
    return path.read_text(encoding="utf-8").strip()


AGENT_INSTRUCTIONS = _load_markdown("agent-instructions.md")
PROMPT_TEMPLATE = _load_markdown("query-template.md")


class SourceCandidate(BaseModel):
    """Information about a candidate download link."""

    label: str = Field(..., description="Short label describing where the link points")
    url: str = Field(..., description="The URL that likely hosts the source file(s)")
    confidence: float = Field(
        0.5,
        ge=0.0,
        le=1.0,
        description="0-1 confidence score that this link is official",
    )
    notes: str | None = Field(
        None, description="Why this link was chosen (brief sentence)"
    )

    @field_validator("url")
    def ensure_url(cls, value: str) -> str:
        if not value.startswith("http"):
            raise ValueError("url must start with http(s)")
        return value


class DatasetFile(BaseModel):
    """Represents a geospatial file within a dataset, along with its download URLs."""

    name: str = Field(
        ..., description="Describe the dataset file, e.g., Territorial Sea footprints"
    )
    format: str | None = Field(
        None, description="Geospatial format (e.g., GDB, GeoJSON, KMZ)"
    )
    urls: list[SourceCandidate] = Field(
        default_factory=list,
        description="One or more download links for this file",
    )


class DatasetSources(BaseModel):
    """Structured JSON that the agent should emit for a single dataset."""

    dataset: str = Field(..., description="Friendly dataset name")
    publisher: str | None = Field(None, description="Publisher or agency name")
    summary: str | None = Field(
        None, description="Short reasoning or context for the URLs"
    )
    query: str = Field(..., description="DuckDuckGo search query used")
    files: list[DatasetFile] = Field(
        default_factory=list,
        description="Geospatial files within the dataset, each with format and download URLs",
    )
    candidates: list[SourceCandidate] = Field(
        default_factory=list,
        description="Optional fallback list of candidate download links (most reliable first)",
    )


def check_download_url(url: str) -> str:
    """Check if a URL is valid and seems to point to a downloadable file without downloading the entire file.

    Args:
        url: The URL to check

    Returns:
        A string describing the result, including HTTP status, content-type, and content-length if available.
    """
    try:
        # First try a HEAD request
        with httpx.Client(follow_redirects=True, timeout=10.0) as client:
            response = client.head(url)

            # Some servers reject HEAD requests with 405 Method Not Allowed or 403 Forbidden
            if response.status_code in (405, 403, 400, 501):
                # Fallback to GET with stream=True so we only fetch headers
                with client.stream("GET", url) as stream_response:
                    headers = stream_response.headers
                    status_code = stream_response.status_code
            else:
                headers = response.headers
                status_code = response.status_code

            content_type = headers.get("content-type", "unknown")
            content_length = headers.get("content-length", "unknown")

            if status_code >= 400:
                return f"URL returned error status: {status_code}"

            return f"Success (Status {status_code}). Content-Type: {content_type}, Content-Length: {content_length} bytes."

    except Exception as e:
        return f"Failed to check URL: {str(e)}"


def build_agent(model_name: str | None = None) -> Agent:
    """Create a per-dataset agent configured with guidance and tools."""

    default_model_id = os.environ.get("HIFLD_AGNO_MODEL", "gpt-5.1-codex-mini")
    model_id = model_name or default_model_id

    if isinstance(model_id, str) and model_id.startswith("openrouter:"):
        model_id = model_id.replace("openrouter:", "", 1)

    api_key = os.environ.get("OPENROUTER_API_KEY")
    tool_limit = int(os.environ.get("HIFLD_TOOL_CALL_LIMIT", "200"))

    return Agent(
        name="HIFLD Source Hunter",
        description="Searches DuckDuckGo for the origin of an inventory dataset",
        instructions=AGENT_INSTRUCTIONS,
        tools=[
            DuckDuckGoTools(),
            Crawl4aiTools(max_length=15000),
            check_download_url,
        ],
        output_schema=DatasetSources,
        tool_call_limit=tool_limit,  # per agent run (each parallel agent has its own limit)
        model=(
            OpenRouter(id=model_id, max_tokens=8192, api_key=api_key)
            if isinstance(model_id, str)
            else model_id
        ),
        markdown=False,
    )


def load_inventory(csv_path: Path) -> list[dict[str, str]]:
    """Load the HIFLD inventory CSV into a list of normalized rows."""

    df = pl.read_csv(csv_path)
    str_columns = [pl.col(col).cast(pl.Utf8).fill_null("") for col in df.columns]
    df = df.with_columns(str_columns)
    return df.to_dicts()


def format_metadata(row: dict[str, str]) -> str:
    """Render the most useful pieces of metadata for the agent prompt."""

    fields: Sequence[tuple[str, str]] = [
        ("Title", "title"),
        ("Filename", "filename"),
        ("Path", "path"),
        ("Publisher", "publisher"),
        ("Agency", "agency"),
        ("Office", "office"),
        ("Download Location", "Download Location"),
        ("Notes", "Notes"),
    ]
    snippets = []
    for label, key in fields:
        value = row.get(key) or row.get(key.lower(), "")
        if value:
            snippets.append(f"{label}: {value}")

    if desc := row.get("description"):
        snippets.append(f"Description: {desc}")
    if keywords := row.get("keywords"):
        snippets.append(f"Keywords: {keywords}")

    return "\n".join(snippets) if snippets else "(no metadata preserved)"


def build_query(dataset_name: str, row: dict[str, str]) -> str:
    """Construct a DuckDuckGo query that targets the dataset source."""

    fragments = [dataset_name]
    publisher = row.get("publisher") or row.get("agency")
    if publisher:
        fragments.append(publisher)
    fragments.extend(["dataset source file", "download"])
    if keywords := row.get("keywords"):
        fragments.extend(keywords.split())
    return " ".join(fragment for fragment in fragments if fragment).strip()


def build_prompt(dataset_name: str, row: dict[str, str], query: str) -> str:
    metadata = format_metadata(row)
    return PROMPT_TEMPLATE.replace("{metadata}", metadata).replace("{query}", query)


async def arun_agent_for_row(
    row: dict[str, str], model_name: str | None = None
) -> DatasetSources:
    """Run the Agno agent for a single CSV row asynchronously, returning structured JSON."""

    dataset_name = (
        row.get("title") or row.get("filename") or row.get("path") or "unnamed dataset"
    )
    publisher = row.get("publisher") or row.get("agency")
    query = build_query(dataset_name, row)
    prompt = build_prompt(dataset_name, row, query)
    agent = build_agent(model_name)
    run_output = await agent.arun(prompt, stream=False)
    content = run_output.content

    if isinstance(content, DatasetSources):
        return content

    if isinstance(content, dict):
        try:
            return DatasetSources.model_validate(
                {
                    **content,
                    "dataset": content.get("dataset", dataset_name),
                    "publisher": content.get("publisher", publisher),
                    "query": content.get("query", query),
                }
            )
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Failed to validate structured output: %s", exc)

    return DatasetSources(
        dataset=dataset_name,
        publisher=publisher,
        summary=None,
        query=query,
        candidates=[],
    )
