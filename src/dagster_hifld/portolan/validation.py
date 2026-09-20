"""Offline validation and normalization for a candidate Portolan release."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict
from urllib.parse import urljoin, urlparse

from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError
from rashid import validate as validate_with_rashid
from rashid.model import Severity

from .catalog import (
    HIFLD_NEXT_HOST_NAME,
    HIFLD_NEXT_HOST_URL,
    PORTOLAN_STAC_EXTENSION,
    stac_spatial_bbox,
)


class ValidationException(TypedDict):
    version_path: str
    reason: str
    source_evidence: str


class PortolanValidationError(ValueError):
    """A candidate release cannot be published safely."""


def _read_document(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"Candidate STAC document must be an object: {path}")
    return value


def _provider_has_source_role(
    provider: object, agency: object, source_publisher: str | None
) -> bool:
    if not isinstance(provider, dict):
        return False
    if provider.get("name") == HIFLD_NEXT_HOST_NAME:
        return False
    roles = provider.get("roles")
    return (
        isinstance(roles, list)
        and any(role in {"publisher", "producer"} for role in roles)
    ) or (
        roles is None
        and (
            (
                isinstance(agency, str)
                and bool(agency)
                and provider.get("name") == agency
            )
            or (
                source_publisher is not None
                and provider.get("name") == source_publisher
            )
        )
    )


def _normalize_providers(
    document: dict[str, object], source_publisher: str | None = None
) -> bool:
    providers = document.get("providers")
    if not isinstance(providers, list):
        return False
    normalized = False
    agency = document.get("hifld:agency")
    for provider in providers:
        if _provider_has_source_role(provider, agency, source_publisher):
            assert isinstance(provider, dict)
            provider["roles"] = ["producer"]
            normalized = True
    if (
        not normalized
        and source_publisher
        and not any(
            isinstance(provider, dict) and provider.get("name") != HIFLD_NEXT_HOST_NAME
            for provider in providers
        )
    ):
        providers.insert(0, {"name": source_publisher, "roles": ["producer"]})
        normalized = True
    if normalized and not any(
        isinstance(provider, dict) and provider.get("name") == HIFLD_NEXT_HOST_NAME
        for provider in providers
    ):
        providers.append(
            {
                "name": HIFLD_NEXT_HOST_NAME,
                "roles": ["host"],
                "url": HIFLD_NEXT_HOST_URL,
            }
        )
    return normalized


def _remove_portolan_extension(document: dict[str, object]) -> None:
    extensions = document.get("stac_extensions")
    if isinstance(extensions, list):
        document["stac_extensions"] = [
            extension
            for extension in extensions
            if extension != PORTOLAN_STAC_EXTENSION
        ]


def _normalize_spatial_extent(document: dict[str, object]) -> None:
    extent = document.get("extent")
    if not isinstance(extent, dict):
        return
    spatial = extent.get("spatial")
    if not isinstance(spatial, dict):
        return
    boxes = spatial.get("bbox")
    if not isinstance(boxes, list) or len(boxes) != 1:
        return
    bounds = boxes[0]
    if (
        not isinstance(bounds, list)
        or len(bounds) != 4
        or not all(
            isinstance(value, (int, float)) and not isinstance(value, bool)
            for value in bounds
        )
    ):
        return
    normalized, extent_status = stac_spatial_bbox(bounds)
    if extent_status is not None:
        spatial["bbox"] = [normalized]
        document["hifld:spatial_extent_status"] = extent_status


def normalize_candidate_tree(
    root: Path, source_publisher: Callable[[str], str | None] | None = None
) -> list[ValidationException]:
    """Normalize source producers and report collections lacking one.

    This operates exclusively on local candidate JSON. It never assigns HIFLD
    Next a producer role and only removes the declared Portolan profile from a
    Collection that has no authored source producer evidence.
    """
    exceptions: list[ValidationException] = []
    for path in sorted(root.rglob("collection.json")):
        document = _read_document(path)
        if document.get("type") != "Collection":
            continue
        identifier = document.get("id")
        if not isinstance(identifier, str):
            raise TypeError(f"Collection ID must be a string: {path}")
        _normalize_spatial_extent(document)
        has_source_producer = _normalize_providers(document)
        if not has_source_producer and source_publisher is not None:
            has_source_producer = _normalize_providers(
                document, source_publisher(identifier)
            )
        if not has_source_producer:
            _remove_portolan_extension(document)
            exceptions.append(
                {
                    "version_path": identifier,
                    "reason": "missing_source_producer",
                    "source_evidence": "not_present",
                }
            )
        path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    exceptions.sort(key=lambda exception: exception["version_path"])
    report_path = root / "_catalog" / "validation-exceptions.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps({"exceptions": exceptions}, indent=2) + "\n", encoding="utf-8"
    )
    return exceptions


def _candidate_documents(root: Path) -> dict[str, Path]:
    return {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*.json")
        if path.name in {"catalog.json", "collection.json"}
    }


def _link_target(document_path: str, href: str, document_paths: set[str]) -> str | None:
    parsed = urlparse(href)
    if parsed.scheme:
        candidates = [
            candidate
            for candidate in document_paths
            if parsed.path.rstrip("/").endswith(f"/{candidate}")
            or parsed.path.rstrip("/") == candidate
        ]
        return max(candidates, key=len) if candidates else None
    normalized = urlparse(
        urljoin(f"https://candidate.invalid/{document_path}", parsed.path)
    ).path.lstrip("/")
    return normalized if normalized in document_paths else None


def _validate_local_links(root: Path, document_paths: dict[str, Path]) -> list[str]:
    failures: list[str] = []
    known_paths = {
        path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()
    }
    root_document = _read_document(root / "catalog.json")
    root_links = root_document.get("links")
    root_href: str | None = None
    if isinstance(root_links, list):
        for link in root_links:
            if (
                isinstance(link, dict)
                and link.get("rel") == "self"
                and isinstance(link.get("href"), str)
            ):
                root_href = link["href"]
                break
    root_url = urlparse(root_href) if isinstance(root_href, str) else None
    local_prefix = (
        root_url.path.removesuffix("catalog.json")
        if root_url is not None and root_url.scheme
        else None
    )
    for relative_path, path in document_paths.items():
        document = _read_document(path)
        links = document.get("links")
        if not isinstance(links, list):
            continue
        for link in links:
            if not isinstance(link, dict) or link.get("rel") not in {
                "child",
                "parent",
                "root",
                "self",
                "agents",
                "describedby",
                "license",
            }:
                continue
            href = link.get("href")
            parsed = urlparse(href) if isinstance(href, str) else None
            is_relative = parsed is not None and not parsed.scheme
            is_same_origin = (
                parsed is not None
                and root_url is not None
                and parsed.scheme == root_url.scheme
                and parsed.netloc == root_url.netloc
                and local_prefix is not None
                and parsed.path.startswith(local_prefix)
            )
            is_same_release = is_relative or is_same_origin
            if (
                isinstance(href, str)
                and is_same_release
                and _link_target(relative_path, href, known_paths) is None
            ):
                failures.append(
                    f"{relative_path}: unresolved {link['rel']} link {href}"
                )
        assets = document.get("assets")
        if isinstance(assets, dict):
            for asset in assets.values():
                if not isinstance(asset, dict):
                    continue
                href = asset.get("href")
                if (
                    isinstance(href, str)
                    and not urlparse(href).scheme
                    and _link_target(relative_path, href, known_paths) is None
                ):
                    failures.append(f"{relative_path}: unresolved asset {href}")
    return failures


def _profile_validator() -> Draft7Validator:
    schema_path = Path(__file__).with_name("schemas") / "portolan-v0.2.0.json"
    schema = _read_document(schema_path)
    return Draft7Validator(schema)


def _profile_error_messages(error: ValidationError, document_type: str) -> list[str]:
    """Select the relevant schema branch without serializing an entire Collection."""
    if error.validator == "oneOf" and error.context:
        branch = 0 if document_type == "Catalog" else 1
        relevant = [
            child
            for child in error.context
            if child.schema_path and child.schema_path[0] == branch
        ]
        if relevant:
            return [
                f"{'/'.join(str(part) for part in child.path) or 'document'}: "
                f"{child.message[:300]}"
                for child in relevant
            ]
    return [
        f"{'/'.join(str(part) for part in error.path) or 'document'}: "
        f"{error.message[:300]}"
    ]


def validate_candidate_tree(root: Path) -> None:
    """Fail closed on malformed local STAC or declared Portolan profile errors."""
    document_paths = _candidate_documents(root)
    report = validate_with_rashid(
        root, rules=(), structural=True, schema=False, data=False
    )
    failures = [
        f"{finding.path}: {finding.message}"
        for finding in report.findings
        if finding.severity in {Severity.ERROR, Severity.WARNING}
    ]
    failures.extend(_validate_local_links(root, document_paths))
    validator = _profile_validator()
    for relative_path, path in document_paths.items():
        document = _read_document(path)
        extensions = document.get("stac_extensions")
        if (
            not isinstance(extensions, list)
            or PORTOLAN_STAC_EXTENSION not in extensions
        ):
            continue
        for error in sorted(validator.iter_errors(document), key=str):
            failures.extend(
                f"{relative_path}: Portolan profile: {message}"
                for message in _profile_error_messages(error, str(document.get("type")))
            )
    if failures:
        raise PortolanValidationError("; ".join(failures[:20]))
