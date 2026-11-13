"""Collect dataset registrations that asset modules declare themselves.

A dataset module can list its definitions in module-level ``INGEST_ASSETS``,
``ASSET_CHECKS``, ``JOBS`` and ``SCHEDULES`` instead of editing the shared
registry and ``definitions.py``, so parallel dataset PRs do not conflict.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterable

# Modules that build on the registry itself would import it circularly.
_SKIP = frozenset({"discovery", "ingest_registry", "supported_assets"})


def discovered(attribute: str) -> list:
    import dagster_hifld.assets as package

    found = []
    for info in sorted(pkgutil.iter_modules(package.__path__), key=lambda item: item.name):
        if info.ispkg or info.name.startswith("_") or info.name in _SKIP:
            continue
        module = importlib.import_module(f"{package.__name__}.{info.name}")
        found.extend(getattr(module, attribute, ()))
    return found


def unique(*groups: Iterable) -> list:
    """Concatenate groups, keeping the first occurrence of each object."""
    seen: set[int] = set()
    result = []
    for group in groups:
        for item in group:
            if id(item) not in seen:
                seen.add(id(item))
                result.append(item)
    return result
