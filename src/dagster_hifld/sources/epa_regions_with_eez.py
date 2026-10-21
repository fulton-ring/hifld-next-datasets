"""Inventory #76 adapter for the shared EPA EEZ capture implementation."""

from dagster_hifld.sources.epa_eez import (
    WITH_EEZ_IDENTITY,
    acquire,
    inspect_captures,
    stage_epa_eez,
)


def stage_epa_regions_with_eez(storage, *, capture_dir=None):
    return stage_epa_eez(
        storage,
        capture_dir=capture_dir,
        dataset=WITH_EEZ_IDENTITY,
    )
