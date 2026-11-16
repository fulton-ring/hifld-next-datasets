"""EPA Facilities (Labels) thin adapter over the shared EPA Facilities lifecycle."""

from __future__ import annotations

from dagster_hifld.sources.epa_facilities import (
    EPA_FACILITIES_PROFILE,
    stage_epa_facilities,
)

SLUG = "epa-facilities-labels"
EPA_FACILITIES_LABELS_PROFILE = EPA_FACILITIES_PROFILE.labels


def stage_epa_facilities_labels(storage, *, capture_dir: str | None = None):
    """Stage producer MapServer layer 0 using the shared verifier and lifecycle."""
    return stage_epa_facilities(
        storage,
        profile=EPA_FACILITIES_LABELS_PROFILE,
        capture_dir=capture_dir,
    )
