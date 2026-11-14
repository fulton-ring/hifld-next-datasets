"""Native, unapproved capture of EPA's FRS-published ICIS wastewater plant layer.

This is the ``epa-frs-icis-wastewater-treatment-plants`` HIFLD inventory entry.
The DHS HIFLD Open crosswalk row names EPA's own ``epa.maps.arcgis.com`` item
``0895b107f9184e7cb31707767b506a64`` ("EPA Facility Registry Service -
Integrated Compliance Information System (ICIS) Wastewater Treatment Plants")
as the external landing page and the EPA_GEO-hosted ``FRS_Wastewater``
FeatureServer as the open REST service, so the capture reuses the shared EPA
ICIS wastewater facility core and publishes under this entry's own slug.
"""
from dagster_hifld.sources.epa_icis_wastewater import (
    ITEM_ID, LAYER_ID, LAYER_NAME, LAYER_URL, MAX_CAPTURE_BYTES,
    MAX_RESPONSE_BYTES, PORTAL_ITEM_URL, PUBLISHER, SCHEMA_SHA256, SERVICE_URL,
    capture_epa_icis_wastewater,
)

DATASET_SLUG = "epa-frs-icis-wastewater-treatment-plants"
FILE_SLUG = DATASET_SLUG
DEFAULT_CAPTURE_ROOT = "/tmp/hifld-native-reconstruction/393/captures"
CAPTURE_BUDGET_BYTES = MAX_CAPTURE_BYTES
RESPONSE_BUDGET_BYTES = MAX_RESPONSE_BYTES


def capture_epa_frs_icis_wastewater(storage, *, version: str,
                                    capture_directory: str | None = None,
                                    retained_capture_directory: str | None = None,
                                    retained_capture_manifest_sha256: str | None = None,
                                    max_capture_bytes: int = CAPTURE_BUDGET_BYTES,
                                    max_response_bytes: int = RESPONSE_BUDGET_BYTES) -> dict:
    """Capture the shared EPA ICIS wastewater layer under this entry's slug."""
    return capture_epa_icis_wastewater(
        storage, version=version, dataset_slug=DATASET_SLUG,
        capture_directory=capture_directory,
        retained_capture_directory=retained_capture_directory,
        retained_capture_manifest_sha256=retained_capture_manifest_sha256,
        max_capture_bytes=max_capture_bytes, max_response_bytes=max_response_bytes,
        default_capture_root=DEFAULT_CAPTURE_ROOT,
    )
