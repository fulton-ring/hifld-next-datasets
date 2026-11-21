"""Ingest asset registry.

New datasets declare ``INGEST_ASSETS`` in their own asset module (see
``discovery.py``) rather than adding entries here. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.discovery import discovered, unique

hifld_ingest_assets = unique(discovered("INGEST_ASSETS"))
