# Census Designated Places draft

This pipeline reconstructs the historical Census Bureau 2024 substate FileGDB
layer `Census_Designated_Place` from the pinned complete archive. It preserves
the native rows, identifiers, physical field declarations, and EPSG:4269
MultiPolygon WKB without repair or reprojection. The 2024 edition is not
represented as a current release, and no baseline, ID/schema compatibility,
license, crosswalk, or release decision is accepted by this draft.

The registered `cdp_source_integrity` job runs the source asset; its monthly
schedule remains stopped. The source archive and response receipt are retained
in isolated staging with the candidate. A refresh must pass the fixed source
URL, byte count, SHA-256, archive roster, layer schema, full population,
state/territory counts, and native geometry checks. Changed or incomplete
observations are quarantined rather than substituted.

Focused behavior tests:

```sh
PYTHONPATH=src python -m unittest discover -s tests -p test_census_designated_places.py
```

This pipeline is a local unapproved draft. Do not activate its stopped schedule
or publish a release without the pending source, baseline, compatibility,
rights, and independent approval decisions.
