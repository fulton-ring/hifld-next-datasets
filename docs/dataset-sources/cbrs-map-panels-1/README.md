# FWS CBRS Map Panels draft

- Official landing: https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data
- Candidate is the exact FWS-linked delegated USGS CBRS Mapper MapServer layer 5, `CBRS Map Panels`. The retained native REST observation contains 574 polygons, 13 source attributes, native EPSG:4269 coordinates, and matching before/after count and full OBJECTID rosters.
- Registered local job: `fws_cbrs_map_panels_draft_refresh`; its monthly schedule remains stopped. It rejects remote storage.
- Packaged construction provenance uses the shared Git-or-installed-package identity. A packaged runtime records a `packaged-sha256:` identity over installed source, dependency lockfiles, and runtime inventory; it does not claim a Git commit.
- The retained observation was collected on 2026-10-19. Replay preserves those original receipt timestamps and does not claim a new acquisition or producer release date.
- The feed publication date is unknown. FWS use limits and share-back/attribution terms, the complete historical baseline, date/null/title field mappings, and release acceptance remain unresolved. No source or release approval is asserted.
- Four capture-independent mechanics tests run by default. Complete-capture tests accept an explicitly supplied retained capture directory:

  ```bash
  HIFLD_CBRS_MAP_PANELS_TEST_CAPTURE_DIR="$CAPTURE_DIR" \
    PYTHONPATH=src:tests:. uv run python -m unittest discover -s tests -p 'test_cbrs_map_panels_source.py'
  ```
