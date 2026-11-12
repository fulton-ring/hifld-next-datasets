# NIFC GeoMAC Historic Perimeters Combined 2000-2018 draft

- Official source: NIFC ArcGIS Online File Geodatabase item
  `5b3ff19978be49208d41a9d9a461ecfb`
  (owner `NIFC_Admin`, organization `T4QMspbfLg3qTGWY`),
  `Historic_Geomac_Perimeters_All_Years_2000_2018.gdb.zip`, 625,319,314 bytes,
  item created 2019-11-14, modified 2022-08-03. The identical archive was
  captured 2026-11-09 and re-captured live on 2026-11-12 with measured sha256
  `c5b65a2d3f1f6a55c7ec867c669282e628aa64c9c3a54a5a8b7b1db1dffdf6d1`
  (byte-identical across captures). The source publishes no hash, so this pin
  is a measured identity, not a source-published digest. The download endpoint
  redirects (302) to a short-lived signed `www.arcgis.com/itemdata` URL; the
  adapter follows redirects while pinning the requested item URL and never
  retains the signed query.
- Candidate is the combined layer `US_HIST_FIRE_PERIMTRS_2000_2018_DD83` only:
  23,776 MultiPolygon features, native NAD83 (EPSG:4269), OBJECTID contiguous
  unique 1..23,776, 22 producer-order attribute fields, 13,400,285 vertices,
  observed bounds equal the declared layer bounds
  ([-178.8414555557593, 3.3864261395352173, -65.33316018923853,
  70.15916168068225], including the known ~3.386° south artifact). The 19
  per-year layers (`US_HIST_FIRE_PERIM_YYYY_DD83`, older GeoMAC schema with
  latest/active flags, census 75,603 features) are receipted but not selected;
  the combined layer is an undocumented reduction of that fuller history.
- Dirty producer values are preserved verbatim and unpublishable values are
  never repaired: 881 self-intersecting native geometries (kept unrepaired,
  release blocked), `datecurrent` up to year 3013, `perimeterdatetime` down to
  year 1712, one `agency` value containing CR/LF, 4,231 duplicated
  `uniquefireidentifier` values.
- Corroboration, not merger: the same-org hosted Feature Service item
  `ef25d7e8c9f3499ba9e3d8e09606e488` (layer `US_HIST_FIRE_PERIMTRS_2000_2018_DD83`)
  reports the same 23,776 count; sampled `outSR=4269` spot-checks match the
  retained native rows within ~3e-12 degrees. That tolerance documents server
  reprojection noise; server-reprojected output is never labeled bitwise-native
  NAD83. The service spells computed fields `shape__Length`/`shape__Area`
  (double underscore) versus the gdb's `shape_Length`/`shape_Area`; the two
  representations are corroborated and never merged.
- Registered local job: `historic_geomac_perimeters_draft_refresh`; its monthly
  schedule remains STOPPED. The blocking completeness check pins the 23,776
  feature population, unique OBJECTID coverage, zero null geometries and the
  881-invalid-geometry census, and keeps `release_blocked` while those invalid
  producer geometries are preserved unrepaired. The adapter rejects remote
  storage and keeps every approval flag false.
- Producer release date is unknown (portal modified times are not release
  dates). Historical HIFLD baseline equivalence, schema/ID contract, rights and
  publication acceptance remain unresolved; no source or release approval is
  asserted.
- Offline mechanics tests run by default with mocked pyogrio/zip inputs.
  Complete-capture tests accept an explicitly supplied retained capture
  directory containing `{name}.body`/`{name}.receipt.json` for the roster in
  `src/dagster_hifld/sources/historic_geomac_perimeters.py`:

  ```bash
  HIFLD_GEOMAC_PERIMETERS_TEST_CAPTURE_DIR="$CAPTURE_DIR" \
    PYTHONPATH=src:tests:. uv run python -m unittest discover -s tests -p 'test_historic_geomac_perimeters_source.py'
  ```
