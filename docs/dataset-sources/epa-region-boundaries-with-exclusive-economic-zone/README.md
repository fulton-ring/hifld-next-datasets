# EPA Region Boundaries with Exclusive Economic Zone (#76)

This is a separate draft dataset from #75, EPA Region Boundaries Exclusive
Economic Zone (EEZ). Both point to EPA `OEI/EPA_Locations/MapServer/4`, but #76
uses its own identity, raw-capture namespace, construction state, staging
partition, catalog objects, and stopped refresh schedule. Evidence from #75 is
not used as #76 acquisition evidence.

The adapter retains the producer's complete feature response and original
attribute/geometry declarations in native NAD83 (EPSG:4269) GeoParquet. It
requests true curves and omits unsupported Z/M output switches because the
producer layer does not advertise those dimensions. The staged candidate is
not an accepted release: source identity, edition, coverage compatibility,
rights, historical schema compatibility, and publication approval remain
pending.

The complete #76 capture was acquired independently from the producer on
2026-10-21. The page request used `objectIds=1,2,3,4,5,6,7,8,9,10`,
`outFields=*`, `returnGeometry=true`, `returnTrueCurves=true`, and
`outSR=4269`. Its response was HTTP 200, 15,388,991 bytes, SHA-256
`31e9fe39c7fc0d2ca665092e5dceb4641da83772d16e3237ca094ea505842845`, observed
at `2026-10-21T21:11:13.541775+00:00`. Although the page hash matches the
separate #75 response, this is a new HTTP response with its own #76 receipt;
no #75 receipt or capture file was reused.

The registered layer metadata and before/after count and ID witnesses agreed:
10 features, IDs 1–10, the five nongeometry attributes
`FIRST_CONTACT`, `Shape_Length`, `Shape_Area`, `OBJECTID`, and `EPAREGION`, plus
the producer `Shape` geometry field. Decoding preserved 22 rings and 377,337
XY vertices in EPSG:4269; all ten geometries are present and valid. The EPA
license page was also captured. Bodies and response receipts are retained under
`/workspace/work/76-evidence/capture-input/`; the source candidate and Dagster
database are under the same isolated #76 evidence root. The stored source
manifest pins processing code commit
`ad38ef6d6cb33f575651c1edbbe1163d99ead724`.

## Verification

The focused `tests.test_epa_eez` run passed 13 tests; the opt-in full-live
capture test remained skipped because the complete capture is exercised through
the registered asset runs below.

- Registered #76 source run `b61a4164-26ab-48c6-b68b-6843bda33772` succeeded
  with 10 features and the blocking full-geometry check passing. Its output
  candidate is `v0.0.0-draft-epa-eez-3ac0d1467ecd3e9b`.
- Registered `publish/catalog` run `187bc98b-3705-40ff-9db5-b6d2bc90d87a`
  succeeded; the all-row `quality_manifest` check passed with 10 features,
  EPSG:4269, and zero null or invalid geometries.
- Registered source replay `83919f46-af7e-4d03-bf49-c47fc759fc9a` used only
  the retained #76 bodies and receipts, succeeded with `reused: true`, and
  passed the full-geometry check after catalog metadata had been added.
- Independent full typed readback at
  `/workspace/work/76-evidence/native-parity.json` matched all ten source
  rows, field names, Arrow types, original field metadata, IDs, region labels,
  WKB geometry and ordered ring coordinates; the GeoParquet CRS is EPSG:4269.
- The registered gate proof at
  `/workspace/work/76-evidence/gates/registered-gate-proof.json` used two
  independent copies of this cataloged candidate and a populated three-object
  synthetic published fixture. A copy with only catalog `quality_check_passed`
  set false failed the blocking check before verify/promote
  (`f30b1e14-9aca-4c90-bd36-71d66e866fee`). The second copy omitted quality
  checks solely to isolate exact approval refusal; `verify` failed for the
  empty registry and `promote` did not start
  (`e16ae058-f926-4154-b4ce-6f70a4f942f4`). The original candidate and
  synthetic published fixture inventories remained unchanged.

These checks establish draft pipeline behavior only. No historical baseline
or archive comparison is claimed, and source identity, edition, coverage,
rights, compatibility, acceptance and publication remain pending.
