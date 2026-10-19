# American Red Cross Divisions — external source decisions required

The exact divisions card was audited independently of the county sibling. The complete published candidate and live master division layer both have **six rows**, OBJECTIDs 1–6. Every existing diagnostic attribute pair matches, including original IDs, division codes/names, States and area values. This establishes a strong content relationship; it does not establish the publisher’s delegation or the actual effective release. No working dataset pipeline is claimed here.

The public item `8c6753c3c5814d05be65723cd4615d7c`, owned by `NHQRedCrossGIS` in organization `pGfbNJoYypmNq86F`, claims Red Cross production. Layer 1 is `Master_RC_Geo_Division_2027`. Fresh anonymous official Red Cross mapping/fire-risk pages describe different products. The official SSO success page links RC View, which redirects to a Volunteer Connection login; that observed path does not connect this divisions account or layer. Same-portal organization/account branding is retained as a claim. No independent delegation was located in the bounded paths checked; this does not prove none exists.

The item says FY27/July2026 and the service also claims July2026 updates, while its prominent active notice says it is temporarily reverted to FY26/January2026. Neither fetch time nor an edit timestamp resolves the actual effective six-row release. DOI/DataLumos both return provider HTTP403: the exact historical crosswalk remains **unverified/inaccessible**, not unmatched. Working catalog, GCS and ArcGIS reads are recorded separately from one failed CONNECT403 lead at `maps.redcross.org`.

The user authorizes draft pipeline implementation and local testing before contract acceptance. This is not a missing-contract implementation stop: the external prerequisites are producer distribution/delegation and effective release. The existing PASDA `conus_clipped` archive mirror is not relabeled as a maintained source and no archive-refresh replacement is implemented.

## Complete comparison and preservation

The full 13,870,710-byte source ESRIJSON preserves nine declared fields, all six IDs, native EPSG:3857 rings and all coordinate values/multiplicities. Census/IDs/schema/edit metadata match before and after extraction; all six rows are returned without transfer-limit/error and a terminal query is empty. The complete 4,765,923-byte published Parquet remains an unapproved candidate.

Five geometries are ordered/normalized/topologically exact. All six original ring-coordinate multisets match without a coordinate tolerance. OBJECTID5 has identical 535 rings/86,111 coordinates but published nested-shell invalidity versus valid ESRIJSON grouping; OBJECTID6 retains a self-intersection in both. No geometry is repaired. All eight existing diagnostic attribute pairs match. The source additionally contains `Division_DCODE`; full source `Shape__Area`/`Shape__Length` differ from truncated published names, source `Acres`/`SQ_MI` are declared doubles while published types are int64, and published `id`/`bbox` are absent from the source. These require review, not silent schema changes.

Full-row evidence and receipt pins are committed. Complete original source and baseline bytes remain in `/workspace/work/hifld-coordinator/14`; re-auditing historical geometry requires those matching preserved captures, rather than a changing live refetch.

```bash
PYTHONPATH=src uv run python docs/dataset-sources/american-red-cross-divisions/audit_captures.py /workspace/work/hifld-coordinator/14 --output /tmp/divisions-comparison.json
PYTHONPATH=src uv run python docs/dataset-sources/american-red-cross-divisions/audit_ring_representation.py /workspace/work/hifld-coordinator/14 --output /tmp/divisions-rings.json
```

## Required decisions

1. Official Red Cross publication/delegation or producer confirmation for this exact account, item and division layer.
2. Actual effective division release, resolving simultaneous FY26/FY27 claims. Alternatively, explicitly authorize a specifically named preserved historical snapshot candidate with its unresolved lineage/vintage stated; no such authorization is assumed and no current archive-refresh feed is substituted.
3. After those are established, approve the complete baseline/identifier/schema/quality compatibility plan, including source physical types and invalid geometry; preserve all original inputs/IDs and historical releases.
4. Accessible exact DHS/archive crosswalk evidence; current403 responses do not establish a match or mismatch.

No materialization/full-suite/CI success is claimed for this documentation/audit change. No source, baseline, contract, release, merge, issue closure or production publication is approved. Requested workflow README paths are absent at this branch’s reviewed base and returned404 at the open PR418 head; exact attempted reads are recorded without modifying that PR.
