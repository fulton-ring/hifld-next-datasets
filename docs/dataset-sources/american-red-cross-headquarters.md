# American Red Cross Headquarters draft

The candidate adapter reads the Red Cross ArcGIS FeatureServer layer captured
on 2026-10-19. It checks the declared 19-field schema, point geometry, native
EPSG:3857 declaration, complete count and ID rosters before and after paging,
every requested page URL, and the exact row IDs. It retains all nine response
bodies and their original receipts. GeoParquet retains typed source attributes
and native WKB coordinates; a row-keyed sidecar records that the source omits
the geometry member for `OBJECTID=226` (`ECODE=55004`). No geometry is borrowed
from the historical candidate, geocoded, projected, or dropped.

The old response set is an observed 2026-10-19 snapshot, not a currentness
claim. The publisher account's delegation remains unverified, the layer's
FY27 title conflicts with its active FY26 reversion notice, and the historical
baseline has unresolved identifier, geometry, name, address, and type
differences. No source, edition, baseline, contract, rights, coverage, release,
or production decision is accepted. The catalog's all-row quality gate is
expected to fail on the retained missing geometry. The monthly source-only
draft schedule is STOPPED.

The complete preserved response set is under
`/workspace/work/15-evidence/capture/` in this execution environment. It was
recovered from the pre-cleanup evidence commit
`8dcb840f6e3705f1301e39eb52c456b61db6c444`; the original full evidence archive
and research history remain available in that Git commit. The pipeline can
also capture the exact layer live when invoked with an empty capture directory,
but that path was not exercised for this snapshot replay.
