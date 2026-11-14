# Agricultural Minerals Operations — blocked decision

Issue #7 remains **Needs decision / Potential breaking change**. The inventory describes USGS Minerals Information Team mines and plants active in 2003, replacing a June 2004 layer, and says the legacy data are not expected to be updated. The 2017 archive dates are not a current source release. A modern operations compilation would change the population. No maintained official machine source, exact DHS crosswalk match, rights, source release date, accepted baseline or versioned source/output contract has been established. No source, contract, compatibility or release approval is implied; no job is enabled.

Candidate records, not accepted source claims:

- DataLumos inventory V1: <https://www.datalumos.org/datalumos/project/238557/version/V1/view>. Crosswalk DOI <https://doi.org/10.3886/E241367V1> redirects to DataLumos; the workbook and hyperlink match remain unavailable (403), and the ICPSR alternative returned 404.
- Third-party ArcGIS mirror item <https://www.arcgis.com/home/item.html?id=1c9f0c8899bb4f47be90fd6723b926e1> and query endpoint <https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Agricultural_Minerals_Operations/FeatureServer/0/query>. The captured item is owned by `clibby007`, not verified as USGS or a delegated distributor; it is a fallback only.
- USGS NMIC information page <https://www.usgs.gov/centers/national-minerals-information-center> does not establish a machine source. Exploratory MRData endpoint <https://mrdata.usgs.gov/mineral-resources/aggr.html> returned 403 from this runner; it is unavailable here, not declared dead.
- Published HIFLD `v1.0.0` GeoParquet comparison candidate: <https://storage.googleapis.com/hifld-next-datasets-prod/hifld/agricultural-minerals-operations/agricultural-minerals-operations/v1.0.0/geoparquet/agricultural-minerals-operations.parquet>. It is not an accepted baseline: its IDs and CRS differ from the mirror, and the source relationship is unverified.

The unresolved human choice is whether to preserve the declared historical 2003 population or define a distinct modern successor, with explicit coverage, lineage, ID, column and CRS migration decisions. Maintainer acceptance and the shared #417 comparison/promotion gates remain required. Existing evidence is historical and retained in the archive; this docs-only cleanup ran no tests or source replay.

Public evidence: [issue #7 research update](https://github.com/fulton-ring/hifld-next-datasets/issues/7#issuecomment-6259969864), [independent research review](https://github.com/fulton-ring/hifld-next-datasets/issues/7#issuecomment-6259979183), [PR #422 review record](https://github.com/fulton-ring/hifld-next-datasets/pull/422#issuecomment-6259979357).

The 42 original tracked research documents, including `source.json`, logs, captured bodies, reports, candidate metadata and one-off audit scripts, are preserved at `/tmp/hifld-native-reconstruction/cleanup7/issue-7-removed-research-docs-4e23031c6638.tar.gz` (137,304 bytes; SHA-256 `a5474b6965f189ae086f44da075692c10e4661b78efdca18bd58c2dcd3a91ce0`). The sidecar manifest lists every member. Restore outside the worktree with:

```sh
mkdir -p /tmp/hifld-native-reconstruction/cleanup7/restore
tar -xzf /tmp/hifld-native-reconstruction/cleanup7/issue-7-removed-research-docs-4e23031c6638.tar.gz -C /tmp/hifld-native-reconstruction/cleanup7/restore
```
