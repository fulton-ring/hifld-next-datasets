# American Red Cross Divisions — rebuilt draft from the producer service

The repository source audit (issue #14, 2026-11-12) retired the retired PASDA
HIFLD mirror input that the old `assets/red_cross.py` ingest downloaded, and
identified the original producer publication: the American Red Cross publishes
this layer itself through its `NHQRedCrossGIS` publisher account (item
[8c6753c3c5814d05be65723cd4615d7c](https://www.arcgis.com/home/item.html?id=8c6753c3c5814d05be65723cd4615d7c),
organization `pGfbNJoYypmNq86F`), layer 1 `Master_RC_Geo_Division_2027` of
`https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/1`.
PR #758 had unregistered the mirror-fed pipeline; this PR rebuilds the ingest
from that service only, modeled on the registered sibling Red Cross HQ and ARC
Regions adapters.

Dated live capture evidence (2026-11-12): complete six-division population,
OBJECTIDs 1-6 with ring counts 297/258/64/139/535/382 and 473,821 vertices; the
features query body is 13,870,710 bytes, byte-count-identical to the preserved
native capture from the exact earlier audit. Frozen censuses: geometry
`9f80eb2d1843f5a6b9414c349a6615ca1d762fd4eb06497c56b49477482c448c`, attributes
`30259c13416de0bb7c5b99bef310d9da0d1c3e2ed3e7351fab18285f3c84fa61`; any source
change refuses staging and names the observed digest for review. Two local
end-to-end Dagster materializations with real captures passed the blocking
population check (run ids `da1379f0-4dc2-47a1-b33c-7f674cf72c29` and
`1f05c723-4ba8-48f3-acb4-d0c3758e645f`; candidate
`v0.0.0-research-20261112T124727Z`).

Still unaccepted gates, preserved by the adapter's false flags:
- Rights: the item metadata claims no explicit constraints; no independent
  rights verification. Attribution to the publisher account is unaccepted.
- Effective release: item/title claims FY27/July2026 while the active service
  notice says temporarily reverted to FY26/January2026; the effective six-row
  edition is unresolved.
- Historical baseline and schema/ID compatibility: source Double fields map to
  a proposed float64 and the unspecified-width OID to a proposed int64
  container encoding; `Shape__Area`/`Shape__Length` retain full names; GeoPackage
  and shapefile outputs remain unsupported pending reviewed mappings. OBJECTID6
  keeps its source self-intersection and is reported, never repaired; no
  geometry substitution is approved.
- DHS crosswalk (https://doi.org/10.3886/E241367V1): provider returned HTTP 403
  on earlier reads; the crosswalk remains unverified, not unmatched.

The 24 tracked research files from the earlier audit (153,408 original bytes)
remain archived at
`/tmp/hifld-native-reconstruction/cleanup14/issue-14-removed-research-docs-622709ac8978.tar.gz`
(SHA-256 `47bd7ff0a2d386d183f9941af266be51af5d6bfaa5af6a07cea3ba2476205e24`).
Evidence: [issue #14 exact audit](https://github.com/fulton-ring/hifld-next-datasets/issues/14#issuecomment-6265891690),
[PR #758 mirror unregistration](https://github.com/fulton-ring/hifld-next-datasets/pull/758),
[this PR](https://github.com/fulton-ring/hifld-next-datasets/pull/451).
Source, rights, baseline, contract, release, merge and closure approvals remain
false; publication stays gated on maintainer acceptance.
