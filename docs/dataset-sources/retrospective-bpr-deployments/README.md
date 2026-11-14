# Retrospective BPR Deployments draft

- Inventory entry `retrospective-bpr-deployments`; the HIFLD inventory's "US Army Corps of
  Engineers" agency attribution is contradicted by the entry's own description and tags
  (NOAA/NCEI/DART/BPR/PMEL/NDBC): the producer is NOAA.
- Producer publication: NOAA National Centers for Environmental Information. NOAA's
  Natural Hazards Viewer (https://www.ncei.noaa.gov/maps/hazards/?layers=4) embeds webmap
  item `5cb326668189408e8c4c20ddd92aa2ed`, whose `retrospective-dart-deployments` layer
  delegates to the NOAA GeoPlatform hosted Feature Service item
  `764d3a99a4f8440ea05522815b5f5e19` (owner `ncei_noaa`, org `C8EMgrsFcRFL6LrL`), layer 1
  "Retrospective BPR Deployments". Program page:
  https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure
- The DataLumos project 240598 record and the DHS crosswalk (DOI 10.3886/E241367V1) are
  third-party rescue/mirror context only and are never pipeline inputs under the current
  source policy.
- Observed live 2026-11-13: 524 point features, contiguous OBJECTID 1572..2095, 20 declared
  fields, native EPSG:3857, maxRecordCount 2000, layer editingInfo lastEditDate
  1794564219406 (2026-11-13T10:03:39Z, operational metadata, not a release date).
- Local job: `retrospective_bpr_draft_refresh`; schedule `retrospective_bpr_draft_refresh_schedule`
  remains STOPPED.
- The sibling "Current DART Deployments" pipeline (merged, issue 49) documents the same
  delegation chain for layer 0 of the same service; this module covers layer 1 only.
- Source authority, historical baseline, identifier/schema/physical-type mappings, output
  formats and release decisions remain unresolved; no production write or acceptance is implied.
- All acquisition approval flags are false; the shared publish flow must refuse promotion
  until a maintainer accepts the proposed contract.
