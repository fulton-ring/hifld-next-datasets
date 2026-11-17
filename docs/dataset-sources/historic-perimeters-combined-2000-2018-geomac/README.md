# NIFC InterAgency Fire Perimeter History draft

- Official source: NIFC ArcGIS Online feature service item
  `e02b85c0ea784ce7bd8add7ae3d293d0`
  (`InterAgencyFirePerimeterHistory_AllYears_View`, owner
  `WFMRDA_Authoritative`, organization `T4QMspbfLg3qTGWY`), layer 0
  `InteragencyFirePerimeterHistory`. The organization and owner are NIFC's;
  the Wildland Fire Management Research, Development & Application program
  data team maintains the layer. The layer `editingInfo` data edit date was
  2026-06-25 when this draft was written, so the publication is maintained.
- This replaces the retired GeoMAC 2000-2018 File Geodatabase capture. The
  GeoMAC archive is no longer read; the NIFC layer is the producer publication
  and covers the historical perimeters through the current record.
- Candidate retains the service's native EPSG:3857 polygon geometry and all 19
  declared attributes in producer order with their declared Esri types:
  `OBJECTID`, `IRWINID`, `FORID`, `INCIDENT`, `GIS_ACRES` (Single),
  `UNQE_FIRE_ID`, `DATE_CUR`, `FIRE_YEAR_INT` (SmallInteger), `UNIT_ID`,
  `POO_RESP_I`, `LOCAL_NUM`, `FEATURE_CA`, `MAP_METHOD`, `COMMENTS`, `GEO_ID`,
  `SOURCE`, `AGENCY`, `FIRE_YEAR`, `GlobalID`. No field is renamed, dropped or
  generated.
- The layer is captured as ordered-offset pages of the sorted OBJECTID roster.
  Count/id rosters and the layer `editingInfo` edit witness must match before
  and after every page, and the accepted pages must tile the complete roster
  without gaps or overlaps. The feature count is not pinned: the capture-time
  roster is the completeness evidence.
- Polygon rings are converted by the shared GDAL ESRIJSON path
  (`sources/esri_geometry.to_wkb`). Null, empty and OGC-invalid source polygons
  fail the capture closed; the replacement source produced none in the audit.
- Registered local job: `historic_geomac_perimeters_draft_refresh`; its monthly
  schedule remains STOPPED. The blocking completeness check requires the
  coherent roster and zero invalid geometries and keeps every acceptance flag
  false. The adapter rejects remote storage.
- Producer release date is unknown (the layer publishes no dated release; the
  `editingInfo` edit date is operational metadata). Historical HIFLD baseline
  equivalence, schema/ID contract, rights and publication acceptance remain
  unresolved; no source or release approval is asserted.
- Offline tests run by default with synthetic capture fixtures. To exercise a
  retained capture directory:

  ```bash
  PYTHONPATH=src:tests:. uv run python -m unittest discover -s tests -p 'test_historic_geomac_perimeters_source.py'
  ```
