# Dam Lines (USACE IENC)

The draft source is the `DAM_LINE` polyline layer (ID 35) in the USACE IENC Feature Classes MapServer. Its complete directly observed layer contains 40 features in EPSG:4326 within the inland chart extent shown in the retained source audit. This is a regional chart-service layer, not a nationwide dam inventory.

The source captures all native attributes and `OBJECTID` values, ordered XY paths, count and ID snapshots before and after paging, and response bodies, wire bodies, and request receipts. Candidate generation retains the source CRS and path coordinates without reprojection or geometry repair. GeoParquet is the only candidate format; no display or lossy output policy is accepted.

USDOT BTS's current public IENC PDF item links the USACE Feature Classes service and master geodatabase. A prior HIFLD draft cited a different BTS item and a DataLumos archive. The archive's exact crosswalk row and relationship to this service layer have not been verified, so historical continuity and layer edition remain pending. Rights, source acceptance, baseline comparison, schema/ID/coverage contracts, and release approval remain false. The registered refresh schedule is stopped.
