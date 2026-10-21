# Coastlines (USACE IENC)

This draft captures the complete `COASTLINE_LINE` layer published by the USACE IENC Feature Classes service. The current layer contained 22,819 features in EPSG:4326 on 2026-10-21. IENC coverage follows inland navigation charts; this dataset is not a nationwide coastline inventory.

The candidate preserves the source attributes, `OBJECTID`, `Feature_ID`, and native polyline coordinates in GeoParquet. It does not reproject or repair geometry. Candidate generation and catalog quality passed, and immutable replay reused the retained snapshot. Release approval, source succession, historical HIFLD equivalence, source edition/date, rights, schema and ID contracts, and coverage acceptance remain pending. Production refresh is stopped.

The current public BTS item describes USACE/AGC bi-monthly IENC updates and links the official feature-class service and master geodatabase. The prior BTS item returned anonymous-access denial; succession between those item records is unaccepted. This draft records the current item as source-discovery evidence without claiming a dated layer release or approved historical continuity. See [source metadata](source.json) and [local run evidence](local-materialization.json).
