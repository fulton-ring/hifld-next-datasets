# Dynamic Buoys (USACE IENC)

The draft reads the complete `3UABUOYS` layer (MapServer layer 1) from USACE's official IENC `USACE_Buoys_Service`. The live layer description identifies the records as buoy positions supplied by the US Coast Guard and says the Army Geospatial Center updates the dataset weekly. This is an inland IENC chart layer with regional extent, not nationwide buoy coverage. The historical HIFLD archive describes the same product, but its DataLumos page was blocked by a Cloudflare challenge during this research; no byte-level historical comparison is claimed.

The current observed population is 11,770 ordered `OBJECTID` values (1–11,770), with nine declared fields and point geometry. Source coordinates are EPSG:4326; the service advertises EPSG:3857 by default, so extraction explicitly requests the source CRS. The candidate preserves all declared attributes and IDs, requests Z/M/true-curve returns, records complete response wire and decoded bodies with receipts, and quarantines schema, ID, count, or page drift.

HIFLD's historical record, source edition/date, comparison baseline, identifier/schema contract, coverage equivalence, rights, and release acceptance remain pending. The candidate and recurring refresh stay unapproved/stopped.
