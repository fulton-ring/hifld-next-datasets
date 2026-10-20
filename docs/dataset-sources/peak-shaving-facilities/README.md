# Peak Shaving Facilities source finding

The inventory identifies DOE/ORNL as publisher, but the DataLumos historical archive and DHS crosswalk could not be inspected in this run because both returned HTTP 403 challenge pages. ArcGIS Online search found a public HIFLD-labelled item owned by `HSEMA_PPD`; its description says it is HIFLD data, while its saved FeatureServer URL now returns HTTP 400 `Invalid URL`. This is historical mirror evidence, not a maintained DOE/ORNL feed.

A similarly titled `US Peak Shaving Facilities` service is published by `maps_ft`, a third-party ArcGIS account. Its description counts 60 peak-shaver and 41 satellite facilities but does not identify a DOE/ORNL delegation or a source release. It remains a separately labelled fallback candidate and is not suitable for a source adapter without lineage evidence.

No maintained producer source, release date, exact historical match, complete extraction, or schema/ID contract is established. Do not build an archive-refresh pipeline from these copies. Reopen source research if DOE/ORNL supplies a maintained feed or confirms the third-party service's lineage and release relationship.
