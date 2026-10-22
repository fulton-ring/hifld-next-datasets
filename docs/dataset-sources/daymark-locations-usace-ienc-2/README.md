# Daymark Locations (USACE IENC)

The inventory identifies the historical DataLumos V2 package. A current public BTS IENC catalog record links the USACE IENC Feature Classes service and master geodatabase; the service identifies USACE IENC as copyright holder. This draft queries the service's `DAYMARK_POINT` MapServer layer 9 directly. That establishes a traceable current service source for the candidate, not continuity with the DataLumos/HIFLD records or approval of a successor.

The observed layer is a regional inland-chart dataset, not a nationwide daymark inventory. Its candidate retains the complete observed `OBJECTID` roster, all declared attributes including `Feature_ID`, and native point coordinates. Every feature query requests `returnZ=true` and `returnM=true`; the adapter validates the actual returned coordinate dimensions and fails closed on Z or M ordinates. Source release date, historical baseline, schema/ID contract, coverage acceptance, rights, and release remain unaccepted. The refresh schedule is stopped.

The archived inventory modification date is not treated as a current source release date. No DataLumos crosswalk match, historical equivalence, field compatibility, or accepted coverage claim is inferred from the shared subject or current service response.
