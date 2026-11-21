# Nursing Homes (nursing-homes)

## Current source (issue #1056, 2026-11-18)

The maintainer selected CMS's national Provider Information dataset for this
inventory layer. The pipeline reads dataset `4pq5-n9py` from
<https://data.cms.gov/provider-data/dataset/4pq5-n9py> through the producer's
metadata and datastore APIs:

| URL | Publisher | Last update |
| --- | --- | --- |
| <https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/4pq5-n9py?show-reference-ids> | Centers for Medicare & Medicaid Services (CMS) | metadata modified 2026-10-01, released 2026-10-28 |
| <https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0> | Centers for Medicare & Medicaid Services (CMS) | same release; count and CCN roster witness |
| <https://data.cms.gov/provider-data/sites/default/files/resources/0970c657767e40f8521187d02ba8d0ed_1792181744/NH_ProviderInfo_Oct2026.csv> | Centers for Medicare & Medicaid Services (CMS) | `Processing Date` 2026-10-01; next update 2026-12-02 |

The adapter selects the CSV download URL from the current metadata
distribution, so the quarterly edition is read without a code pin. It captures
the producer datastore count and the exact native CCN roster before and after
the CSV, and requires all three to agree.

Current capture: **14,686 rows**, one per CMS Certification Number (CCN), with
all 102 source fields retained as lexical strings. Observed state and territory
codes are the 50 states plus DC, Puerto Rico and Guam.

CMS supplies a coordinate pair for every row (14,686 of 14,686, 100%), so no
Census geocoding is performed. The coordinates are preserved as a proposed
EPSG:4326 point geometry; the source datum and positional accuracy are
unverified. 808 rows carry CMS geocoding footnote 22, which is retained in the
native `Geocoding Footnote` field.

## Coverage limit

The dataset covers **Medicare- and Medicaid-certified nursing-home providers
only**. It is not the broader licensed nursing-home / assisted-care inventory
described by the historical ORNL layer, and it does not claim HIFLD identifier
or attribute parity. The CCN identifies a provider, not a timeless physical
site.

## Historical source blocker

The producer named by the DHS crosswalk is Oak Ridge National Laboratory
(ORNL), Department of Energy. The last HIFLD Open release is dated 2024-06-26.
The crosswalk records the layer as migrated to the DHS GII Portal, which
requires a DHS-authorized Login.gov account, and every historical public channel
was confirmed dead on 2026-11-12: the GeoPlatform FeatureServer, the old portal
item, the HIFLD Open hub domains and ORNL's own download channels.

The FEMA RAPT "Nursing Homes RAPT" feature service is owned by a non-producing
agency, and DataLumos, HSDL, the Data Rescue Project/HIFLD Next copies and PASDA
mirrors are archives. They remain excluded under project source policy.

The maintainer decision on #1056 accepts CMS as a changed, narrower contract for
this layer; it does not establish ORNL population equivalence. All runtime
approval flags remain false and the refresh schedule stays stopped. No
production publication is implied by merging the adapter.
