# Electric Retail Service Territories (electric-retail-service-territories)

## Current source (issue #1057, 2026-11-18)

The pipeline builds a county-level utility service-territory draft from the
current EIA-861 annual utility-by-county release joined to the current Census
TIGER/Line county equivalents. It does not read the frozen NBAM/HIFLD ArcGIS
copy or the ORNL login-gated polygon product.

The adapter selects the newest active `f861YYYY.zip` link on
<https://www.eia.gov/electricity/data/eia861/>. The current capture selected the
2025 data year:

<https://www.eia.gov/electricity/data/eia861/zip/f8612025.zip>
(server `Last-Modified: 2026-10-07T13:48:05Z`).

It reads `Service_Territory_2025.xlsx` for utility-by-county rows and
`Frame_2025.xlsx` for utility ownership and service-territory participation.
It also discovers the newest TIGER annual directory and captures the national
county ZIP. The current capture selected TIGER/Line 2026:

<https://www2.census.gov/geo/tiger/TIGER2026/COUNTY/tl_2026_us_county.zip>
(server `Last-Modified: 2026-10-07T01:45:57Z`).

Each EIA utility-county row is joined by state and normalized county name to a
current TIGER county equivalent, then the joined counties are dissolved into
one feature per utility-state pair. The output keeps the native EIA utility
number, utility name, ownership type, state and data year, plus the number of
source county rows dissolved into the feature.

**The boundaries are county-level approximations.** They are not utility-owned
legal service boundaries, franchise areas or engineering limits. Utilities that
operate in more than one state have one feature per state.

The source uses legacy county names for Connecticut and Alaska that no longer
match current TIGER county equivalents. The adapter carries a small explicit
crosswalk for those names (and Bedford City, Virginia, which merged into Bedford
County), documented in `source.json`. Connecticut legacy counties map to every
current planning region they overlap, so those features are coarser than other
states.

Current capture: **11,893 EIA utility-county rows**, **2,909 utilities**,
**3,101 utility-state features**, **3,232 distinct current counties** and
**56 states/territories**. One `Not Applicable` county sentinel row is skipped.

The schedule is stopped. Every source, rights, coverage, schema, identifier,
mapping and release flag is false. Historical HIFLD/ORNL equivalence is
unverified.

Inputs fetched:

| URL | Publisher | Last update |
| --- | --- | --- |
| <https://www.eia.gov/electricity/data/eia861/> | U.S. Energy Information Administration | release page |
| <https://www.eia.gov/electricity/data/eia861/zip/f8612025.zip> | U.S. Energy Information Administration | 2026-10-07 |
| <https://www2.census.gov/geo/tiger/> | U.S. Census Bureau, Geography Division | annual directory |
| <https://www2.census.gov/geo/tiger/TIGER2026/COUNTY/> | U.S. Census Bureau, Geography Division | product directory |
| <https://www2.census.gov/geo/tiger/TIGER2026/COUNTY/tl_2026_us_county.zip> | U.S. Census Bureau, Geography Division | 2026-10-07 |
| <https://www.eia.gov/about/copyrights_reuse.php> | U.S. Energy Information Administration | reuse policy |

## Historical source (excluded)

The previous module preserved a frozen NTIA NBAM ArcGIS copy of the HIFLD
layer. Its item said it was downloaded from HIFLD on 2025-08-21 and would not be
updated, so it is not an original-producer source and is no longer fetched.
The ORNL I-Fac product remains login-gated and is not a pipeline input.
