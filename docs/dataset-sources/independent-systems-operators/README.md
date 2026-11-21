# Independent Systems Operators source record

Inventory entry `independent-systems-operators` (HIFLD Open Inventory 2025-12-11 row): electric power
Independent System Operators, agency Department of Energy, publisher not specified, DataLumos archive
project 239294, inventory date issued 2021-11-08 and archive inventory modification 2022-12-15 (the
latter is a collection edit date, not a source release date).

## DHS crosswalk match (dated)

The DHS HIFLD Open crosswalk row for "Independent Systems Operators" was read from the saved
crosswalk copy (https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx,
the DHS-published geoplatform item `41b4ff18bb094ba996dec75916de3bf3` is itself dead: HTTP 200 shell /
CONT_0001 on both the item and its `/data` on 2026-11-12). The row states: Status `Migrated`,
New Hosting Location `GII Portal`, DUA Required `No`, GII Access Required `Yes`, Agency
`Oak Ridge National Laboratory (ORNL)`, External Landing Page empty, Open REST Service
`https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Independent_System_Operator/FeatureServer`,
Open ID `50f80920d36e435d9a34db2bd0fd3ad8`, Old/New Secure IDs "Please see GII".
The source inventory row therefore matches the crosswalk exactly (title and HIFLD lineage), and DHS
records the layer as migrated behind DHS GII access control, not to any public feed.

## Producer identified

The layer is part of the multi-laboratory HSIP "Electric Foundation Energy Data" product. The HIFLD
record credits "Oak Ridge National Laboratory (ORNL), Los Alamos National Laboratory (LANL), Idaho
National Laboratory (INL), National Geospatial-Intelligence Agency (NGA) Homeland Security
Infrastructure Program (HSIP) Team", states "Data source: Oak Ridge National Laboratory. Data and
details are hosted by HIFLD as received from the data provider", and labels use limitations
"None (Public Use)". The DOE poster "Creating Foundation Electric Energy Infrastructure Data from
Open-Sources" (Singh, Schmidt, Visarraga, Elliott; energy.gov, 2023-01) lists Independent System
Operators among the government-owned foundation layers compiled from open sources (the layer's own
SOURCE value is "FERC 714, EIA 860, EIA 861, TIGER/Line Shapefiles - U.S. Census") and "distributed
without any restrictions through the Homeland Infrastructure Foundation-Level Data (HIFLD)
platform". FERC defined ISOs but publishes no ISO/RTO footprint GIS file (PDF maps only), so ORNL's
HSIP compilation through HIFLD is the producer publication of record.

## Historical vintage facts (research context, no accepted baseline)

The final HIFLD Open release was issued 2021-11-08 ("updated attribution using best available
sources"); the published HIFLD Next v1.0.0 rescue snapshot of the same layer (comparison copy only,
never an input) reports 7 features (CAISO, ERCOT, ISO-NE, MISO, NYISO, PJM, SPP), EPSG:4326
polygons, and row values SOURCEDATE 2020-06-24, VAL_DATE 2021-05-28, YEAR 2019, PEAK_LOAD
24324-151499 MW, string ID values (13434, 13501, 14725, 2775, 56669, 5723, 59504) with ZIP strings
carrying leading zeros. Columns of the final release: OBJECTID, ID(str 5), NAME(60), ADDRESS(26),
CITY(11), STATE(2), ZIP(5), TELEPHONE(14), COUNTRY(3), NAICS_CODE(4), NAICS_DESC(56), SOURCE(63),
SOURCEDATE(date), VAL_METHOD(5), VAL_DATE(date), WEBSITE(27), PEAK_LOAD(double), AVG_LOAD(double),
YEAR(4), GlobalID(38), SHAPE_Leng, Shape_Length, Shape_Area. Proposed contract for any future
rebuild: preserve every column, name, type and value verbatim (string IDs and ZIP leading zeros, no
substitute row IDs), native EPSG:4326 geometry, and record the producer release date separately from
portal edit/fetch dates. Completeness (7 features) has not been verified against any allowed
producer source; it rests on the rescue snapshot and mirrors listed below.

## Dated anonymous checks (2026-11-12, this evidence only)

- DHS crosswalk row read (copy above); GeoPlatform crosswalk item `41b4ff18bb094ba996dec75916de3bf3`
  and its `/data` return CONT_0001 "Item does not exist or is inaccessible".
- Open REST service `services1.arcgis.com/Hp6G80Pky0om7QvQ/.../Independent_System_Operator/FeatureServer?f=pjson`
  → HTTP 400 "Invalid URL" (the HIFLD org is gone); geoplatform item `50f80920d36e435d9a34db2bd0fd3ad8`
  and `www.arcgis.com/.../items/50f80920d36e435d9a34db2bd0fd3ad8` → CONT_0001;
  `hifld-geoplatform.hub.arcgis.com/datasets/50f80920d36e435d9a34db2bd0fd3ad8_0/about` and the hub
  DCAT feed → CONT_0001; `https://gii.dhs.gov/HIFLD` 301-redirects to that dead hub.
- EIA U.S. Energy Atlas v3 dataset API (`https://atlas.eia.gov/api/v3/datasets`, public, no token)
  returns an empty `data` array for slugs `independent-systems-operators`,
  `independent-system-operator`, `iso`, `rto`, and `regional-transmission-organizations`; a
  `q=operator` search returns no EIA-owned ISO layer.
- ANL Geospatial Energy Mapper (https://gem.anl.gov, DOE Office of Electricity, DE-OE000316): the
  full public catalog (369 layers, fetched 2026-11-12 through the tool's own `catalog:fetch`
  websocket API at `wss://gem.anl.gov/socket/websocket`) contains no independent-system-operator
  layer (closest HIFLD copies are `electric_substation_hifld` and the mislabeled
  `electric_planning_area_hifld` control-areas copy); direct `download.zip` probes for
  `independent_system_operator(_s)`, `iso`, `iso_rto`, `iso_boundary` slugs return 404.
- ORNL/DOE Open Energy Hub (https://openenergyhub.ornl.gov, API
  `/api/explore/v2.1/catalog/datasets`): 183 datasets enumerated; no ISO dataset; sibling HSIP
  records `control-areas` and `electric-retail-service-territories` are marked "Dataset does not
  exist anymore", and the live NERC Reliability Coordinators record is NERC's own feed, not HSIP.
- ORNL distributes no public copy: OSTI Data Explorer API title search for "independent system
  operators" returns an empty list; `ornl.gov` only describes ORNL's HIFLD contributions.
- ArcGIS Online title search for "Independent System Operators" (61 hits) shows no item owned by a
  producing-agency account; owners are third parties (NRDC `alevin_NRDCINC1`, DC HSEMA, FEMA GDAC
  Louisiana subset, PNNL, private users).
- Data.gov CKAN `package_search` returns 404 anonymously (as in prior sibling runs).
- DataLumos project 239294 (DOI 10.3886/E239294V1) returns HTTP 403 (Cloudflare challenge) to
  anonymous requests; DataLumos is a banned input in any case.

## Mirrors identified, banned as inputs

PASDA HIFLD_FEMA MapServer layer 53 (`minemaps.pasda.psu.edu`, banned host); NASA NCCS 2019 HIFLD
mirror `maps.nccs.nasa.gov/.../hifld_open/energy/MapServer` layer 28 "Downloaded from the HILFD
archives May 2019" (DNS-unreachable from this environment; 2019 vintage); DataLumos 239294; NRDC
AGOL item `482e7e9d9dd3416ab7234113f9a8d494` / FeatureServer `OmhjjQKPQ4s7vrAK`; HSEMA item
`344d3baca6734bbebb7e3623cf18d451`; ourgridfuture.org RTO download citing the HIFLD 2021-11-08
release; HIFLD Next's own published v1.0.0 (rescue snapshot; usable only as a comparison baseline).
None may be used as a pipeline input or "fallback" under the current source policy.

## Blocker (Needs decision)

No maintained public producer feed exists. DHS migrated the layer to the GII (HIFLD Secure) with
"GII Access Required: Yes" and no external landing page, so the only faithful sources are:
(a) a DHS GII/HIFLD Secure account with access to the layer (credentials/access grant the factory
cannot obtain itself), (b) an explicit maintainer decision accepting some other distribution as an
officially delegated one (none is currently claimed by its operator to be that), or (c) a
no-rebuild/retire decision. Until then no adapter, fetch, tests or staging candidate is built, and
the published inventory must not be repopulated from a mirror.
