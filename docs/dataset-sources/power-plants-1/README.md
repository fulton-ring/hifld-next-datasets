# Power Plants (power-plants-1)

## Current source (issue #1051, 2026-11-17)

The pipeline reads EIA's monthly EIA-860M generator inventory:
<https://www.eia.gov/electricity/data/eia860m/>. It selects the newest active
completed-month workbook link from that page rather than pinning a month. The
current capture selected the September 2026 workbook
<https://www.eia.gov/electricity/data/eia860m/xls/september_generator2026.xlsx>
(server `Last-Modified: 2026-10-23T15:32:18Z`). EIA publishes a new month
about monthly; the page also lists future months inside HTML comments, which
the selector ignores.

The adapter retains the `Operating` and `Operating_PR` sheets, including
Puerto Rico and the source operating, standby and out-of-service statuses. It
aggregates the retained generator rows by native EIA `Plant ID`:

- plant names, states and counties as sorted distinct lists;
- generator count;
- total nameplate and net summer capacity, summed from numeric native cells;
- the technology and energy source code with the largest summed nameplate
  capacity, ties broken lexicographically;
- native latitude and longitude only when every retained generator at the plant
  has the same non-blank numeric pair. Coordinates are never averaged.

The producer datum is not established, so the output is a **nonspatial**
Parquet table: coordinates stay lexical attributes and no geometry or CRS is
assigned. The schedule is stopped and every acceptance, rights, coverage,
schema, identifier and release flag is false. Equivalence to the historical
HIFLD population and its identifiers is unverified.

The generator-level table is published separately as
`eia-860m-electricity-inventory/existing-generators`; this dataset does not
duplicate it.

Current capture (September 2026): **28,712 generators** and **14,864 plants**,
including 228 Puerto Rico generator rows. Total native nameplate capacity is
1,423,491.7 MW. All 14,864 plant coordinate pairs are unanimous in the source.

Inputs fetched:

| URL | Publisher | Last update |
| --- | --- | --- |
| <https://www.eia.gov/electricity/data/eia860m/> | U.S. Energy Information Administration | release page |
| <https://www.eia.gov/electricity/data/eia860m/xls/september_generator2026.xlsx> | U.S. Energy Information Administration | 2026-10-23 |
| <https://www.eia.gov/survey/form/eia_860m/instructions.pdf> | U.S. Energy Information Administration | monthly form instructions |
| <https://www.eia.gov/survey/form/eia_860/instructions.pdf> | U.S. Energy Information Administration | annual form instructions |
| <https://www.eia.gov/about/copyrights_reuse.php> | U.S. Energy Information Administration | reuse policy |

## Historical source (retired)

The previous pipeline read EIA's frozen `eia.gov/maps/map_data`
`PowerPlants_US_EIA.zip` (August 2021 data, server `Last-Modified:
2022-01-31`). EIA no longer updates that zip, so it is no longer fetched and
the pinned snapshot code is removed. The Energy Atlas replacement layer is
token-gated; the publicly queryable Esri copy is owned by a non-EIA portal and
is not a pipeline input.
