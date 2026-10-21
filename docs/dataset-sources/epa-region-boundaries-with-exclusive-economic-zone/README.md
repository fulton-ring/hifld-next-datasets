# EPA Region Boundaries with Exclusive Economic Zone (#76)

This is a separate draft dataset from #75, EPA Region Boundaries Exclusive
Economic Zone (EEZ). Both point to EPA `OEI/EPA_Locations/MapServer/4`, but #76
uses its own identity, raw-capture namespace, construction state, staging
partition, catalog objects, and stopped refresh schedule. Evidence from #75 is
not used as #76 acquisition evidence.

The adapter retains the producer's complete feature response and original
attribute/geometry declarations in native NAD83 (EPSG:4269) GeoParquet. It
requests true curves and omits unsupported Z/M output switches because the
producer layer does not advertise those dimensions. The staged candidate is
not an accepted release: source identity, edition, coverage compatibility,
rights, historical schema compatibility, and publication approval remain
pending.

The source capture and registered lifecycle verification are being recorded
separately for this draft before it is offered for review. No historical
baseline or archive comparison is claimed here.
