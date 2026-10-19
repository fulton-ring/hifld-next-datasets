# CBRS Units — working reconstruction draft

This branch reconstructs all **942** units from the producer-hosted FWS2024-12-19 full publication. It is reviewable draft implementation under the user’s explicit scope correction; source, baseline, contract, identifier mapping and release acceptance remain pending. No merge, production write or legal property determination is authorized.

The explicit source asset, stopped monthly integrity refresh, atomic local staging, lossless original archive/receipts, deterministic cache revalidation and shared promotion gates are implemented. Native NAD83 coordinates, all eight original DBF fields and intrinsic source FIDs are preserved. Original DBF physical widths/decimal bytes remain in the complete ZIP; float64 decoding is declared rather than called the original physical DBF representation. Published OBJECTID/id and separately delegated API IDs remain in complete pinned captures, without invented substitutions.

The full published942-row candidate has the same942 unique Unit codes, but only941 topologically equal geometries. Exact Unit joins show734 Shore_Mile,938 Shape_Leng and441 Shape_Area value differences. Fast_Acres/Wet_Acres published integers differ from original DBF numeric declarations; most Polygon wrappers also differ. Zero original FIDs equal the corresponding published OBJECTID or id. All942 delegated API OBJECTIDs match published OBJECTID, but the ZIP/API themselves disagree on one geometry and two Names. These are concrete decision inputs, not approved mappings or tolerances.

Read `source.json` and complete comparison files in `evidence/`. Original full inputs, complete source and published identifier rosters, all native geometries and source declarations are retained. DOI/DataLumos403 prevents an exact archive crosswalk match; it does not prevent reconstructing the currently producer-distributed edition.

Local run IDs and verification results will be added after execution. Independent review is pending. No CI or real dataset acceptance is inferred from fixtures or local tests.
