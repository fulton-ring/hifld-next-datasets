# Current USGS uranium and vanadium deposits candidate

This is a separately named draft authorized by the user on 2026-11-15. It does
not reconstruct or replace the worldwide MRDS inventory
`uranium-and-vanadium-deposits` (#352); that original product remains parked.

The producer is USGS, distributed by USGS ScienceBase item
[6464de5bd34ec179a83d9e6c](https://www.sciencebase.gov/catalog/item/6464de5bd34ec179a83d9e6c),
[DOI 10.5066/P9K1HBNT](https://doi.org/10.5066/P9K1HBNT),
“Critical mineral deposits of the United States (ver. 3.0, September 2026)”.
The item records initial publication 2023-07-18 and last revision 2026-09-09;
retrieval time is recorded separately. This versioned product has revisions,
with no fixed refresh interval established by this draft.

The implementation captures the item JSON and its exact
`critical_mineral_deposits_v3.zip` download URI, validates the complete 1,271
features, and selects comma-delimited, case-insensitive **whole tokens**
`uranium` or `vanadium` in `CritMin`: uranium 81, vanadium 38, both 22, union 97.
The selected candidate uses dataset `usgs-critical-mineral-deposits`, file
`uranium-vanadium-deposits`, preserving all 21 DBF fields and native WGS84 Point
coordinates. `OGC_FID` is the native reader's per-file feature identifier,
validated against each original SHP record; it is not an agency-wide or
cross-release identifier. No MRDS IDs are mapped or invented.

The archive measured 272,122 bytes, SHA-256
`e11c52c07c7fd6f0fef41e865c797e813683e2687252b37c2a79e418e8f9ca77`
on a fresh anonymous 2026-11-15 capture. Raw metadata, ZIP, HTTP wire bodies
and receipts remain outside Git and accompany the staged candidate. Any source,
schema, population, value, identifier, geometry or archive-member drift fails
closed into quarantine. Repeat acquisition validates the sealed first
construction rather than replacing its provenance.

Implementation sequence: test commodity selection and complete native decoding;
add the narrow source adapter using existing capture/seal/catalog helpers; wire
an explicit asset, completeness check and stopped schedule; verify the complete
original-source ingest/catalog/quality, all-row parity, replay and refusal to
publish into an already populated local inventory. Rights, schema/ID contract,
baseline and release acceptance remain false. No production publication or
worldwide-coverage claim is authorized.
