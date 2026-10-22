# BSEE OCS Platforms (draft)

The official BSEE Mapping page advertises the Platforms Shapefile at
<https://www.data.bsee.gov/Mapping/Files/platform.zip>. Its accompanying
metadata describes the product as the Gulf of Mexico Federal OCS platforms in
the database, with monthly updates, a publication date of 2026-03-02, 7,317
points, and NAD27 coordinates. It also warns that GIS coordinates are
approximate and are not an official record. This is evidence for a complete
observed source population within BSEE's stated product scope; it does not
establish currentness, a historical HIFLD crosswalk, or accepted geographic
coverage.

The retained anonymous extraction contains the official mapping page,
`platform_meta.html`, and `platform.zip`, each with decoded body, wire body, and
GET receipt. The platform ZIP was 254,511 bytes with SHA-256
`24f3feeeda2727fd6d91b256225ffc3f6754b1ab95479f1a0af79710f825503c`. The
replayable capture packet is retained outside Git with the run evidence.

The draft retains all five physical Shapefile fields and their native values
and types: `STRUCTURE_` (integer), `COMPLEX_ID` (64-bit integer), `STRUCTURE1`
(string), `INSTALL_DA` (date), and `REMOVAL_DA` (date). It retains both IDs
unchanged and NAD27 point geometry without reprojection. The pair
`(COMPLEX_ID, STRUCTURE_)` is unique in this capture; no accepted public ID
mapping is asserted.

Source release/currentness, historical correspondence, identifier/schema and
coverage contracts, rights/attribution, and release approval remain pending.
The refresh schedule is stopped; a successful draft ingest or catalog check
does not clear those decisions or permit publication.
