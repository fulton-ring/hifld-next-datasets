# FHWA National Bridge Inventory 2025 ASCII draft

Producer publication: https://www.fhwa.dot.gov/bridge/nbi/ascii.cfm (2025 state files: `ascii2025.cfm`).
The layout and per-item positions come from the producer record format page: https://www.fhwa.dot.gov/bridge/nbi/format.cfm.
Official SNBI Data Crosswalk: https://www.fhwa.dot.gov/bridge/snbi/datacrosswalk.xlsx (B.L.05/B.L.06 conversion rules).
Run registered `nbi_source_integrity` with isolated local staging; its quarterly schedule is STOPPED.
`HIFLD_NBI_CAPTURE_DIR` selects complete original `.body`/`.receipt.json` inputs instead of live HTTP.
The adapter captures all 53 state files (50 states, DC, PR, VI) plus landing/format/crosswalk/disclaimer pages.
All 124 fixed-width Coding Guide items survive as exact source-width strings; no trimming or unit conversion.
Columns decode 1:1 as ISO-8859-1; the 7 non-ASCII NY/PR location bytes are preserved and count-pinned.
Points derive only from Items 16/17 by the official crosswalk rules (EPSG:4326, 6-dp quantization).
Blank/partial source coordinates yield null geometry; zero and out-of-envelope values are preserved and pinned.
The archived DataLumos 239684 copy is an USDOT/BTS NTAD redistribution; its 624,193 count matches this release.
SNBI Excel/JSON/GeoJSON are a changed successor with different populations (e.g. Idaho 4,643 vs 4,646) and schema.
2025 is the final Coding Guide ASCII year; all state files were posted 2025-06-12 (per-file Last-Modified retained).
No producer row ID exists; the composite (STATE_CODE_001, STRUCTURE_NUMBER_008) pair has two pinned duplicate pairs.
Native GeoParquet only; raw bodies, receipts, declarations and first-construction witnesses persist in staging.
Source, schema/ID contract, coordinate basis, rights and publication acceptance remain pending human decisions.
