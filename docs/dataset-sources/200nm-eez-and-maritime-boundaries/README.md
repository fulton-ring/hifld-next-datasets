# 200NM EEZ and Maritime Boundaries

Working draft NOAA extraction preserves all 96 source IDs, 13 attributes, physical types and ordered geometry vertices. It performs before/after count and ID checks, retains original responses and receipts, quarantines drift, and uses the normal catalog, verification and promotion graph. Refresh is weekly and stopped by default. Release requires the decisions in [DECISION.md](DECISION.md); source, baseline, contract and compatibility approval remain pending.

Shapefile is disabled after complete-data testing demonstrated string truncation, numeric rounding and nullable-date type changes. Native candidate and supported terminal outputs remain separate from unapproved format migrations. No source values are shortened or repaired.

Full captures and generated audit/materialization reports are retained in the checksummed artifact described by [artifact-manifest.json](evidence/artifact-manifest.json), outside Git. The manifest records the original reviewed commit and every original file's size and SHA256. Preserve that artifact for replay; live refetch is not equivalent historical evidence. The complete published baseline candidate remains separate and unapproved.

To replay, verify the artifact SHA256, extract it into an external directory, then verify every member against the manifest. Set HIFLD_NOAA_EEZ_EVIDENCE_DIR to its docs/dataset-sources/200nm-eez-and-maritime-boundaries/evidence directory.

Run default focused tests with:

    uv run --frozen python -m unittest discover -s tests -p 'test_noaa*.py'

After setting that variable, repeat the command for complete real-input materialization, or run audit.py --quality. compare_candidate.py --baseline PATH also uses this verified external evidence directory.

Default tests retain small synthetic extraction, completeness, physical-type, receipt-origin, error, drift and refresh-registration checks. Complete real-input replay is explicitly opt-in; synthetic fixtures grant no real acceptance. scripts/verification/noaa_eez_local.py runs the actual registered graph with isolated storage and records its run IDs, output checksums and previous-release preservation. Existing historical execution records retain their exact tested commits and limitations, including earlier missing sibling IaC failures. No CI pass is inferred.

Current verification is summarized in [verification.json](evidence/verification.json); its checksummed artifact preserves full logs, captured bodies, native outputs, run databases and audit reports. Historical count/ID fixture slots may reuse captured bodies; fresh live extraction is recorded separately.

This PR targets the shared implementation in PR #421. Shared verification/provenance/catalog changes belong in that PR; dataset-specific behavior and tests belong here.
