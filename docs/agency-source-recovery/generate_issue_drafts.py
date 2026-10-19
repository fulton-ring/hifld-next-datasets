"""Generate one research-first issue draft per inventory ID; performs no writes to GitHub."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
inventory = json.loads((root / 'dataset-inventory.json').read_text())
datasets = inventory['datasets']
ids = [row['dataset_id'] for row in datasets]
if len(ids) != len(set(ids)):
    raise ValueError('Duplicate dataset IDs in inventory')

with (root / 'dataset-issues.jsonl').open('w') as output:
    for row in datasets:
        dataset_id = row['dataset_id']
        body = f'''<!-- hifld-dataset:{dataset_id} -->
Inventory ID: `{dataset_id}`
Agency (inventory): {row['agency'] or 'Unknown'}
Publisher (inventory): {row['publisher'] or 'Not specified'}
Archive: {row['archive_url']}
Inventory modification date: {row['inventory_date_modified']} (not a current release date)
Existing ingest module: {row['existing_ingest_module'] or 'No explicit ingest mapped'}

## Source research gate
Source is unverified. Match the DHS crosswalk (https://doi.org/10.3886/E241367V1), record official landing and machine-download URLs with dated anonymous request evidence, and compare coverage, schema and IDs with this archive. Reuse https://github.com/fulton-ring/agent-content-demos/issues/40 as prior evidence. A responding service, archive mirror or partial substitute does not establish a maintained equivalent.

## Done means
- [ ] Record an exact crosswalk match or reviewed unmatched result, producer and access terms.
- [ ] Verify complete extraction, pagination/limits, schema, geometry, stable IDs and release date.
- [ ] Accept the source relationship: continuation, changed successor, partial substitute, archive-only, restricted or unresolved.
- [ ] For an accepted maintained public source, implement or update the explicit Dagster ingest and existing catalog/publish wiring; preserve provenance and historical versions.
- [ ] Add source-appropriate refresh, idempotency, failure handling and tests; record required suite results and local end-to-end validation.
- [ ] Independent review and required checks pass on the current PR head before auto-merge.

Start in Source research, not Ready. Follow AGENTS.md and docs/agency-source-recovery/README.md. If no maintained public source is found, document evidence and the blocker instead of building an archive-refresh pipeline. Production deployment is separate.
'''
        draft = {'dataset_id': dataset_id, 'title': f"Source and pipeline: {row['title']}", 'body': body}
        output.write(json.dumps(draft, ensure_ascii=False) + '\n')
print(f'Prepared {len(datasets)} drafts in {root / "dataset-issues.jsonl"}')
