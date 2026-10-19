from pathlib import Path
import json
r=Path('/workspace/work/hifld-coordinator/30/repairs/cache-graph');d=json.loads((r/'cache-repair.json').read_text());expected={'false_fidelity_report':'cached_complete_source_fidelity_evidence_drift:metadata/geopackage_source_fidelity.json','false_accepted_placeholder':'cached_complete_source_fidelity_evidence_drift:metadata/baseline_comparison.json','false_shared_report':'cached_complete_source_fidelity_evidence_drift:metadata/source-fidelity/baseline_comparison.json','false_markdown':'cached_complete_source_fidelity_evidence_drift:metadata/source-fidelity/baseline_comparison.md','unknown_SQL_identifiers':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','geometry_envelope':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','contents_SRS':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','missing_ready_marker':'existing_candidate_missing_ready_marker','unexpected_identifier_file':'existing_candidate_unrostered_or_missing_object'}
verified={};needs_retry={}
for kind,probe in d['probes'].items():
 row=next(x for x in d['runs'] if x['run_id']==probe['run_id']);reason=json.loads(Path(probe['quarantine_report']).read_text())['reason'];correct=expected[kind] in reason and any(expected[kind] in x for x in row['errors'])
 (verified if correct else needs_retry)[kind]={'run_id':probe['run_id'],'quarantine_reason':reason,'expected_reason_present':correct}
assert not needs_retry,needs_retry
(r/'reason-code-audit.json').write_text(json.dumps({'head':d['head'],'verified':verified,'requires_environmental_retry':needs_retry},indent=2)+'\n');print(json.dumps({'reason_specific_rejections':len(verified),'requires_retry':needs_retry}))
