# American Red Cross regions: identity and authority blocker

The intended product is unresolved. The pinned published object at `american-red-cross-chapter-1` contains 56 state polygons matching States layer 4, while the inventory describes Red Cross Chapter Regions. The current Chapters layer 3 has 226 features. Do not treat the States object as a Chapters release or substitute one for the other. The prior audit records 30/56 ordered zero-tolerance geometry matches and 53/56 topological matches, type differences, and three invalid geometries in each candidate without repair.

The ArcGIS account is attributed to American Red Cross National Headquarters, but independent evidence tying its public master service to the producer or an explicit delegation is still missing. The official RCView link redirected to Volunteer Connection login, not to the ArcGIS service. The item describes FY27 effective July 2026 while the service warns of a temporary reversion to FY26; the represented release remains unresolved. DataLumos and the DOI crosswalk returned provider-side HTTP 403, so their contents remain unverified, not unmatched. These observations do not establish a maintained feed or release date.

The maintainer decision is to identify whether this inventory entry means the historical 56-state product under an accurate identity or a separately versioned 226-chapter successor, verify producer delegation and represented vintage, and then review the selected product's IDs, fields, types, coverage, and geometry handling. No identity, mapping, repair, source, baseline, contract, or release is accepted. A faithful draft pipeline remains possible once identity and producer are established; do not use an archive refresh as a substitute.

Evidence: [complete-capture progress and decision request](https://github.com/fulton-ring/hifld-next-datasets/issues/12#issuecomment-6260745305), [independent blocker review](https://github.com/fulton-ring/hifld-next-datasets/issues/12#issuecomment-6260788857), and [PR review](https://github.com/fulton-ring/hifld-next-datasets/pull/429#issuecomment-6260792041). The complete raw capture bundle remains at `/workspace/work/hifld-coordinator/12-captures.tar.gz` (SHA-256 `4d81a7a4ee0dd573da3a69c9d969d5fdf2ba00db3053a64000936346068770a7`).

Original tracked research, evidence and the one-off capture audit are retained outside Git in `/tmp/hifld-native-reconstruction/cleanup12/issue-12-removed-research-docs-8ac55e62d726.tar.gz` (16 members; 172,319 original bytes; archive SHA-256 `fe48970828a49b9d163ad964083e1bc4d3ff90ceb8b002d47aa09e9436507871`). Restore from the worktree root with:

```bash
tar -xzf /tmp/hifld-native-reconstruction/cleanup12/issue-12-removed-research-docs-8ac55e62d726.tar.gz -C /workspace/hifld-worker-worktrees/12
```
