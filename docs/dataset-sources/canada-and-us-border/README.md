# IBC Canada–US boundary draft

- Producer landing: https://www.internationalboundarycommission.org/en/maps-coordinates/coordinates.php
- Static edition 1.3 download: https://www.internationalboundarycommission.org/uploads/shapefile/us-canada-boundary-v1-3.zip (published 2018-04-20).
- Local draft job: `ibc_boundary_draft_refresh`; schedule remains stopped.
- Blockers: edition currency, published compatibility/baseline decisions, and embedded OGL Canada versus generic website commercial terms need review.
- Original tracked research and captures: `/workspace/work/hifld-coordinator/artifacts/issue-26-removed-research-docs-038e67c265c1.tar.gz`.
- Archive SHA256: `0999d9e05aab5d16cdd75f00e5c475c7d09a8af480e64f8dafb44f9daa2bd531`.
- Full-capture tests skip unless `HIFLD_CANADA_US_BORDER_TEST_CAPTURE_DIR` points to restored original captures.
- Opt in: `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-26-removed-research-docs-038e67c265c1.tar.gz -C /tmp/ibc-evidence && HIFLD_CANADA_US_BORDER_TEST_CAPTURE_DIR=/tmp/ibc-evidence/docs/dataset-sources/canada-and-us-border/evidence/captures PYTHONPATH="$PWD/src" uv run --frozen python -m unittest discover -s tests -p test_canada_us_border_source.py`.
- Draft status only; no source, licence, baseline, compatibility, or release acceptance is implied.
