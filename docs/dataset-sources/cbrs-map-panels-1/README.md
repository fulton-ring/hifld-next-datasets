# FWS CBRS Map Panels draft

- Official landing: https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data
- Candidate endpoint: https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer/5/query
- Local job: `fws_cbrs_map_panels_draft_refresh_job`; schedule remains stopped.
- Blockers: feed publication date is unknown; use limits and FWS share-back/attribution terms require review.
- Archived research and all original captured test inputs: `/workspace/work/hifld-coordinator/artifacts/issue-29-removed-research-docs-44d3150d4398.tar.gz`.
- Archive SHA256: `ef8619e786de209d0edb4bfe0b557caee3979694d09a4f4c3d23bb1efad4cb68`.
- Four capture-independent mechanics tests stay in the default run; full-capture tests require `HIFLD_CBRS_MAP_PANELS_TEST_CAPTURE_DIR`.
- Opt in: `HIFLD_CBRS_MAP_PANELS_TEST_CAPTURE_DIR=/workspace/work/hifld-coordinator/29/artifact-cleanup-29/original-complete-captures PYTHONPATH=src:tests:. /workspace/hifld-worker-worktrees/19/.venv/bin/python -m unittest discover -s tests -p test_cbrs_map_panels_source.py`.
- Draft only; no source, legal, baseline, or release acceptance is implied.
