# Census Designated Places reconstruction draft

- Publisher: U.S. Census Bureau; 2024 substate FileGDB download: https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_substategeo.gdb.zip
- Selected layer: `Census_Designated_Place`; historical edition only.
- Local job: `cdp_source_integrity`; monthly schedule remains stopped.
- Blockers: 38 retained invalid source geometries, unresolved baseline/schema/ID mapping, crosswalk, and independent release approval.
- Original tracked research and reports: `/workspace/work/hifld-coordinator/artifacts/issue-34-removed-research-docs-2d80048eb3b3.tar.gz`.
- Archive SHA256: `6c03d72fb3116a0998aa0a1e9a3a8bc4a330c0cf224ff57249cdc34bc013e053`.
- The required original runtime ZIP remains in place and was not read or copied.
- Synthetic tests run with `PYTHONPATH=src uv run --frozen python -m unittest discover -s tests -p test_census_designated_places.py`.
- Opt-in full-source test only when its preserved ZIP is already available: `CDP_TEST_ARCHIVE=<original-zip> PYTHONPATH=src uv run --frozen python -m unittest discover -s tests -p test_census_designated_places.py`.
- Draft only; source, baseline, geometry, and release acceptance remain pending.
