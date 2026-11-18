# Ferry Terminals source record

Source: the BTS National Census of Ferry Operators (NCFO) terminals file on
`data.bts.gov`. The adapter queries the BTS Socrata catalog for exact
`YYYY NCFO Terminals File` datasets, selects the newest edition, and captures
that dataset's view metadata, row count and every row. The current edition is
the 2024 file (dataset `difu-6bgs`, published 2026-06-10, 575 terminal records).

The 2024 file has one row per terminal; BTS combines operator answers where
they disagree. Columns are retained in their published order, and
`terminal_id`, latitude and longitude are typed from the Socrata JSON string
encoding without changing values. Geometry is an EPSG:4326 point built from the
published longitude and latitude.

The separate NTAD Ferry Routes layer used by issue #88 still serves NCFO 2020 /
data year 2019.

Job: `bts_ferry_terminals_draft_refresh`; proposed monthly source-integrity
schedule **STOPPED**. This is a local draft and is not approved for publication.

Release blockers: source release, schema, identifier, mapping, rights and
release approvals are all false. A future catalog edition or schema change
builds a new content-derived version or fails closed; it does not rewrite the
existing candidate.

Focused tests use an in-process HTTP mock and need no network:

```sh
PYTHONPATH=tests uv run python -m unittest tests.test_ferry_terminals
```
