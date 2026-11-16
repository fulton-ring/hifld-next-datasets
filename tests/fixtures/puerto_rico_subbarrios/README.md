The source ZIP stays outside Git. Its official URL is
https://www2.census.gov/geo/tiger/TIGER2026/SUBBARRIO/tl_2026_72_subbarrio.zip.
The pinned archive is 137,565 bytes with SHA-256
`a85af8bea84e763c6a9150950a3aca40193dfa8bc46ece8ea97122e03cd80b64` (the maintained TIGER2026
vintage; issue #777 migrated the four TIGER2023 pipelines to it).

Run all archive checks offline using a retained local copy:

```sh
HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE=/absolute/path/to/tl_2026_72_subbarrio.zip \
  uv run python -m unittest tests.test_puerto_rico_subbarrios
```

Or explicitly opt into fetching the official URL:

```sh
HIFLD_FETCH_PUERTO_RICO_SUBBARRIOS_FIXTURE=1 \
  uv run python -m unittest tests.test_puerto_rico_subbarrios
```

Both paths enforce the pinned size and SHA-256 before inspecting the archive.
Missing or changed explicitly requested fixtures fail. Without either option,
the suite stays offline and skips the two archive-dependent checks with setup
instructions. No disposable worker path is required.
