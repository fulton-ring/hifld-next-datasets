The source ZIP stays outside Git. Its official URL is
https://www2.census.gov/geo/tiger/TIGER2023/SUBBARRIO/tl_2023_72_subbarrio.zip.
The pinned archive is 133,211 bytes with SHA-256
`5358e982d49bc6a5c099e12d32220e756e05aff848f24ba89d862dd89d7b8d8a`.

Run all archive checks offline using a retained local copy:

```sh
HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE=/absolute/path/to/tl_2023_72_subbarrio.zip \
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
