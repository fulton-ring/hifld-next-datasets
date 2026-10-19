# Historical Tsunami Observations — issue 114 / PR 449

Working draft, unapproved. The explicit NOAA NCEI → Time Lapse webmap → delegated ArcGIS layer chain supplies 34,495 observations with 50 native attributes and EPSG:3857 points. Source `ID`, `ObjectId`, repeated `TSEVENT_ID`, Int16/Int32/Float32/Float64 declarations, original bytes and WKB remain preserved. BCE and unknown `DATE_STRING` values remain strings. Physical Float32 decoding is recorded separately from original wire values.

The complete historical candidate has 32,614 rows across six preserved formats. Diagnostic current `ID` versus historical `source_ID` gives 32,595 common values, 1,900 current-only and 19 historical-only. This is not an accepted join. ID/schema/type/nullability/geometry/coverage, source/baseline selection, rights and output policy decisions remain prerequisites to release. No source, baseline, contract or release is approved here.

## Artifact replay

The current Git tree contains code, a clearly synthetic two-row fixture and small manifests. The former 115,205,310 bytes of complete captures, six baselines, full reports and historical execution logs are preserved in a checksummed local tar.gz bundle. `artifact-manifest.json` pins both the bundle and its complete private member manifest. All 60 outer members and 234 nested ZIP members were replayed against SHA256 and ZIP CRCs. Original Git history remains intact; historical Dagster/first-construction state is retained separately.

Obtain the named bundle and member manifest from the coordinator's preserved local artifacts, then verify/extract to a fresh private directory:

```sh
uv run python scripts/verification/tsunami_observations_artifacts.py --bundle /path/reviewed-cc3acce-complete-evidence.tar.gz --member-manifest /path/reviewed-cc3acce-complete-evidence.manifest.json --extract /path/fresh-replay
HIFLD_TSUNAMI_OBSERVATIONS_CAPTURE_DIR=/path/fresh-replay/evidence/captures uv run python -m unittest discover -s tests -p test_tsunami_observations.py
uv run python scripts/verification/tsunami_observations_baseline.py --evidence-dir /path/fresh-replay/evidence
uv run python scripts/verification/tsunami_observations_local.py --capture-dir /path/fresh-replay/evidence/captures --runtime /path/fresh-local-dagster --skip-native-probes
```

Replay writes full reports only to the supplied private artifact/runtime directories. No network, production write or real approval registry is supplied. The local Dagster runner exercises the registered source, refresh, normal catalog/formats, actual missing-approval gate and a separate explicitly synthetic quality failure while preserving a prior local fixture. `--skip-native-probes` omits the already-reviewed expensive metadata attack rebuilds, not source fidelity or quality/approval checks.

Default focused tests run real decoder, type/ID/authority guards, Git construction identity, locking, staging, failure retention and cache checks against an explicitly invented two-row fixture. The complete 34,495-row source test requires the explicit preserved capture path above; it never silently fetches replacements.

## Validation scope

Historical source processing commit `7ea570c77769db46f3fab41cf484d34a8e7ec3b8` and reviewed head `cc3acced869848c984a56127ac12362622699e86` retain the full native/baseline/history and original full-suite results in the artifact bundle. Cleanup changes do not relabel those as new runs. Current committed-base checks and actual replay are recorded separately after the new public processing checkpoint. Shared verifier integration changes the installed processing fingerprint; a fresh candidate and first-construction context are required. Independent review of this cleanup is performed by another worker.
