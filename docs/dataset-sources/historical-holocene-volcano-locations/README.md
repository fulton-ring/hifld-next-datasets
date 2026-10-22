# Historical Holocene Volcano Locations draft

Official [Smithsonian GVP Holocene list](https://volcano.si.edu/volcanolist_holocene.cfm) delegates its [WFS](https://volcano.si.edu/database/webservices.cfm). The complete captured feed has 1,214 points, 19 properties, original feature `id` and `Volcano_Number`. Both full ID rosters remain raw: all server feature IDs changed across consecutive reads while properties/coordinates stayed identical. Changed feature IDs require reviewed refresh; replaying the same capture reuses its candidate.

Canonical GeoParquet preserves int32, int16, exact decimals, literal strings/nulls, original schema declarations and native EPSG:4326 WKB. Decimal128(38,9) is a proposed exact staging encoding, not producer-declared precision; new scale/overflow is rejected. Coercive output formats stay disabled. Source publication VOTW5.4.0 is cited7Aug2026; the WFS release day is unspecified. Monthly refresh stays STOPPED.

Run `python -m unittest discover -s tests -p test_holocene_volcanoes.py`. Set `HOLOCENE_TEST_CAPTURE_DIR` for the complete-real-source test and `HIFLD_HOLOCENE_CAPTURE_DIR` for registered Dagster job `holocene_source_integrity`; use isolated local staging, output and `DAGSTER_HOME`.

Complete inputs/baseline candidates: `/workspace/work/hifld-coordinator/110/holocene-source-baseline-inputs.tar.gz`, SHA256 `60879096991ddd5d686e16a243af02f50d3e8a5e70f90ad7bea2e2809794d4f8`. Its43-member manifest pins every byte and mode; extract `capture/` for replay. Full reports stay outside Git.

The historical baseline has1,383 rows and different IDs/schema/native CRS. No source-ID join, geometry tolerance, source/baseline/contract/compatibility or rights acceptance is established. Smithsonian terms and compilation/photo attribution require review; source photo URLs/credits remain literal text, not downloaded images. Crosswalk/archive provider403 leaves crosswalk acceptance unresolved. Quality and independent approval gates block release. No production publication.
