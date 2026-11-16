import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
import shapely
from dagster import AssetKey, DefaultScheduleStatus

from dagster_hifld.assets.usace_ienc_isolated_danger import (
    usace_ienc_isolated_danger_buoy_point,
    usace_ienc_isolated_danger_buoy_point_complete,
    usace_ienc_isolated_danger_draft_schedule,
)
from dagster_hifld.definitions import defs
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import usace_ienc_isolated_danger as source
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    processing_identity,
    seal_ready_candidate,
    write_immutable,
)
from dagster_hifld.sources.usace_ienc_isolated_danger import (
    BTS_ITEM_ID,
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_FEATURE_IDS,
    FILE_SLUG,
    LAYER_ID,
    MASTER_GEODATABASE_URL,
    PRIOR_BTS_ITEM_ID,
    SERVICE_FOLDER_URL,
    SERVICE_NAME,
    _LAYER_FIELDS,
    _QUERY_FIELDS,
    _canonical,
    _capture_names,
    _sha,
    _parquet_bytes,
    _verify_first_candidate,
    _validate_captures,
    capture_usace_ienc_isolated_danger,
)


def _fields(profile, *, include_domain=True):
    return [
        {"name":name, "type":kind, "alias":name, **({"domain":None} if include_domain else {}), **({"length":length} if length else {})}
        for name, kind, length in profile
    ]


def _captures():
    item = {
        "id":BTS_ITEM_ID,
        "owner":"USDOT_BTS",
        "orgId":"example-transportation-org",
        "title":"Inland Electronic Navigational Charts",
        "description":(
            "The IENC dataset is updated bi-monthly by USACE/Army Geospatial Center. "
            f"Geodatabase: {MASTER_GEODATABASE_URL}. Services: {SERVICE_FOLDER_URL}. "
            "Only charts downloaded from the USACE chart server are suitable for navigation."
        ),
    }
    folder = {"services":[{"name":SERVICE_NAME,"type":"MapServer"}]}
    service = {
        "mapName":"ISOLATED_DANGER_BUOY_POINT",
        "copyrightText":"USACE IENC",
        "layers":[{"id":LAYER_ID,"name":"ISOLATED_DANGER_BUOY_POINT","geometryType":"esriGeometryPoint"}],
    }
    layer = {
        "id":LAYER_ID,
        "name":"ISOLATED_DANGER_BUOY_POINT",
        "geometryType":"esriGeometryPoint",
        "sourceSpatialReference":{"wkid":4326,"latestWkid":4326},
        "advancedQueryCapabilities":{"supportsPagination":True},
        "maxRecordCount":2000,
        "extent":{"xmin":-91.37686549999995,"ymin":40.57548590000005,
                  "xmax":-91.37686549999995,"ymax":40.57548590000005},
        "fields":_fields(_LAYER_FIELDS),
    }
    attributes = {
        "OBJECTID":1,
        "Buoy_Shape":"Pillar",
        "Color":"Red",
        "Object_Name":"Sunken M/V John Paul Lighted Buoy (378.4)",
        "Information":"Removed during ice season and replaced with an unlighted buoy.",
        "Scale_Minimum":60000,
        "Source_Date":"20121004",
        "Source_Indication":"US,U3,SURVY,MVR_Field_Survey,IENC_12_053",
        "Source_Dataset":"U37UM359.000",
        "Feature_ID":"U3 1617851130 00001",
    }
    page = {
        "fields":_fields(_QUERY_FIELDS, include_domain=False),
        "spatialReference":{"wkid":4326,"latestWkid":4326},
        "features":[{"attributes":attributes,"geometry":{"x":-91.37686549999995,"y":40.57548590000005}}],
    }
    docs = {
        "bts_item_before":item,
        "bts_item_after":item,
        "service_folder":folder,
        "service_before":service,
        "service_after":service,
        "layer_before":layer,
        "layer_after":layer,
        "count_before":{"count":EXPECTED_COUNT},
        "count_after":{"count":EXPECTED_COUNT},
        "ids_before":{"objectIds":[1]},
        "ids_seed":{"objectIds":[1]},
        "ids_after":{"objectIds":[1]},
        "page_000":page,
    }
    return {name:(json.dumps(value).encode(),{"status":200}) for name,value in docs.items()}


def _relocated(point_x: float, point_y: float):
    """Reviewer scenario: mutate the point and BOTH live layer extents together."""
    captures = _captures()
    page = json.loads(captures["page_000"][0])
    page["features"][0]["geometry"] = {"x": point_x, "y": point_y}
    captures["page_000"] = (json.dumps(page).encode(), {"status":200})
    for name in ("layer_before", "layer_after"):
        layer = json.loads(captures[name][0])
        layer["extent"] = {"xmin":point_x, "ymin":point_y, "xmax":point_x, "ymax":point_y}
        captures[name] = (json.dumps(layer).encode(), {"status":200})
    return captures


def _ordered_bodies(captures):
    bodies = [json.loads(captures["ids_seed"][0])]
    bodies.extend(json.loads(captures[name][0])
                  for name, _ in _capture_names([1]) if name != "ids_seed")
    return bodies


class UsaceIencIsolatedDangerTests(unittest.TestCase):
    def test_complete_native_capture_preserves_ids_names_types_xy_and_feature_date_semantics(self):
        fields, audit, features = _validate_captures(_captures())
        self.assertEqual(audit["source_feature_count"], 1)
        self.assertEqual(audit["source_objectids"], [1])
        self.assertEqual(audit["source_feature_ids"], sorted(EXPECTED_FEATURE_IDS))
        self.assertEqual(audit["feature_source_dates"], ["20121004"])
        self.assertEqual(audit["source_extent_epsg4326"], {
            "xmin":-91.37686549999995, "ymin":40.57548590000005,
            "xmax":-91.37686549999995, "ymax":40.57548590000005,
        })
        self.assertIsNone(audit["source_release_date"])
        self.assertIn("feature survey date", audit["source_release_date_unknown_reason"])
        parquet = pq.ParquetFile(io.BytesIO(_parquet_bytes(fields, features))).read()
        self.assertEqual(parquet.column_names, [field["name"] for field in fields] + ["SHAPE"])
        self.assertNotIn("id", parquet.column_names)
        for field in fields:
            self.assertEqual(parquet.column(field["name"]).to_pylist(), [features[0]["attributes"][field["name"]]])
        primary, _column, crs = geometry_metadata(parquet.schema)
        self.assertEqual(primary, "SHAPE")
        self.assertEqual(crs.to_epsg(), 4326)
        point = shapely.from_wkb(parquet.column("SHAPE")[0].as_py())
        self.assertEqual((point.x, point.y), (features[0]["geometry"]["x"], features[0]["geometry"]["y"]))

    def test_dagster_registry_contains_candidate_check_and_stopped_refresh(self):
        keys = {key for asset_definition in defs.assets for key in asset_definition.keys}
        self.assertIn(next(iter(usace_ienc_isolated_danger_buoy_point.keys)), keys)
        self.assertEqual(usace_ienc_isolated_danger_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_missing_or_changed_native_feature_id_fails_closed(self):
        for value in (None, "", "changed-id"):
            captures = _captures()
            page = json.loads(captures["page_000"][0])
            page["features"][0]["attributes"]["Feature_ID"] = value
            captures["page_000"] = (json.dumps(page).encode(), {"status":200})
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "source_feature_id"):
                _validate_captures(captures)

    def test_epsg4326_domain_bounds_pass_and_out_of_domain_points_fail_closed(self):
        # The closed EPSG:4326 domain bounds themselves are valid source
        # geometry and must round-trip unmodified; anything outside the
        # domain is rejected before staging even when the captured layer
        # extents are mutated to agree with the relocated point.
        for x, y, in_domain in (
            (180.0, 90.0, True), (-180.0, -90.0, True),
            (180.0, -90.0, True), (-180.0, 90.0, True),
            (180.0000001, 0.0, False), (-180.0000001, 0.0, False),
            (0.0, 90.0000001, False), (0.0, -90.0000001, False),
            (500.0, 100.0, False), (-500.0, -100.0, False),
        ):
            captures = _relocated(x, y)
            with self.subTest(point=(x, y)):
                if in_domain:
                    fields, audit, features = _validate_captures(captures)
                    self.assertEqual(audit["source_extent_epsg4326"],
                                     {"xmin":x, "ymin":y, "xmax":x, "ymax":y})
                    table = pq.ParquetFile(io.BytesIO(_parquet_bytes(fields, features))).read()
                    point = shapely.from_wkb(table.column("SHAPE")[0].as_py())
                    self.assertEqual((point.x, point.y), (x, y))
                else:
                    with self.assertRaisesRegex(ValueError, "source_point_outside_epsg4326_domain:1"):
                        _validate_captures(captures)

    def test_count_and_schema_or_z_m_drift_fail_closed(self):
        captures = _captures()
        captures["count_after"] = (b'{"count":2}', {"status":200})
        with self.assertRaisesRegex(ValueError, "source_count"):
            _validate_captures(captures)

        captures = _captures()
        layer = json.loads(captures["layer_after"][0])
        layer["fields"].append({"name":"NEW_FIELD","type":"esriFieldTypeString"})
        captures["layer_after"] = (json.dumps(layer).encode(), {"status":200})
        with self.assertRaisesRegex(ValueError, "metadata_changed"):
            _validate_captures(captures)

        captures = _captures()
        for name in ("layer_before", "layer_after"):
            layer = json.loads(captures[name][0])
            layer["fields"][3]["alias"] = "Unreviewed alias"
            captures[name] = (json.dumps(layer).encode(), {"status":200})
        with self.assertRaisesRegex(ValueError, "schema_or_complete_coverage"):
            _validate_captures(captures)

        captures = _captures()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 0.0
        captures["page_000"] = (json.dumps(page).encode(), {"status":200})
        with self.assertRaisesRegex(ValueError, "dimension_or_geometry"):
            _validate_captures(captures)

    def test_transfer_limit_and_incomplete_page_fail_closed(self):
        captures = _captures()
        page = json.loads(captures["page_000"][0])
        page["exceededTransferLimit"] = True
        captures["page_000"] = (json.dumps(page).encode(), {"status":200})
        with self.assertRaisesRegex(ValueError, "transfer_limit"):
            _validate_captures(captures)

        captures = _captures()
        page = json.loads(captures["page_000"][0])
        page["features"] = []
        captures["page_000"] = (json.dumps(page).encode(), {"status":200})
        with self.assertRaisesRegex(ValueError, "page_feature_count"):
            _validate_captures(captures)

    def test_current_bts_item_identity_fail_closed(self):
        def with_item(mutate):
            captures = _captures()
            item = json.loads(captures["bts_item_before"][0])
            mutate(item)
            body = json.dumps(item).encode()
            captures["bts_item_before"] = (body, {"status":200})
            captures["bts_item_after"] = (body, {"status":200})
            return captures

        cases = {
            "wrong_owner": lambda item: item.update(owner="usace_agc_dkd"),
            "wrong_item_id": lambda item: item.update(id=PRIOR_BTS_ITEM_ID),
            "missing_org": lambda item: item.pop("orgId"),
            "wrong_title": lambda item: item.update(title="Some Other Chart Product"),
            "missing_cadence": lambda item: item.update(description=item["description"].replace("updated bi-monthly", "updated occasionally")),
        }
        for label, mutate in cases.items():
            with self.subTest(case=label), self.assertRaisesRegex(ValueError, "bts_distribution_link_or_cadence_drift"):
                _validate_captures(with_item(mutate))

    def test_bts_item_service_link_and_delegation_fail_closed(self):
        def with_description(transform):
            captures = _captures()
            item = json.loads(captures["bts_item_before"][0])
            item["description"] = transform(item["description"])
            body = json.dumps(item).encode()
            captures["bts_item_before"] = (body, {"status":200})
            captures["bts_item_after"] = (body, {"status":200})
            return captures

        cases = {
            "missing_service_folder_link": lambda text: text.replace(SERVICE_FOLDER_URL, "https://example.invalid/services"),
            "missing_master_geodatabase_link": lambda text: text.replace(MASTER_GEODATABASE_URL, "https://example.invalid/gdb.zip"),
            "missing_chart_server_attribution": lambda text: text.replace("Only charts downloaded from the USACE chart server", "Charts are available"),
            "missing_all_delegation": lambda text: "No distribution links are provided.",
        }
        for label, transform in cases.items():
            with self.subTest(case=label), self.assertRaisesRegex(ValueError, "bts_distribution_link_or_cadence_drift"):
                _validate_captures(with_description(transform))

    def test_denied_prior_item_error_body_fails_closed(self):
        # Live failure mode observed for the earlier pinned BTS item: HTTP 200
        # transport carrying an ArcGIS permission-error body (403 GWM_0003),
        # documented in issue #130 comment 6330196988. Reproduced here with a
        # synthetic body; the denied item itself is never requested.
        denial = {"error":{"code":403, "messageCode":"GWM_0003",
                           "message":"Item does not exist or is inaccessible.", "details":[]}}
        for label, body in (
            ("arcgis_error_json", json.dumps(denial).encode()),
            ("empty_object", b"{}"),
        ):
            captures = _captures()
            captures["bts_item_before"] = (body, {"status":200})
            captures["bts_item_after"] = (body, {"status":200})
            with self.subTest(case=label), self.assertRaisesRegex(ValueError, "bts_distribution_link_or_cadence_drift"):
                _validate_captures(captures)

    def test_audit_records_prior_item_without_succession_claim(self):
        _fields, audit, _features = _validate_captures(_captures())
        self.assertEqual(audit["official_bts_item_id"], BTS_ITEM_ID)
        self.assertEqual(audit["prior_bts_item_id"], PRIOR_BTS_ITEM_ID)
        self.assertIn("anonymous-access denial", audit["prior_bts_item_status"])
        self.assertFalse(audit["bts_item_succession_claimed"])
        self.assertFalse(audit["source_release_accepted"])
        self.assertFalse(audit["release_accepted"])
        # Extent equality with the live service metadata is same-acquisition
        # consistency only; no independent geographic coverage is claimed.
        self.assertIn("not an independent geographic coverage baseline", audit["source_coverage_scope"])
        self.assertIn("never normalized or repaired", audit["drift_guard"])

    def test_retained_candidate_replay_keeps_original_receipts_and_rejects_rogue_objects(self):
        fields, audit, features = _validate_captures(_captures())
        parquet_body = _parquet_bytes(fields, features)
        identity = processing_identity(Path(__file__).parents[1])
        version = "v130-cache-fixture"
        prefix = candidate_prefix("isolated-danger-buoy-point-usace-ienc", "isolated-danger-buoy-point-usace-ienc", version)
        fingerprint = _sha(_canonical(identity))
        state_key = f"_construction-state/isolated-danger-buoy-point-usace-ienc/{version}.json"
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            outputs = {
                "geoparquet/isolated-danger-buoy-point-usace-ienc.parquet": parquet_body,
                "metadata/source_manifest.json": b'{"fixture":"unapproved"}\n',
                "metadata/source_audit.json": _canonical(audit) + b"\n",
                "metadata/processing_identity.json": _canonical(identity) + b"\n",
            }
            receipts = {}
            fresh_captures = {}
            for name, (body, _response) in sorted(_captures().items()):
                wire = b"original-wire:" + body
                receipt = {
                    "method":"GET", "url":f"https://fixture.invalid/{name}",
                    "final_url":f"https://fixture.invalid/{name}",
                    "observed_at":"2026-10-19T00:00:00+00:00", "status":200,
                    "complete_body":True, "wire_bytes":len(wire), "wire_sha256":_sha(wire),
                    "bytes":len(body), "sha256":_sha(body), "content_encoding":None,
                    "content_length":str(len(wire)), "error_code":None,
                }
                receipts[name] = receipt
                fresh_captures[name] = (body, receipt)
                outputs[f"raw/{name}.body"] = body
                outputs[f"raw/{name}.wire"] = wire
                outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
            ready = {
                "approved":False, "version":version, "source_feature_count":audit["source_feature_count"],
                "source_feature_id_sha256":audit["source_feature_id_sha256"],
                "source_record_sha256":audit["source_record_sha256"],
                "source_schema_sha256":audit["source_schema_sha256"],
                "source_release_accepted":False, "freshness_accepted":False,
                "schema_contract_accepted":False, "id_contract_accepted":False,
                "rights_accepted":False, "mapping_approved":False, "release_accepted":False,
            }
            outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
            descriptors = {}
            for name, body in outputs.items():
                write_immutable(storage, f"{prefix}/{name}", body)
                descriptors[name] = {"bytes":len(body), "sha256":_sha(body)}
            payload = {
                "processing_identity":identity,
                "source_record_sha256":audit["source_record_sha256"],
                "source_schema_sha256":audit["source_schema_sha256"],
                "capture_receipts":receipts, "objects":descriptors,
            }
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)

            # The shared cache verifier recomputes catalog derivatives from the
            # sealed native candidate, so a partial or placeholder catalog
            # sidecar set is refused fail-closed instead of blessing reuse.
            for name, (body, receipt) in list(fresh_captures.items()):
                refreshed = dict(receipt)
                refreshed["observed_at"] = "2026-10-20T00:00:00+00:00"
                fresh_captures[name] = (body, refreshed)
            write_immutable(storage, f"{prefix}/metadata/quality_manifest.json", b'{"derived":true}\n')
            with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_incomplete"):
                _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity,
                                        audit, parquet_body, fresh_captures)
            (Path(tmpdir) / prefix / "metadata/quality_manifest.json").unlink()
            result = _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity,
                                             audit, parquet_body, fresh_captures)
            self.assertTrue(result["reused"])
            self.assertEqual(result["source_feature_id_sha256"], audit["source_feature_id_sha256"])
            self.assertEqual(result["source_record_sha256"], audit["source_record_sha256"])
            # The reused-candidate handoff must still carry the completeness
            # metrics so the blocking check fails closed when they are absent.
            self.assertEqual(result["source_null_geometry_count"], 0)
            self.assertEqual(result["source_invalid_geometry_count"], 0)
            self.assertTrue(usace_ienc_isolated_danger_buoy_point_complete(result).passed)
            retained = json.loads(storage.read_key(f"{prefix}/raw/bts_item_before.receipt.json"))
            self.assertEqual(retained["observed_at"], "2026-10-19T00:00:00+00:00")

            write_immutable(storage, f"{prefix}/rogue.json", b"unexpected")
            with self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity,
                                        audit, parquet_body, fresh_captures)


class CoordinatedRelocationCaptureRegression(unittest.TestCase):
    """The reviewer's coordinated mutations driven through the real capture path."""

    def _fixture_client(self, bodies):
        response_index = 0

        def handler(request):
            nonlocal response_index
            body = bodies[response_index]
            response_index += 1
            return httpx.Response(
                200,
                stream=httpx.ByteStream(json.dumps(body).encode("utf-8")),
                request=request,
                headers={"content-type": "application/json"},
            )

        transport = httpx.MockTransport(handler)
        real_client = httpx.Client

        def fixture_client(**kwargs):
            kwargs.pop("follow_redirects", None)
            kwargs.pop("timeout", None)
            kwargs.pop("headers", None)
            return real_client(transport=transport, **kwargs)

        return fixture_client

    def _published_inventory(self, root):
        published = PublishedStorageResource(use_local=True, local_dir=str(root / "published"))
        published.write(DATASET_SLUG, FILE_SLUG, "accepted-v1",
                        "geoparquet/isolated-danger-buoy-point-usace-ienc.parquet", b"accepted-parquet")
        published.write(DATASET_SLUG, FILE_SLUG, "accepted-v1",
                        "metadata/quality_manifest.json", b'{"published":true}')
        return published, {key: published.read_key(key) for key in published.list_prefix()}

    def test_out_of_range_point_capture_fails_closed_without_seal_or_publication(self):
        with tempfile.TemporaryDirectory(prefix="hf-buoy-invalid-") as temporary:
            root = Path(temporary)
            staging = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            published, published_before = self._published_inventory(root)
            bodies = _ordered_bodies(_relocated(500.0, 100.0))
            capture_root = root / "captures"
            with patch.object(source.httpx, "Client", self._fixture_client(bodies)), patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=400_000_000)
            ):
                with self.assertRaisesRegex(ValueError, "source_point_outside_epsg4326_domain:1"):
                    capture_usace_ienc_isolated_danger(
                        staging, version="invalid-domain-500-100", capture_directory=str(capture_root)
                    )

            keys = list(staging.list_prefix())
            self.assertTrue(keys)
            self.assertFalse(any("candidate_ready" in key for key in keys))
            self.assertFalse(any(key.startswith("_construction-state/") for key in keys))
            self.assertFalse(any(key.startswith(f"{DATASET_SLUG}/") for key in keys))
            quarantine = [key for key in keys if key.startswith("_quarantine/")]
            self.assertTrue(quarantine)
            self.assertTrue(any(key.endswith("/raw/page_000.body") for key in quarantine))
            failure_keys = [key for key in quarantine if key.endswith("/metadata/failure.json")]
            self.assertEqual(len(failure_keys), 1)
            failure = json.loads(staging.read_key(failure_keys[0]))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertIn("page_000", failure["captured_names"])
            quarantine_base = failure_keys[0].rsplit("/metadata/failure.json", 1)[0]
            retained_page = json.loads(staging.read_key(f"{quarantine_base}/raw/page_000.body"))
            self.assertEqual(retained_page["features"][0]["geometry"], {"x": 500.0, "y": 100.0})
            manifests = list(capture_root.glob("*/capture_manifest.json"))
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_bytes())
            self.assertIs(manifest["complete"], False)
            self.assertEqual(manifest["error_code"], "ValueError")
            published_after = {key: published.read_key(key) for key in published.list_prefix()}
            self.assertEqual(published_after, published_before)

    def test_domain_boundary_point_capture_seals_and_check_passes(self):
        # A point exactly on the closed EPSG:4326 domain bound is genuine
        # source geometry: it must seal, round-trip unmodified, and pass the
        # blocking check; the pre-populated published inventory is untouched.
        with tempfile.TemporaryDirectory(prefix="hf-buoy-boundary-") as temporary:
            root = Path(temporary)
            staging = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            published, published_before = self._published_inventory(root)
            bodies = _ordered_bodies(_relocated(180.0, 90.0))
            capture_root = root / "captures"
            with patch.object(source.httpx, "Client", self._fixture_client(bodies)), patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=400_000_000)
            ):
                result = capture_usace_ienc_isolated_danger(
                    staging, version="boundary-180-90", capture_directory=str(capture_root)
                )

            self.assertEqual(result["feature_count"], 1)
            self.assertEqual(result["source_null_geometry_count"], 0)
            self.assertEqual(result["source_invalid_geometry_count"], 0)
            self.assertFalse(result["release_accepted"])
            check = usace_ienc_isolated_danger_buoy_point_complete(result)
            self.assertTrue(check.passed)
            self.assertEqual(check.metadata["source_invalid_geometry_count"].value, 0)
            prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, result["version"])
            self.assertEqual(result["candidate_ready"], prefix + "/metadata/candidate_ready.json")
            ready = json.loads(staging.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            parquet = pq.ParquetFile(io.BytesIO(
                staging.read_key(f"{prefix}/geoparquet/isolated-danger-buoy-point-usace-ienc.parquet")
            )).read()
            point = shapely.from_wkb(parquet.column("SHAPE")[0].as_py())
            self.assertEqual((point.x, point.y), (180.0, 90.0))
            self.assertFalse(any(key.startswith("_quarantine/") for key in staging.list_prefix()))
            self.assertTrue(any(key.startswith("_construction-state/") for key in staging.list_prefix()))
            published_after = {key: published.read_key(key) for key in published.list_prefix()}
            self.assertEqual(published_after, published_before)


class BlockingCompletenessCheck(unittest.TestCase):
    """The blocking asset check must fail closed for missing or bad metrics."""

    def _candidate(self) -> dict:
        return {
            "feature_count": EXPECTED_COUNT,
            "source_null_geometry_count": 0,
            "source_invalid_geometry_count": 0,
            "source_feature_id_sha256": "0" * 64,
            "source_coverage_scope": "single observed feature; extent is same-acquisition consistency only",
            "drift_guard": "guard",
        }

    def test_complete_metrics_pass(self):
        result = usace_ienc_isolated_danger_buoy_point_complete(self._candidate())
        self.assertTrue(result.passed)
        self.assertEqual(result.metadata["source_invalid_geometry_count"].value, 0)
        self.assertEqual(result.metadata["source_null_geometry_count"].value, 0)

    def test_missing_or_invalid_geometry_metrics_fail_closed(self):
        candidate = self._candidate()
        check = usace_ienc_isolated_danger_buoy_point_complete
        self.assertFalse(check(dict(candidate, source_invalid_geometry_count=1)).passed)
        self.assertFalse(check(dict(candidate, source_null_geometry_count=1)).passed)
        missing_invalid = {key: value for key, value in candidate.items()
                            if key != "source_invalid_geometry_count"}
        self.assertFalse(check(missing_invalid).passed)
        self.assertEqual(check(missing_invalid).metadata["source_invalid_geometry_count"].value, "missing")
        missing_null = {key: value for key, value in candidate.items()
                         if key != "source_null_geometry_count"}
        self.assertFalse(check(missing_null).passed)

    def test_population_failures_still_fail_closed(self):
        candidate = self._candidate()
        check = usace_ienc_isolated_danger_buoy_point_complete
        self.assertFalse(check(dict(candidate, feature_count=EXPECTED_COUNT - 1)).passed)
        without_ids = {key: value for key, value in candidate.items()
                       if key != "source_feature_id_sha256"}
        self.assertFalse(check(without_ids).passed)


if __name__ == "__main__":
    unittest.main()
