import io
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
import shapely
from dagster import AssetKey, DefaultScheduleStatus

from dagster_hifld.assets.usace_ienc_isolated_danger import (
    usace_ienc_isolated_danger_buoy_point,
    usace_ienc_isolated_danger_draft_schedule,
)
from dagster_hifld.definitions import defs
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    processing_identity,
    seal_ready_candidate,
    write_immutable,
)
from dagster_hifld.sources.usace_ienc_isolated_danger import (
    BTS_ITEM_ID,
    EXPECTED_COUNT,
    EXPECTED_FEATURE_IDS,
    LAYER_ID,
    MASTER_GEODATABASE_URL,
    SERVICE_FOLDER_URL,
    SERVICE_NAME,
    _LAYER_FIELDS,
    _QUERY_FIELDS,
    _canonical,
    _sha,
    _parquet_bytes,
    _verify_first_candidate,
    _validate_captures,
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

            # Catalog-owned additions are allowed, but the first acquisition's
            # timestamps, URL, body and wire bytes remain pinned to the seal.
            write_immutable(storage, f"{prefix}/metadata/quality_manifest.json", b'{"derived":true}\n')
            result = _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity, audit, parquet_body)
            self.assertTrue(result["reused"])
            self.assertEqual(result["source_feature_id_sha256"], audit["source_feature_id_sha256"])
            self.assertEqual(result["source_record_sha256"], audit["source_record_sha256"])

            write_immutable(storage, f"{prefix}/rogue.json", b"unexpected")
            with self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity, audit, parquet_body)


if __name__ == "__main__":
    unittest.main()
