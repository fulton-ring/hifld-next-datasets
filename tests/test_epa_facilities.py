import base64
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
from dagster_hifld.sources.epa_facilities import (
    EPA_FACILITIES_PROFILE,
    LayerProfile,
    _profile_urls,
    _validate_cached_catalog_sidecars,
    decode_captures,
    inspect_captures,
)


class EpaFacilitiesSourceTests(unittest.TestCase):
    def test_post_catalog_cache_accepts_only_consistent_catalog_sidecars(self):
        class Storage:
            def __init__(self, keys, documents):
                self.keys = keys
                self.documents = documents

            def list_keys(self, *_args):
                return self.keys

            def read_key(self, key):
                value = self.documents[key]
                return value if isinstance(value, bytes) else json.dumps(value).encode()

            def object_snapshot(self, key):
                body = self.read_key(key)
                return (len(body), hashlib.sha256(body).hexdigest())

        table = pa.table({"OBJECTID": pa.array([1], type=pa.int64()), "Shape": pa.array([b"point"], type=pa.binary())})
        prefix = "epa-facilities/epa-facilities/v1"
        version = "v1"
        manifest = b'{"processing_identity":{"code_commit":"test"}}'
        manifest_sha = hashlib.sha256(manifest).hexdigest()
        expected = {
            f"{prefix}/geoparquet/epa-facilities.parquet", f"{prefix}/metadata/candidate_ready.json",
            f"{prefix}/metadata/source_manifest.json",
        }
        source_objects = {"metadata/source_manifest.json": {"bytes": len(manifest), "sha256": manifest_sha}}
        quality_key = f"{prefix}/metadata/quality_manifest.json"
        dictionary_key = f"{prefix}/metadata/data_dictionary.json"
        snapshot_keys = {
            f"{prefix}/metadata/source/source_manifest.json",
            f"{prefix}/metadata/source/version_{manifest_sha}_source_manifest.json",
        }
        release_keys = {f"{prefix}/{name}" for name in ("collection.json", "README.md", "AGENTS.md")}
        quality = {
            "version": version, "feature_count": 1, "geometry_type": "Point", "native_geometry_column": "Shape",
            "native_crs": {"id": {"authority": "EPSG", "code": 4269}}, "null_geometry_count": 0,
            "invalid_geometry_count": 0, "empty_geometry_count": 0, "unparseable_geometry_count": 0,
            "quality_check_passed": True,
        }
        dictionary = {
            "featureCount": 1, "columns": [{"name": "OBJECTID"}, {"name": "Shape"}],
            "nativeArrowSchemaIPCBase64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        }
        storage = Storage(expected | {quality_key, dictionary_key} | snapshot_keys | release_keys, {
            quality_key: quality, dictionary_key: dictionary, f"{prefix}/metadata/source_manifest.json": manifest,
            **{key: manifest for key in snapshot_keys},
            **{key: b"release derivative" for key in release_keys},
        })
        with patch("dagster_hifld.sources.epa_facilities.validate_staged_release_derivatives", return_value=release_keys) as validate_release:
            _validate_cached_catalog_sidecars(storage, EPA_FACILITIES_PROFILE.ordinary, version, prefix, table, expected, source_objects)
        self.assertEqual(validate_release.call_args.args[2], set(storage.keys))

        storage.documents[quality_key] = {**quality, "feature_count": 2}
        with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
            with patch("dagster_hifld.sources.epa_facilities.validate_staged_release_derivatives", return_value=release_keys):
                _validate_cached_catalog_sidecars(storage, EPA_FACILITIES_PROFILE.ordinary, version, prefix, table, expected, source_objects)
        storage.documents[quality_key] = quality
        storage.keys.add(f"{prefix}/metadata/unrecognized.json")
        with self.assertRaisesRegex(ValueError, "cached_candidate_object_roster_changed"):
            with patch("dagster_hifld.sources.epa_facilities.validate_staged_release_derivatives", return_value=release_keys):
                _validate_cached_catalog_sidecars(storage, EPA_FACILITIES_PROFILE.ordinary, version, prefix, table, expected, source_objects)

    def test_accepts_the_retained_43_object_labels_catalog_roster(self):
        inventory_path = Path(__file__).parent / "fixtures" / "epa_facilities_labels_cataloged_roster.json"
        inventory = json.loads(inventory_path.read_text())
        prefix = inventory["candidate_prefix"]
        version = prefix.rsplit("/", 1)[-1]
        entries = {item["relative_path"]: item for item in inventory["objects"]}
        self.assertEqual(len(entries), 43)
        derived = {
            "metadata/quality_manifest.json", "metadata/data_dictionary.json", "metadata/resolved_provenance.json",
            "metadata/source/source_manifest.json",
            next(name for name in entries if name.startswith("metadata/source/version_") and name.endswith("_source_manifest.json")),
        }
        expected = {f"{prefix}/{name}" for name in entries if name not in derived}
        quality_key, dictionary_key = (f"{prefix}/metadata/{name}.json" for name in ("quality_manifest", "data_dictionary"))
        manifest = b"sealed source manifest retained by source construction"
        table = pa.table({"OBJECTID": pa.array([1], type=pa.int64()), "Shape": pa.array([b"point"], type=pa.binary())})
        quality = {
            "version": version, "feature_count": 1, "geometry_type": "Point", "native_geometry_column": "Shape",
            "native_crs": {"id": {"authority": "EPSG", "code": 4269}}, "null_geometry_count": 0,
            "invalid_geometry_count": 0, "empty_geometry_count": 0, "unparseable_geometry_count": 0,
            "quality_check_passed": True,
        }
        dictionary = {
            "featureCount": 1, "columns": [{"name": "OBJECTID"}, {"name": "Shape"}],
            "nativeArrowSchemaIPCBase64": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
        }
        snapshots = {f"{prefix}/{name}" for name in derived if name.startswith("metadata/source/")}
        release_keys = {f"{prefix}/{name}" for name in ("collection.json", "README.md", "AGENTS.md")}
        actual = expected | {quality_key, dictionary_key, f"{prefix}/metadata/resolved_provenance.json"} | snapshots | release_keys

        class Storage:
            def list_keys(self, *_args):
                return actual

            def read_key(self, key):
                if key == f"{prefix}/metadata/source_manifest.json" or key in snapshots:
                    return manifest
                if key == quality_key:
                    return json.dumps(quality).encode()
                if key == dictionary_key:
                    return json.dumps(dictionary).encode()
                if key == f"{prefix}/metadata/resolved_provenance.json":
                    return b"{}"
                return b"release derivative"

            def object_snapshot(self, key):
                body = self.read_key(key)
                return (len(body), hashlib.sha256(body).hexdigest())

        descriptor = entries["metadata/source_manifest.json"]
        source_objects = {"metadata/source_manifest.json": {"bytes": descriptor["bytes"], "sha256": descriptor["sha256"]}}
        with patch("dagster_hifld.sources.epa_facilities.provenance_projection", return_value={}), \
             patch("dagster_hifld.sources.epa_facilities.validate_staged_release_derivatives", return_value=release_keys):
            _validate_cached_catalog_sidecars(
                Storage(), EPA_FACILITIES_PROFILE.labels, version, prefix, table, expected, source_objects,
            )

    def test_profiles_keep_the_ordinary_and_labels_layers_distinct(self):
        ordinary = EPA_FACILITIES_PROFILE.ordinary
        labels = EPA_FACILITIES_PROFILE.labels
        self.assertEqual((ordinary.layer_id, ordinary.name), (1, "EPA Facilities"))
        self.assertEqual((labels.layer_id, labels.name), (0, "EPA Facilities (Labels)"))
        self.assertEqual(ordinary.object_ids, tuple(range(1, 78)))
        self.assertEqual(labels.object_ids, tuple(range(1, 78)))
        self.assertFalse(EPA_FACILITIES_PROFILE.release_date)

    def test_query_page_schema_is_validated_before_native_decode(self):
        source_ref = {"wkid": 4269, "latestWkid": 4269}
        display_ref = {"wkid": 3857, "latestWkid": 3857}
        fields = (
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OID", "nullable": False, "domain": None},
            {"name": "Shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "nullable": True, "domain": None},
            {"name": "LABEL", "type": "esriFieldTypeString", "alias": "Label", "length": 8, "nullable": True, "domain": None},
            {"name": "RuleID_1", "type": "esriFieldTypeInteger", "alias": "RuleID_1", "nullable": True, "domain": None},
        )
        feature = {"attributes": {"OBJECTID": 1, "LABEL": "EPA", "RuleID_1": 1}, "geometry": {"x": -77.0, "y": 38.0, "z": 0.0}}
        canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        profile = LayerProfile(
            layer_id=0, name="EPA Facilities (Labels)", slug="test-epa-labels", geometry_type="esriGeometryPoint",
            source_crs=4269, source_spatial_reference=source_ref, display_spatial_reference=display_ref,
            field_definitions=tuple(fields), count=1, object_ids=(1,), source_record_sha256=hashlib.sha256(canonical([feature])).hexdigest(),
            schema_verified=True, population_verified=True,
        )
        service = {"layers": [{"id": 0, "name": profile.name}]}
        metadata = {"id": 0, "name": profile.name, "geometryType": profile.geometry_type, "fields": list(fields),
                    "sourceSpatialReference": source_ref, "extent": {"spatialReference": display_ref}}
        page_fields = [{k: field[k] for k in ("name", "type", "alias", "length") if k in field} for field in fields if field["type"] != "esriFieldTypeGeometry"]
        page = {"geometryType": profile.geometry_type, "spatialReference": source_ref, "fields": page_fields,
                "features": [feature], "exceededTransferLimit": False}
        documents = {
            "source_service": service, "layer_before": metadata, "layer_after": metadata,
            "count_before": {"count": 1}, "count_after": {"count": 1},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": [1]},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": [1]}, "page_00000": page,
        }
        urls = _profile_urls(profile)
        captures = {}
        for name, document in documents.items():
            body = canonical(document)
            captures[name] = (body, {"url": urls[name], "final_url": urls[name], "observed_at": "2026-10-19T00:00:00+00:00",
                                     "status": 200, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "complete_body": True})
        table, audit = decode_captures(captures, profile=profile)
        self.assertEqual(table.column_names, ["OBJECTID", "Shape", "LABEL", "RuleID_1"])
        self.assertEqual(table.schema.field("RuleID_1").type, __import__("pyarrow").int32())
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["Shape"]["geometry_types"], ["Point Z"])
        self.assertEqual(audit["feature_count"], 1)
        changed = dict(page)
        changed["fields"] = [dict(field) for field in page_fields]
        changed["fields"][0]["nullable"] = True
        body = canonical(changed)
        captures["page_00000"] = (body, {**captures["page_00000"][1], "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
        with self.assertRaisesRegex(ValueError, "page_field_optional_property_drift"):
            inspect_captures(captures, profile=profile)
        changed = dict(page)
        changed["fields"] = [dict(field) for field in page_fields]
        changed["fields"][1]["length"] = 9
        body = canonical(changed)
        captures["page_00000"] = (body, {**captures["page_00000"][1], "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
        with self.assertRaisesRegex(ValueError, "page_field_schema_drift"):
            inspect_captures(captures, profile=profile)


if __name__ == "__main__":
    unittest.main()
