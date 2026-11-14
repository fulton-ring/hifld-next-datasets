import json
import hashlib
import shutil
import tempfile
from pathlib import Path
import unittest
import httpx

from dagster_hifld.esri_pbf import (
    EsriPbfError,
    DraftGeoParquetWriter,
    decode_feature_collection_pbf,
)
from dagster_hifld.assets.national_forests import _get
from dagster_hifld.assets.national_forests import national_forests_draft_schedule
from dagster_hifld.sources.epa_frs_common import content_derived_version


class EsriPbfDecoderTests(unittest.TestCase):
    def setUp(self):
        self.body = (Path(__file__).parent / "fixtures" / "national_forests_53512.pbf").read_bytes()

    def test_decodes_source_schema_transform_ids_and_polygon(self):
        decoded = decode_feature_collection_pbf(self.body)
        self.assertEqual(decoded["objectIdFieldName"], "objectid")
        self.assertEqual(decoded["spatialReference"]["latestWkid"], 3857)
        self.assertEqual(decoded["transform"]["scale"][1], 0.0001)
        self.assertEqual([field["name"] for field in decoded["fields"]], [
            "boundarytype", "objectid", "permanent_identifier", "source_featureid",
            "source_datasetid", "source_datadesc", "source_originator", "data_security",
            "distribution_policy", "loaddate", "name", "areasqkm", "ftype", "fcode",
            "admintype", "ownerormanagingagency", "globalid", "shape_Length", "shape_Area",
        ])
        feature = decoded["features"][0]
        self.assertEqual(feature["attributes"]["objectid"], 53512)
        self.assertEqual(feature["attributes"]["ownerormanagingagency"], 11)
        self.assertEqual(feature["attributes"]["source_datadesc"], "PAD-US 4.1")
        self.assertEqual([len(ring) for ring in feature["geometry"]["rings"]], [14, 11])

    def test_draft_geoparquet_preserves_typed_values_and_declarations(self):
        import pyarrow.parquet as pq
        from dagster_hifld.native_geoparquet_catalog import discover_prebuilt_native
        from dagster_hifld.resources import StagingStorageResource

        decoded = decode_feature_collection_pbf(self.body)
        # This bounded fixture has two separate exterior rings, as Esri
        # identifies polygon membership from ring orientation and containment.
        declarations = [
            {"name": field["name"], "type": field["type"], "nullable": True}
            for field in decoded["fields"]
        ]
        declarations.append({"name": "shape", "type": "esriFieldTypeGeometry", "nullable": True})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "draft.parquet"
            writer = DraftGeoParquetWriter(path, declarations, {"latestWkid": 3857})
            writer.write_page(decoded)
            self.assertEqual(writer.close(), (1, 25))
            table = pq.read_table(path)
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.schema.field("objectid").type, __import__("pyarrow").int64())
            self.assertEqual(table.schema.field("areasqkm").type, __import__("pyarrow").float64())
            self.assertEqual(table.column("objectid")[0].as_py(), 53512)
            self.assertIn(b"esriFieldTypeGlobalID", table.schema.metadata[b"hifld:source_field_declarations"])
            self.assertIn(b"true", table.schema.metadata[b"hifld:draft"])
            prefix = "national-forests/national-forests/vfixture"
            candidate = Path(directory) / prefix / "geoparquet" / "national-forests.parquet"
            candidate.parent.mkdir(parents=True)
            shutil.copyfile(path, candidate)
            manifest = {
                "title": "National Forests (USGS layer 31)",
                "description": "Unapproved draft candidate",
                "publisher": "U.S. Geological Survey",
                "agency": "USGS",
                "source_url": "https://carto.nationalmap.gov/arcgis/rest/services/govunits/MapServer/31",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "Draft representation with source PBF and field declarations retained",
                    "objects": [{
                        "path": "geoparquet/national-forests.parquet",
                        "bytes": candidate.stat().st_size,
                        "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
                    }],
                }}}}
            manifest_path = Path(directory) / prefix / "metadata" / "source_manifest.json"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_text(json.dumps(manifest))
            staging = StagingStorageResource(local_dir=directory, use_local=True)
            self.assertTrue(discover_prebuilt_native(staging, prefix))

    def test_rejects_truncated_and_empty_responses(self):
        with self.assertRaisesRegex(EsriPbfError, "empty_pbf_response"):
            decode_feature_collection_pbf(b"")
        with self.assertRaises(EsriPbfError):
            decode_feature_collection_pbf(self.body[:-1])

    def test_candidate_identity_is_deterministic_and_refresh_is_stopped(self):
        identity = {"code_commit": "a" * 40, "files": {"src/a.py": "b" * 64}}
        first = content_derived_version("c" * 64, "d" * 64, "e" * 64, identity)
        self.assertEqual(first, content_derived_version("c" * 64, "d" * 64, "e" * 64, identity))
        self.assertNotEqual(first, content_derived_version("c" * 64, "d" * 64, "f" * 64, identity))
        self.assertEqual(national_forests_draft_schedule.default_status.value, "STOPPED")

    def test_failed_http_status_retains_body_wire_and_receipt(self):
        body = b"temporary source failure"

        def handler(request):
            return httpx.Response(503, headers={"content-length": str(len(body))},
                                  stream=httpx.ByteStream(body), request=request)

        with tempfile.TemporaryDirectory() as directory, httpx.Client(transport=httpx.MockTransport(handler)) as client:
            capture_path = Path(directory) / "failure.json"
            with self.assertRaisesRegex(RuntimeError, "national_forests_source_http_capture_incomplete"):
                _get(client, "https://example.test/query", capture_path)
            self.assertEqual(capture_path.read_bytes(), body)
            self.assertEqual(capture_path.with_name("failure.json.wire").read_bytes(), body)
            receipt = json.loads(capture_path.with_name("failure.json.receipt.json").read_text())
            self.assertEqual(receipt["status"], 503)
            self.assertTrue(receipt["complete"])


if __name__ == "__main__":
    unittest.main()
