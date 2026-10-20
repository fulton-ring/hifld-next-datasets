"""Focused drift and completeness checks for the USACE IENC PIPE_AREA source."""

from __future__ import annotations

import hashlib
import json
import unittest

import httpx

from dagster_hifld.sources.usace_pipeline_areas import (
    EXPECTED_FIELDS,
    EXPECTED_OBJECTIDS_SHA256,
    LAYER_URL,
    SERVICE_URL,
    SourceDriftError,
    extract_pipeline_areas,
)


_ESRI_TYPES = {
    "OBJECTID": "esriFieldTypeOID",
    "Object_Name": "esriFieldTypeString",
    "Product": "esriFieldTypeString",
    "Restriction": "esriFieldTypeString",
    "Status": "esriFieldTypeString",
    "Category_of_Pipeline": "esriFieldTypeString",
    "Information": "esriFieldTypeString",
    "Scale_Minimum": "esriFieldTypeInteger",
    "Source_Date": "esriFieldTypeString",
    "Source_Indication": "esriFieldTypeString",
    "Source_Dataset": "esriFieldTypeString",
    "Feature_ID": "esriFieldTypeString",
    "Shape__Area": "esriFieldTypeDouble",
    "Shape__Length": "esriFieldTypeDouble",
}


def _response(payload: dict) -> httpx.Response:
    return httpx.Response(200, json=payload)


def _client(*, count: int = 112, omit_last_feature: bool = False) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == SERVICE_URL.removeprefix("https://services7.arcgis.com"):
            return _response({"serviceDescription": "USACE IENC"})
        if request.url.path.endswith(f"/{LAYER_URL.rsplit('/', 1)[-1]}"):
            return _response(
                {
                    "id": 82,
                    "name": "PIPE_AREA",
                    "copyrightText": "USACE IENC",
                    "geometryType": "esriGeometryPolygon",
                    "fields": [
                        {"name": name, "type": _ESRI_TYPES[name], "nullable": name != "OBJECTID"}
                        for name in EXPECTED_FIELDS
                    ],
                }
            )
        if request.url.path.endswith("/query"):
            params = dict(request.url.params)
            if params.get("returnCountOnly") == "true":
                return _response({"count": count})
            if params.get("returnIdsOnly") == "true":
                return _response({"objectIds": list(range(1, 113))})
            features = []
            for oid in range(1, 113 - int(omit_last_feature)):
                properties = {name: None for name in EXPECTED_FIELDS}
                properties.update(
                    OBJECTID=oid,
                    Object_Name=f"pipeline-{oid}",
                    Feature_ID=f"feature-{oid}",
                    Shape__Area=1.0,
                    Shape__Length=1.0,
                )
                features.append(
                    {
                        "type": "Feature",
                        "id": oid,
                        "properties": properties,
                        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
                    }
                )
            return _response(
                {
                    "type": "FeatureCollection",
                    "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                    "features": features,
                }
            )
        raise AssertionError(f"Unexpected request: {request.url}")

    return httpx.Client(transport=httpx.MockTransport(handler))


class UsacePipelineAreasSourceTest(unittest.TestCase):
    def test_complete_native_geojson_capture_and_receipt(self) -> None:
        with _client() as client:
            body, receipt = extract_pipeline_areas(client)
        collection = json.loads(body)
        self.assertEqual(len(collection["features"]), 112)
        self.assertEqual(receipt["feature_count"], 112)
        self.assertEqual(receipt["objectids_sha256"], EXPECTED_OBJECTIDS_SHA256)
        self.assertEqual(receipt["geojson_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(collection["features"][0]["properties"]["OBJECTID"], 1)
        self.assertEqual(collection["features"][0]["properties"]["Shape__Area"], 1.0)

    def test_count_drift_refuses_capture(self) -> None:
        with _client(count=113) as client:
            with self.assertRaisesRegex(SourceDriftError, "count drift"):
                extract_pipeline_areas(client)

    def test_incomplete_feature_payload_refuses_capture(self) -> None:
        with _client(omit_last_feature=True) as client:
            with self.assertRaisesRegex(SourceDriftError, "feature response is incomplete"):
                extract_pipeline_areas(client)


if __name__ == "__main__":
    unittest.main()
