import json
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb

from dagster_hifld.sources import docks


def test_docks_schema_is_closed_and_population_is_pinned():
    assert len(docks.EXPECTED_SCHEMA) == 47
    assert docks.EXPECTED_SCHEMA[0] == ("OBJECTID", "esriFieldTypeOID")
    assert docks.EXPECTED_COUNT == 20_563
    fields = [{"name": name, "type": kind} for name, kind in docks.EXPECTED_SCHEMA]
    assert docks._schema({"fields": fields}) == fields
    fields[-1] = {**fields[-1], "type": "esriFieldTypeDate"}
    try:
        docks._schema({"fields": fields})
    except ValueError as exc:
        assert str(exc) == "source_schema_drift"
    else:
        raise AssertionError("schema drift must fail closed")


def test_docks_native_candidate_preserves_all_attributes_and_nad83_xy():
    fields = [{"name": name, "type": kind} for name, kind in docks.EXPECTED_SCHEMA]
    attrs = {}
    for name, kind in docks.EXPECTED_SCHEMA:
        if name == "OBJECTID":
            attrs[name] = 17
        elif kind == "esriFieldTypeString":
            attrs[name] = None if name == "REMARKS" else "native-" + name
        elif kind == "esriFieldTypeDouble":
            attrs[name] = 42.25
        else:
            raise AssertionError(kind)
    feature = {"attributes": attrs, "geometry": {"x": -70.25, "y": 42.5}}
    candidate = pq.read_table(pa.BufferReader(docks._parquet(fields, [feature])))
    assert candidate.column_names == [name for name, _ in docks.EXPECTED_SCHEMA] + ["native_x", "native_y", "geometry"]
    for name, _ in docks.EXPECTED_SCHEMA:
        assert candidate[name].to_pylist() == [attrs[name]]
    assert candidate["native_x"].to_pylist() == [-70.25]
    assert candidate["native_y"].to_pylist() == [42.5]
    point = wkb.loads(candidate["geometry"][0].as_py())
    assert (point.x, point.y) == (-70.25, 42.5)
    geo = json.loads(candidate.schema.metadata[b"geo"])
    assert geo["columns"]["geometry"]["crs"]["id"] == {"authority": "EPSG", "code": 4269}


def test_docks_non_null_geometry_must_match_native_coordinate_attributes():
    from dagster_hifld.sources.docks import EXPECTED_SCHEMA
    assert ("LATITUDE", "esriFieldTypeDouble") in EXPECTED_SCHEMA
    assert ("LONGITUDE", "esriFieldTypeDouble") in EXPECTED_SCHEMA
    # Capture rejects geometry/attribute disagreement before candidate creation.
    # The page-level validator is extracted below to make the invariant unit-testable.
    assert docks._coordinates_agree({"LATITUDE": 2.0, "LONGITUDE": 1.0}, {"x": 1.00000001, "y": 2.00000001})
    assert not docks._coordinates_agree({"LATITUDE": 2.1, "LONGITUDE": 1.0}, {"x": 1.0, "y": 2.0})


def test_docks_crs_rejects_conflicting_wkt_and_wkid():
    import pyproj

    nad83_wkt = pyproj.CRS.from_epsg(4269).to_wkt()
    wgs84_wkt = pyproj.CRS.from_epsg(4326).to_wkt()
    docks._validate_crs({"wkid": 4269, "latestWkid": 4269, "wkt": nad83_wkt})
    try:
        docks._validate_crs({"wkid": 4269, "latestWkid": 4269, "wkt": wgs84_wkt})
    except ValueError as exc:
        assert str(exc) == "source_crs_drift"
    else:
        raise AssertionError("a conflicting CRS WKT must fail closed")


def _local_staging(path):
    from dagster_hifld.resources import StagingStorageResource
    return StagingStorageResource(use_local=True, local_dir=str(path), prefix="")


def _seed_replay(staging, name, method, url, request_body=b""):
    body = b'{"ok":true}'
    root = f"{docks.DATASET_SLUG}/{docks.FILE_SLUG}/source/raw"
    staging.write_key(f"{root}/{name}.body", body)
    staging.write_key(f"{root}/{name}.wire", body)
    if request_body:
        staging.write_key(f"{root}/{name}.request.body", request_body)
    receipt = {"complete": True, "status": 200, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
        "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(), "method": method,
        "url": url, "final_url": url, "request_body_bytes": len(request_body),
        "request_body_sha256": hashlib.sha256(request_body).hexdigest() if request_body else None}
    staging.write_key(f"{root}/{name}.receipt.json", docks._canonical(receipt))


def test_docks_refuses_existing_target_without_modifying_it(tmp_path):
    staging = _local_staging(tmp_path)
    key = f"{docks.DATASET_SLUG}/{docks.FILE_SLUG}/vexisting/metadata/keep.json"
    staging.write_key(key, b"preserve me")
    try:
        docks.capture(staging, "vexisting", client=object())
    except ValueError as exc:
        assert str(exc) == "docks_target_version_not_empty"
    else:
        raise AssertionError("existing candidate targets must be rejected")
    assert staging.read_key(key) == b"preserve me"
    assert staging.list_keys(docks.DATASET_SLUG, docks.FILE_SLUG, "vexisting") == [key]


def test_docks_requires_explicit_unprefixed_local_staging(tmp_path):
    from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

    invalid_resources = [
        StagingStorageResource(use_local=False, bucket="remote-bucket", local_dir=str(tmp_path)),
        StagingStorageResource(use_local=True, local_dir=str(tmp_path), prefix="tenant"),
        PublishedStorageResource(use_local=True, local_dir=str(tmp_path)),
    ]
    for staging in invalid_resources:
        try:
            docks.capture(staging, "vfresh", client=object())
        except ValueError as exc:
            assert str(exc) == "docks_requires_explicit_unprefixed_local_staging"
        else:
            raise AssertionError("nonlocal, prefixed, and published stores must be rejected")


def test_docks_replay_binds_exact_get_and_post_request(tmp_path):
    from urllib.parse import urlencode

    staging = _local_staging(tmp_path)
    get_params = {"f": "json", "where": "1=1", "returnCountOnly": "true"}
    get_url = str(__import__("httpx").URL(docks.LAYER_URL + "/query").copy_merge_params(get_params))
    _seed_replay(staging, "count_before", "GET", get_url)
    try:
        docks._request(None, staging, "vtarget", "count_before", docks.LAYER_URL + "/query",
                       {**get_params, "where": "1=2"}, [0],
                       f"{docks.DATASET_SLUG}/{docks.FILE_SLUG}/source")
    except ValueError as exc:
        assert str(exc) == "retained_capture_receipt_mismatch:count_before"
    else:
        raise AssertionError("a self-consistent receipt for a different GET must be rejected")
    assert staging.list_keys(docks.DATASET_SLUG, docks.FILE_SLUG, "vtarget") == []

    post_params = {"f": "json", "objectIds": "1,2", "outFields": "*", "returnGeometry": "true"}
    post_body = urlencode(post_params).encode("ascii")
    page_url = docks.LAYER_URL + "/query"
    _seed_replay(staging, "page_0000000", "POST", page_url, post_body)
    try:
        docks._request(None, staging, "vtarget", "page_0000000", page_url,
                       {**post_params, "objectIds": "1,3"}, [0],
                       f"{docks.DATASET_SLUG}/{docks.FILE_SLUG}/source")
    except ValueError as exc:
        assert str(exc) == "retained_capture_request_mismatch:page_0000000"
    else:
        raise AssertionError("a self-consistent digest for different POST parameters must be rejected")
    assert staging.list_keys(docks.DATASET_SLUG, docks.FILE_SLUG, "vtarget") == []


def test_docks_retains_partial_transport_bytes_and_failure_receipt(tmp_path, monkeypatch):
    from types import SimpleNamespace

    staging = _local_staging(tmp_path)
    docks._reserve_target(staging, "vpartial")

    class ResponseContext:
        def __enter__(self):
            self.response = SimpleNamespace(url=docks.LAYER_URL + "?f=pjson", status_code=200)
            return self.response

        def __exit__(self, *_args):
            return False

    class Client:
        def stream(self, *_args, **_kwargs):
            return ResponseContext()

    def fail_after_partial(_response, *, retain_wire, retain_decoded, **_kwargs):
        retain_wire(b"partial-wire")
        retain_decoded(b"partial-body")
        raise RuntimeError("injected connection reset")

    monkeypatch.setattr(docks, "capture_http_response", fail_after_partial)
    try:
        docks._request(Client(), staging, "vpartial", "layer", docks.LAYER_URL, {"f": "pjson"}, [0])
    except ValueError as exc:
        assert str(exc) == "source_transport_error:RuntimeError"
    else:
        raise AssertionError("transport failures must still fail the capture")

    prefix = f"{docks.DATASET_SLUG}/{docks.FILE_SLUG}/vpartial/quarantine/partial"
    files = staging.list_prefix(prefix)
    receipts = [key for key in files if key.endswith(".receipt.json")]
    assert len(receipts) == 1
    stem = receipts[0].removesuffix(".receipt.json")
    receipt = json.loads(staging.read_key(receipts[0]))
    assert receipt["complete"] is False and receipt["error_type"] == "RuntimeError"
    assert staging.read_key(stem + ".body") == b"partial-body"
    assert staging.read_key(stem + ".wire") == b"partial-wire"
