import json
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
