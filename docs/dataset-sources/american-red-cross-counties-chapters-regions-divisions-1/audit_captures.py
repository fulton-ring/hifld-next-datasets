"""Replay complete issue #13 raw captures without accepting a source or contract."""
import argparse
import collections
import hashlib
import json
import pathlib

import fiona
import pyarrow.parquet as pq
from fiona.io import MemoryFile
from shapely import from_wkb
from shapely.geometry import shape


def unique_members(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def load(path):
    return json.loads(path.read_bytes(), object_pairs_hook=unique_members,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def exact_ids(values):
    assert all(type(value) is int for value in values), "identifier coercion forbidden"
    assert len(values) == len(set(values)), "duplicate identifier"
    return set(values)


def audit(root):
    evidence = pathlib.Path(__file__).parent / "evidence"
    for name, pin in load(evidence / "capture-manifest.json").items():
        path = root / name
        assert path.stat().st_size == pin["bytes"], name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == pin["sha256"], name
    layer = load(root / "layer5.body")
    assert layer == load(root / "after-layer.body"), "layer/edit changed during capture"
    fields = [field["name"] for field in layer["fields"]]
    count = load(root / "count.body")["count"]
    assert type(count) is int and count == load(root / "after-count.body")["count"] == 3163
    rows = []
    geometries = {}
    page_counts = []
    for offset in range(0, 4000, 1000):
        path = root / f"native-{offset:04}.body"
        body = load(path)
        page = body["features"]
        assert len(page) == min(1000, count - offset)
        assert bool(body.get("exceededTransferLimit")) == (offset + len(page) < count)
        assert body["spatialReference"] == {"wkid": 102100, "latestWkid": 3857}
        page_counts.append(len(page))
        for feature in page:
            assert set(feature["attributes"]) == set(fields)
            assert feature["geometry"] and feature["geometry"].get("rings")
            rows.append(feature["attributes"])
        with MemoryFile(path.read_bytes(), ext=".json") as memory:
            with memory.open(driver="ESRIJSON") as source:
                assert source.crs.to_epsg() == 3857
                for feature in source:
                    oid = feature.properties["OBJECTID"]
                    assert oid not in geometries
                    geometries[oid] = shape(feature.geometry)
    current_ids = exact_ids([row["OBJECTID"] for row in rows])
    assert current_ids == exact_ids(load(root / "ids.body")["objectIds"])
    assert current_ids == exact_ids(load(root / "after-ids.body")["objectIds"])
    assert list(row["OBJECTID"] for row in rows) == sorted(current_ids)
    assert set(geometries) == current_ids
    assert load(root / "after-end.body")["features"] == []
    by_geoid = {row["GeoID"]: row for row in rows}
    assert len(by_geoid) == count
    assert all(type(row["GeoID"]) is str and type(row["FIPS"]) is str for row in rows)
    table = pq.read_table(root / "baseline.body")
    baseline = table.to_pylist()
    assert len(baseline) == 3162
    assert len({row["GeoID"] for row in baseline}) == len(baseline)
    baseline_ids = exact_ids([row["OBJECTID"] for row in baseline])
    assert exact_ids([row["id"] for row in baseline]) == baseline_ids
    assert all(row["id"] == row["OBJECTID"] for row in baseline)
    baseline_names = [field.name for field in table.schema if field.name not in {"geometry", "bbox", "id"}]
    assert len(baseline_names) == len(fields) == 61
    # This is an explicitly diagnostic positional/truncation hypothesis, NOT an output mapping.
    mapping = dict(zip(baseline_names, fields, strict=True))
    assert all(before == after[:10] for before, after in mapping.items())
    baseline_geoid = {row["GeoID"]: row for row in baseline}
    shared = sorted(set(by_geoid) & set(baseline_geoid))
    diffs = {before + " -> " + after: [key for key in shared
             if baseline_geoid[key][before] != by_geoid[key][after]]
             for before, after in mapping.items()}
    geometry_rows = []
    for geoid in shared:
        old = baseline_geoid[geoid]
        new = by_geoid[geoid]
        original = from_wkb(old["geometry"])
        current = geometries[new["OBJECTID"]]
        geometry_rows.append({"GeoID": geoid, "baseline_OBJECTID": old["OBJECTID"],
                              "source_OBJECTID": new["OBJECTID"],
                              "ordered_exact": original.equals_exact(current, 0),
                              "normalized_exact": original.equals_exact(current, 0, normalize=True),
                              "topological_exact": original.equals(current),
                              "baseline_valid": original.is_valid, "source_valid": current.is_valid,
                              "baseline_type": original.geom_type, "source_type": current.geom_type})
    return {
        "status": "external_source_authority_and_vintage_decision_required",
        "promotion_allowed": False,
        "baseline": {"rows": len(baseline), "schema": str(table.schema),
                     "geometry_metadata": json.loads(table.schema.metadata[b"geo"]),
                     "id_equals_OBJECTID_all_rows": True,
                     "geometry_types": dict(collections.Counter(from_wkb(row["geometry"]).geom_type for row in baseline)),
                     "invalid_geometry_count": sum(not from_wkb(row["geometry"]).is_valid for row in baseline),
                     "distinct_FIPS": len({row["FIPS"] for row in baseline}),
                     "state_counts": dict(sorted(collections.Counter(row["County_ST"] for row in baseline).items()))},
        "candidate": {"rows": count, "source_fields": layer["fields"],
                      "page_counts": page_counts, "complete_id_census": True,
                      "stable_before_after_schema_edits_count_ids": True,
                      "native_crs": layer["extent"]["spatialReference"],
                      "geometry_types": dict(collections.Counter(g.geom_type for g in geometries.values())),
                      "invalid_geometry_count": sum(not g.is_valid for g in geometries.values()),
                      "invalid_OBJECTIDs": sorted(oid for oid,g in geometries.items() if not g.is_valid),
                      "distinct_FIPS": len({row["FIPS"] for row in rows}),
                      "state_counts": dict(sorted(collections.Counter(row["County_ST"] for row in rows).items())),
                      "identifier_stats": {key: {"nulls": sum(row[key] is None for row in rows),
                                               "distinct": len({row[key] for row in rows})}
                                           for key in ["OBJECTID", "GeoID", "FIPS", "ECODE", "RCODE", "DCODE"]}},
        "comparison": {"join": "exact original GeoID values, diagnostics only; no acceptance or remapping",
                       "shared_GeoID_count": len(shared),
                       "baseline_only": [baseline_geoid[key] | {"geometry": None} for key in sorted(set(baseline_geoid)-set(by_geoid))],
                       "source_only": [by_geoid[key] for key in sorted(set(by_geoid)-set(baseline_geoid))],
                       "diagnostic_truncation_mapping": mapping,
                       "all_attribute_differences_by_GeoID": diffs,
                       "all_geometry_comparisons": geometry_rows,
                       "ordered_exact_geometry_count": sum(row["ordered_exact"] for row in geometry_rows),
                       "normalized_exact_geometry_count": sum(row["normalized_exact"] for row in geometry_rows),
                       "topological_exact_geometry_count": sum(row["topological_exact"] for row in geometry_rows)},
        "tools": {"fiona": fiona.__version__, "gdal": fiona.__gdal_version__}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("capture_directory", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    rendered = json.dumps(audit(args.capture_directory), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered)
