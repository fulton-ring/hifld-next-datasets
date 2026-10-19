"""Replay the full #12 evidence without network access; no acceptance decision.

Usage: PYTHONPATH=src uv run python docs/dataset-sources/american-red-cross-chapter-1/audit_captures.py /workspace/work/hifld-coordinator/12
Requires the complete preserved captures named in capture-manifest.json.
"""
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
                      parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))


def exact_ids(values):
    assert all(type(v) is int for v in values), "identifier coercion forbidden"
    assert len(values) == len(set(values)), "duplicate identifiers"
    return set(values)


def geometries(body):
    result = {}
    with MemoryFile(body, ext=".json") as memory:
        with memory.open(driver="ESRIJSON") as source:
            assert source.crs.to_epsg() == 3857
            for feature in source:
                oid = feature.properties["OBJECTID"]
                assert oid not in result
                result[oid] = shape(feature.geometry)
    return result


def audit(root):
    evidence = pathlib.Path(__file__).parent / "evidence"
    manifest = load(evidence / "capture-manifest.json")
    for name, pin in manifest.items():
        path = root / name
        assert path.stat().st_size == pin["bytes"], name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == pin["sha256"], name
    baseline = pq.read_table(root / "baseline.body")
    base = baseline.to_pylist()
    baseline_geo = json.loads(baseline.schema.metadata[b"geo"])
    chapter = load(root / "current-native.body")
    chapter_rows = [feature["attributes"] for feature in chapter["features"]]
    count = load(root / "current-count.body")["count"]
    assert type(count) is int and count == 226 == len(chapter_rows)
    ids = exact_ids([row["OBJECTID"] for row in chapter_rows])
    assert ids == exact_ids(load(root / "current-ids.body")["objectIds"])
    assert ids == exact_ids(load(root / "chapter-after-ids.body")["objectIds"])
    assert load(root / "chapter-after-count.body")["count"] == count
    layer = load(root / "current-layer3.body")
    assert layer == load(root / "chapter-after-layer.body")
    assert not chapter.get("exceededTransferLimit")
    assert load(root / "current-end.body")["features"] == []
    assert all(set(row) == {f["name"] for f in layer["fields"]} for row in chapter_rows)
    cg = geometries((root / "current-native.body").read_bytes())
    assert set(cg) == ids
    assert len(base) == 56
    exact_ids([row["OBJECTID"] for row in base])
    exact_ids([row["id"] for row in base])
    assert all(row["id"] == row["OBJECTID"] for row in base)
    assert len({row["ST_FIPS"] for row in base}) == 56
    states = load(root / "current-states.body")
    state_rows = [feature["attributes"] for feature in states["features"]]
    state_ids = exact_ids([row["OBJECTID"] for row in state_rows])
    assert state_ids == exact_ids(load(root / "states-ids.body")["objectIds"])
    assert load(root / "states-count.body")["count"] == len(state_rows) == 56
    assert not states.get("exceededTransferLimit")
    assert load(root / "states-end.body")["features"] == []
    assert load(root / "current-layer4.body") == load(root / "states-layer-after.body")
    assert len({row["ST_FIPS"] for row in state_rows}) == 56
    by_state = {row["ST_FIPS"]: row for row in state_rows}
    assert set(by_state) == {row["ST_FIPS"] for row in base}
    sg = geometries((root / "current-states.body").read_bytes())
    common = sorted(set(base[0]) & set(state_rows[0]) - {"geometry", "bbox"})
    diffs = {}
    mapping_diffs = {}
    for key in common:
        diffs[key] = [row["ST_FIPS"] for row in base if row[key] != by_state[row["ST_FIPS"]][key]]
    for before, after in [("Shape__Are", "Shape__Area"), ("Shape__Len", "Shape__Length")]:
        mapping_diffs[before + " -> " + after] = [row["ST_FIPS"] for row in base if row[before] != by_state[row["ST_FIPS"]][after]]
    geometry_diffs = []
    for row in base:
        current = sg[by_state[row["ST_FIPS"]]["OBJECTID"]]
        original = from_wkb(row["geometry"])
        geometry_diffs.append({"ST_FIPS": row["ST_FIPS"], "OBJECTID": row["OBJECTID"],
                               "ordered_exact": original.equals_exact(current, 0),
                               "normalized_exact": original.equals_exact(current, 0, normalize=True),
                               "topological_exact": original.equals(current),
                               "baseline_valid": original.is_valid,
                               "current_valid": current.is_valid,
                               "baseline_type": original.geom_type, "current_type": current.geom_type})
    return {
        "status": "needs_source_and_dataset_identity_decision", "promotion_allowed": False,
        "baseline": {"rows": len(base), "schema": str(baseline.schema),
                     "geometry_metadata": baseline_geo,
                     "published_id_equals_OBJECTID_every_row": True,
                     "state_coverage": sorted(by_state),
                     "invalid_geometry_count": sum(not from_wkb(r["geometry"]).is_valid for r in base)},
        "current_chapters": {"rows": count, "complete_id_census": True,
                             "stable_before_after_schema_edit_count_ids": True,
                             "source_fields": layer["fields"],
                             "geometry_types": dict(collections.Counter(g.geom_type for g in cg.values())),
                             "invalid_geometry_count": sum(not g.is_valid for g in cg.values()),
                             "identifier_counts": {key: {"nulls": sum(r[key] is None for r in chapter_rows),
                                                         "distinct": len({r[key] for r in chapter_rows})}
                                                   for key in ("OBJECTID", "ECODE", "Chapter_ECODE")}},
        "current_states": {"rows": len(state_rows), "complete_id_census": True,
                           "common_attribute_differences_by_ST_FIPS": diffs,
                           "diagnostic_truncated_field_mapping_differences_by_ST_FIPS": mapping_diffs,
                           "all_geometry_comparisons": geometry_diffs,
                           "ordered_exact_geometry_count": sum(r["ordered_exact"] for r in geometry_diffs),
                           "normalized_exact_geometry_count": sum(r["normalized_exact"] for r in geometry_diffs),
                           "topological_exact_geometry_count": sum(r["topological_exact"] for r in geometry_diffs)},
        "interpretation": "All56published rows have exact current States ST_FIPS coverage and nine shared attribute values, not226Chapter membership. This is evidence of a misidentified published candidate, not approved source lineage, geometry tolerance or schema mapping.",
        "tools": {"fiona": fiona.__version__, "gdal": fiona.__gdal_version__},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("capture_directory", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    text = json.dumps(audit(args.capture_directory), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text)
