"""Offline full candidate/source comparison; observations are not acceptance decisions.

Run with the worktree's frozen environment. stdout is deterministic JSON.
No live reads, schema rewrites, ID substitution, normalization or promotion occur.
"""
from collections import Counter
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import shapely
from shapely.geometry import shape

ROOT = Path(__file__).parent
EVIDENCE = ROOT / "evidence" / "network-recovery"


def read(name):
    return json.loads((EVIDENCE / name).read_bytes())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def coordinates(geometry):
    """Compare Esri ring and GeoJSON coordinate multisets without changing values."""
    if "rings" in geometry:
        return Counter(tuple(x) for ring in geometry["rings"] for x in ring)
    def walk(value):
        if value and isinstance(value[0], (int, float)):
            yield tuple(value)
        else:
            for child in value:
                yield from walk(child)
    return Counter(walk(geometry["coordinates"]))


for request in read("requests.json"):
    assert digest(EVIDENCE / (request["name"] + ".body")) == request["sha256"]
manifest = read("baseline-manifest.json")
assert manifest["acceptance"] == "UNAPPROVED_BASELINE_CANDIDATE"
assert digest(EVIDENCE / manifest["capture"]) == manifest["sha256"]
assert digest(EVIDENCE / manifest["catalog_capture"]) == manifest["catalog_sha256"]
assert (EVIDENCE / manifest["capture"]).stat().st_size == manifest["bytes"]
table = pq.read_table(EVIDENCE / manifest["capture"])
baseline = table.to_pylist()
old = read("historical-features.body")
master = json.loads((ROOT / "evidence" / "features.json").read_bytes())
historical = read("historical-geojson.body")["features"]
current = read("master-geojson.body")["features"]
ids = read("historical-ids.body")["objectIds"]
assert len(baseline) == len(historical) == len(current) == 72
assert read("historical-count.body")["count"] == len(ids) == len(set(ids)) == 72
assert not old.get("exceededTransferLimit", False)
assert set(ids) == {x["attributes"]["OBJECTID"] for x in old["features"]}
assert set(ids) == {x["properties"]["OBJECTID"] for x in historical}
for raw, geo in [(old, historical), (master, current)]:
    raw_by_id = {x["attributes"]["OBJECTID"]: x for x in raw["features"]}
    assert len(raw_by_id) == len(geo)
    assert set(raw_by_id) == {x["properties"]["OBJECTID"] for x in geo}
    for feature in geo:
        original = raw_by_id[feature["properties"]["OBJECTID"]]
        assert original["attributes"] == feature["properties"]
        assert coordinates(original["geometry"]) == coordinates(feature["geometry"])

h_by_feature = {x["properties"]["Feature_ID"]: x for x in historical}
m_by_feature = {x["properties"]["Feature_ID"]: x for x in current}
assert len(h_by_feature) == len(m_by_feature) == 72
assert set(h_by_feature) == set(m_by_feature) and None not in h_by_feature
# This is a proposed observation mapping for comparison, never a publishing contract.
proposed_mapping = {
    "Category_o": "Category_of_Airport", "Object_Nam": "Object_Name",
    "Informatio": "Information", "Scale_Mini": "Scale_Minimum",
    "Source_Dat": "Source_Date", "Source_Ind": "Source_Indication",
    "Source_D_1": "Source_Dataset",
}
pairs, unmatched_baseline = [], []
used = set()
for row in sorted(baseline, key=lambda x: x["OBJECTID"]):
    geom = shapely.from_wkb(row["geometry"])
    matches = [x for x in historical if geom.equals(shape(x["geometry"]))]
    assert len(matches) <= 1, "Ambiguous exact geometry matching"
    if not matches:
        unmatched_baseline.append({
            "attributes": {k: v for k, v in row.items() if k not in ("geometry", "bbox")},
            "bounds": list(geom.bounds), "geometry_sha256": hashlib.sha256(row["geometry"]).hexdigest(),
        })
        continue
    feature = matches[0]
    props = feature["properties"]
    used.add(props["Feature_ID"])
    differences = {k: {"published": row[k], "source": props[v]}
                   for k, v in proposed_mapping.items() if row[k] != props[v]}
    pairs.append({"published_OBJECTID": row["OBJECTID"], "published_id": row["id"],
                  "source_OBJECTID": props["OBJECTID"], "source_Feature_ID": props["Feature_ID"],
                  "mapped_attribute_differences": differences})
all_geometries = {
    "published_candidate": [shapely.from_wkb(x["geometry"]) for x in baseline],
    "historical_service": [shape(x["geometry"]) for x in historical],
    "master_service": [shape(x["geometry"]) for x in current],
}
comparison = {
    "acceptance": "NONE; both baseline and mappings remain unapproved",
    "published_sha256": manifest["sha256"], "published_generation": manifest["generation"],
    "counts": {k: len(v) for k, v in all_geometries.items()},
    "geometry_audit": {k: {"invalid": sum(not g.is_valid for g in v), "empty": sum(g.is_empty for g in v),
                           "bounds": list(shapely.total_bounds(v))} for k, v in all_geometries.items()},
    "published_physical_schema": [{"name": f.name, "type": str(f.type), "nullable": f.nullable,
                                   "null_count": table.column(f.name).null_count} for f in table.schema],
    "historical_schema": read("historical-layer.body")["fields"],
    "master_schema": json.loads((ROOT / "evidence" / "layer.json").read_bytes())["fields"],
    "geometry_match_method": "Exact GEOS topological equality for every published polygon against every historical polygon; ignores IDs and ring ordering. No rounding/tolerance applied.",
    "exact_geometry_pairs": pairs,
    "exact_geometry_pair_count": len(pairs),
    "paired_rows_with_different_OBJECTID": sum(x["published_OBJECTID"] != x["source_OBJECTID"] for x in pairs),
    "paired_attribute_difference_counts": dict(Counter(k for x in pairs for k in x["mapped_attribute_differences"])),
    "proposed_mapping_UNAPPROVED": proposed_mapping,
    "unmatched_published": unmatched_baseline,
    "unmatched_source": [{"attributes": h_by_feature[k]["properties"], "bounds": list(shape(h_by_feature[k]["geometry"]).bounds)} for k in sorted(set(h_by_feature) - used)],
    "published_id_equals_OBJECTID_rows": sum(x["id"] == x["OBJECTID"] for x in baseline),
    "Feature_ID_unique_nonnull_rows_each_service": 72,
    "historical_master_attribute_differences": [
        {"Feature_ID": k, "column": c, "historical": a, "master": m_by_feature[k]["properties"].get(c)}
        for k in sorted(h_by_feature) for c, a in h_by_feature[k]["properties"].items()
        if a != m_by_feature[k]["properties"].get(c)
    ],
    "historical_master_geometry": {
        "exactly_equal_rows": sum(shape(h_by_feature[k]["geometry"]).equals(shape(m_by_feature[k]["geometry"])) for k in h_by_feature),
        "maximum_Hausdorff_distance_degrees": max(shape(h_by_feature[k]["geometry"]).hausdorff_distance(shape(m_by_feature[k]["geometry"])) for k in h_by_feature),
        "interpretation": "Small differences observed; no tolerance accepted and no coordinate values rounded or rewritten.",
    },
    "source_date_semantics": "Per-feature Source_Date is not a dataset release date.",
    "limitations": "Exact geometry pairs establish snapshot observations only, not approved identifiers, lineage, stable continuity, or an approved mapping. Matching counts/extents do not imply unchanged membership. Feature_ID absent in published candidate; id cannot reconstruct it. No dataset release verified.",
}
print(json.dumps(comparison, indent=2, allow_nan=False))
