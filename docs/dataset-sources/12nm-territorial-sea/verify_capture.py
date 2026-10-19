"""Offline research audit of anonymous responses; never stages or publishes data.

Usage: uv run python docs/dataset-sources/12nm-territorial-sea/verify_capture.py CAPTURE_DIR
Capture response bodies under the keys in evidence/requests.json. See the dated
research note for reproduction and limits. Requires the repository environment.
"""

import collections
import datetime
import hashlib
import json
import sys
from pathlib import Path

from shapely.geometry import shape


def audit(root):
    def read(key):
        value = json.loads((root / key).read_text())
        if "error" in value:
            raise ValueError(f"ArcGIS error in {key}: {value['error']['code']}")
        return value

    geojson = read("geojson")
    features = geojson["features"]
    ids = read("ids")["objectIds"]
    ids_after = read("ids-after")["objectIds"]
    count = read("count")["count"]
    count_final = read("count-final")["count"]
    page_keys = ["page-0", "page-25", "ids-chunk-50", "page-75"]
    pages = [read(key) for key in page_keys]
    rows = [row for page in pages for row in page["features"]]
    raw_ids = [row["attributes"]["OBJECTID"] for row in rows]
    geo_ids = [feature["properties"]["OBJECTID"] for feature in features]
    assert count == count_final == len(ids) == len(set(ids)) == len(features) == len(rows)
    assert len(set(raw_ids)) == len(rows)
    assert set(ids) == set(ids_after) == set(raw_ids) == set(geo_ids)
    boundary_ids = [feature["properties"]["BOUND_ID"] for feature in features]
    assert all(isinstance(value, str) and len(value) == 5 for value in boundary_ids)
    assert len(set(boundary_ids)) == len(features)
    raw_by_id = {row["attributes"]["OBJECTID"]: row for row in rows}
    assert all(feature["id"] == feature["properties"]["OBJECTID"] for feature in features)
    assert all(raw_by_id[feature["id"]]["attributes"] == feature["properties"] for feature in features)
    assert not geojson.get("exceededTransferLimit", False)
    # Offsets 0 and 25 correctly report more data; explicit-ID chunk is complete.
    assert pages[0].get("exceededTransferLimit") is True
    assert pages[1].get("exceededTransferLimit") is True
    assert not pages[2].get("exceededTransferLimit", False)
    assert not pages[3].get("exceededTransferLimit", False)
    assert read("page-100-retry")["features"] == []
    geometries = [shape(feature["geometry"]) for feature in features]
    coordinate_match = all(
        raw_by_id[feature["id"]]["geometry"]["paths"]
        == ([feature["geometry"]["coordinates"]] if feature["geometry"]["type"] == "LineString"
            else feature["geometry"]["coordinates"])
        for feature in features
    )
    assert coordinate_match
    fields = list(features[0]["properties"])
    field_stats = {}
    for field in fields:
        values = [feature["properties"][field] for feature in features]
        stat = {"null_count": sum(value is None for value in values),
                "distinct_non_null": len({value for value in values if value is not None}),
                "json_types": sorted({type(value).__name__ for value in values})}
        if field in ("PUB_DATE", "APPRV_DATE"):
            dates = [datetime.datetime.fromtimestamp(value / 1000, datetime.timezone.utc).date().isoformat()
                     for value in values if value is not None]
            stat.update(min_date=min(dates), max_date=max(dates))
        field_stats[field] = stat
    return {
        "scope": "Anonymous NOAA layer extraction only; no output conversion or accepted-baseline verification",
        "count_before": count, "count_after": count_final,
        "geojson_count": len(features), "reconciled_count": len(rows),
        "object_ids": sorted(ids),
        "boundary_ids": sorted(feature["properties"]["BOUND_ID"] for feature in features),
        "object_id_sets_equal": True, "id_and_OBJECTID_equal": True,
        "all_attributes_equal_across_encodings": True, "all_coordinates_equal_across_encodings": True,
        "page_accounting": [{"key": key, "features": len(page["features"]),
                             "exceededTransferLimit": page.get("exceededTransferLimit")}
                            for key, page in zip(page_keys, pages)],
        "snapshot_consistency": "IDs equal at 03:47 and 03:48 UTC; final 03:49 and 03:50 ID requests returned error 500. Counts stable. No atomic snapshot guarantee.",
        "field_stats": field_stats,
        "regions": dict(sorted(collections.Counter(feature["properties"]["REGION"] for feature in features).items())),
        "geometry_types": dict(collections.Counter(geometry.geom_type for geometry in geometries)),
        "null_geometry_count": sum(feature["geometry"] is None for feature in features),
        "empty_geometry_count": sum(geometry.is_empty for geometry in geometries),
        "invalid_geometry_count": sum(not geometry.is_valid for geometry in geometries),
        "bbox": [min(geometry.bounds[0] for geometry in geometries),
                 min(geometry.bounds[1] for geometry in geometries),
                 max(geometry.bounds[2] for geometry in geometries),
                 max(geometry.bounds[3] for geometry in geometries)],
        "geojson_sha256": hashlib.sha256((root / "geojson").read_bytes()).hexdigest(),
    }


if __name__ == "__main__":
    print(json.dumps(audit(Path(sys.argv[1])), indent=2))
