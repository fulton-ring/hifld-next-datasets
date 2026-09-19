"""Synthetic-only Portolan smoke fixture; never use for production records."""
from __future__ import annotations

import tempfile
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.portolan.catalog import AssetRecord, CatalogRecord, build_catalog_sqlite, render_portolan_tree
from dagster_hifld.resources import PublishedStorageResource


def publish_synthetic() -> str:
    storage = PublishedStorageResource.from_env()
    key = "hifld/synthetic/synthetic/v1.0.0/geoparquet/data.parquet"
    table = pa.table({"id": pa.array([1, 2], type=pa.int64()), "name": ["one", "two"]})
    geo = json.dumps({"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": CRS.from_epsg(3857).to_json_dict(), "bbox": [0, 0, 1, 1]}}}, separators=(",", ":")).encode()
    table = table.append_column("geometry", pa.array([b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"]))
    table = table.replace_schema_metadata({b"geo": geo})
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        parquet_path = root / "data.parquet"
        pq.write_table(table, parquet_path)
        data = parquet_path.read_bytes()
        storage.write_key(key, data)
        import hashlib
        record = CatalogRecord("hifld", "synthetic", "synthetic", "v1.0.0", "Synthetic smoke dataset", "Synthetic-only acceptance data.", "spatial", 2, (AssetRecord("geoparquet", "geoparquet", "GeoParquet", key, "application/vnd.apache.parquet", len(data), hashlib.sha256(data).hexdigest(), storage_slug="seaweedfs-acceptance"),), native_crs="EPSG:3857", geometry_column="geometry", geometry_type="Point", feature_id_column="id", native_bbox=(0,0,1,1), crs84_bbox=(0,0,0.000008983,0.000008983), license_href="../../LICENSE.md", provider="HIFLD synthetic acceptance publisher")
        render_portolan_tree(root, (record,))
        scoped_license = root / "hifld/synthetic/LICENSE.md"
        scoped_license.parent.mkdir(parents=True, exist_ok=True)
        scoped_license.write_text("CC0-1.0 synthetic acceptance fixture only.\n", encoding="utf-8")
        database = root / "catalog.sqlite"
        generation = build_catalog_sqlite(database, (record,))
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.name != "catalog.sqlite":
                storage.write_key(str(path.relative_to(root)), path.read_bytes())
        index_key = "_catalog/catalog.sqlite"
        storage.write_key_if_unchanged(index_key, database.read_bytes(), storage.object_snapshot(index_key))
        return generation


if __name__ == "__main__":
    print(publish_synthetic())
