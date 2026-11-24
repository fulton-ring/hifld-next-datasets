"""Shared streaming GeoParquet candidate writer.

One owner of the GeoParquet 1.1 candidate contract: the ``geo`` schema
metadata is written here in one place, the source CRS is the PROJJSON pyproj
derives from the actual source CRS (EPSG/wkid code or WKT, never a
hand-pasted literal), and one compression and dictionary policy applies to
every candidate. Record batches stream to a local file under the capture
directory with ``ParquetWriter``, so peak memory stays near one batch instead
of the whole layer.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.sources.esri_common import canonical as _canonical
from dagster_hifld.validation import sha256_file

GEO_VERSION = "1.1.0"
COMPRESSION = "zstd"
USE_DICTIONARY = False
CANDIDATE_BATCH_SIZE = 250


def source_crs_projjson(source_crs: int | str) -> dict[str, object]:
    """PROJJSON for a source CRS given as an EPSG/wkid code or a WKT string."""
    if isinstance(source_crs, int):
        crs = pyproj.CRS.from_epsg(source_crs)
    else:
        crs = pyproj.CRS.from_user_input(source_crs)
    return crs.to_json_dict()


def stream_geoparquet_candidate(
    schema: pa.Schema,
    source_crs: int | str | None,
    batches: Iterator[pa.RecordBatch],
    target: Path,
    *,
    geometry_column: str | None = None,
    geometry_types: Sequence[str] | None = None,
) -> tuple[Path, str, int]:
    """Stream record batches to a local GeoParquet candidate file.

    With ``geometry_column`` (plus ``source_crs`` and ``geometry_types``) the
    writer attaches the GeoParquet 1.1 ``geo`` metadata; without it the file
    carries no ``geo`` entry. Attaching ``geo`` merges the key into any
    existing schema metadata (for example a ``hifld:source_esri_fields``
    provenance key) instead of replacing the mapping. Returns the file path,
    its sha256 digest and the row count written.
    """
    if geometry_column is not None:
        if source_crs is None or not geometry_types:
            raise ValueError("candidate_geometry_contract_incomplete")
        if geometry_column not in schema.names:
            raise ValueError("candidate_geometry_column_missing")
        if any(type(kind) is not str for kind in geometry_types):
            raise ValueError("candidate_geometry_types_invalid")
        geo = {
            "version": GEO_VERSION,
            "primary_column": geometry_column,
            "columns": {geometry_column: {
                "encoding": "WKB",
                "geometry_types": list(geometry_types),
                "crs": source_crs_projjson(source_crs),
            }},
        }
        schema = schema.with_metadata({**(schema.metadata or {}), b"geo": _canonical(geo)})
    elif (schema.metadata or {}).get(b"geo") is not None:
        schema = schema.with_metadata({k: v for k, v in schema.metadata.items() if k != b"geo"})
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(target, schema, compression=COMPRESSION, use_dictionary=USE_DICTIONARY)
    rows = 0
    try:
        for batch in batches:
            if not batch.schema.equals(schema):
                raise ValueError("candidate_batch_schema_mismatch")
            writer.write_batch(batch)
            rows += batch.num_rows
    finally:
        writer.close()
    return target, sha256_file(target), rows
