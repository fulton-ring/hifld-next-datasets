"""Exact BSEE Platforms Shapefile capture and native point candidate."""
from __future__ import annotations

import hashlib
import html
import json
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import geopandas as gpd
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "outer-continental-shelf-ocs-drilling-platforms"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "Bureau of Safety and Environmental Enforcement, Office of Technical Data Management, Data Administration Unit"
MAPPING_URL = "https://www.data.bsee.gov/Main/Mapping.aspx"
METADATA_URL = "https://www.data.bsee.gov/Mapping/Files/platform_meta.html"
SOURCE_URL = "https://www.data.bsee.gov/Mapping/Files/platform.zip"
EXPECTED_COUNT = 7317
MAX_BODY_BYTES = 32_000_000
PHYSICAL_FIELDS = ["STRUCTURE_", "COMPLEX_ID", "STRUCTURE1", "INSTALL_DA", "REMOVAL_DA"]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _null(value: object) -> bool:
    return bool(pd.isna(value))


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    import pyogrio
    identity["gdal"] = pyogrio.__gdal_version_string__
    return identity


def _candidate_version(record_sha256: str, schema_sha256: str, evidence: Mapping[str, Mapping[str, str]], identity: Mapping[str, object]) -> str:
    """Include immutable source body and wire bytes, never retrieval times."""
    basis={"record_sha256":record_sha256,"schema_sha256":schema_sha256,
           "evidence":{name:{"body_sha256":item["body_sha256"],"wire_sha256":item["wire_sha256"]} for name,item in sorted(evidence.items())},
           "processing_identity":dict(identity)}
    return "v"+_sha(_canonical(basis))[:32]


def _read_capture(name: str, root: Path) -> tuple[bytes, dict[str, object]]:
    expected_urls = {"mapping_page": MAPPING_URL, "platform_metadata": METADATA_URL, "platform": SOURCE_URL}
    if name not in expected_urls:
        raise ValueError("unexpected_source_capture_name:" + name)
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json")
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("source_capture_symlink_rejected:" + name)
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    receipt = json.loads(receipt_path.read_bytes())
    if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
            or receipt.get("method") != "GET" or receipt.get("url") != expected_urls[name]
            or receipt.get("final_url") != expected_urls[name]):
        raise ValueError("source_capture_receipt_or_bytes_invalid:" + name)
    return body, receipt


def _validate_capture(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[gpd.GeoDataFrame, dict[str, object]]:
    if set(captures) != {"mapping_page", "platform_metadata", "platform"}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    mapping = captures["mapping_page"][0].decode("utf-8", "replace")
    metadata_html = captures["platform_metadata"][0].decode("latin1", "replace")
    metadata = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", metadata_html)))
    if ("/Mapping/Files/platform.zip" not in mapping or "Platforms" not in mapping
            or "Identifies the location of platforms in GOA. All platforms existing in the database are included." not in mapping):
        raise ValueError("official_mapping_product_link_or_scope_drift")
    scope_text = "Only platforms in the Gulf of Mexico, Federal Outer Continental Shelf (OCS) waters are included."
    if ("Publication_Date: 20260302" not in metadata or scope_text not in metadata
            or "Maintenance_and_Update_Frequency: Monthly" not in metadata
            or "Point_and_Vector_Object_Count: 7317" not in metadata
            or "Horizontal_Datum_Name: D North American 1927" not in metadata):
        raise ValueError("official_platform_metadata_contract_drift")
    archive = captures["platform"][0]
    with tempfile.TemporaryDirectory(prefix="bsee-ocs-platform-") as temp:
        root = Path(temp)
        with zipfile.ZipFile(__import__("io").BytesIO(archive)) as zf:
            names = set(zf.namelist())
            if names != {"platform.cpg", "platform.dbf", "platform.prj", "platform.sbn", "platform.sbx", "platform.shp", "platform.shp.xml", "platform.shx"}:
                raise ValueError("source_shapefile_roster_drift")
            if any(Path(n).is_absolute() or ".." in Path(n).parts for n in names):
                raise ValueError("unsafe_source_archive_path")
            dbf = zf.read("platform.dbf")
            zf.extractall(root)
        frame = gpd.read_file(root / "platform.shp")
        import pyogrio
        info = pyogrio.read_info(root / "platform.shp")
    header_bytes = int.from_bytes(dbf[8:10], "little")
    if header_bytes < 33 or (header_bytes - 33) % 32:
        raise ValueError("source_dbf_header_invalid")
    dbf_fields=[]
    for offset in range(32,header_bytes-1,32):
        descriptor=dbf[offset:offset+32]
        dbf_fields.append({"name":descriptor[:11].split(b"\0",1)[0].decode("ascii"),
                           "type":chr(descriptor[11]),"width":descriptor[16],"scale":descriptor[17]})
    expected_dbf=[{"name":"STRUCTURE_","type":"N","width":5,"scale":0},
                  {"name":"COMPLEX_ID","type":"N","width":10,"scale":0},
                  {"name":"STRUCTURE1","type":"C","width":15,"scale":0},
                  {"name":"INSTALL_DA","type":"D","width":8,"scale":0},
                  {"name":"REMOVAL_DA","type":"D","width":8,"scale":0}]
    if dbf_fields!=expected_dbf or int.from_bytes(dbf[4:8],"little")!=EXPECTED_COUNT:
        raise ValueError("source_dbf_physical_schema_drift")
    if list(frame.columns) != [*PHYSICAL_FIELDS, "geometry"]:
        raise ValueError("source_field_roster_drift")
    if list(info.get("ogr_types", [])) != ["OFTInteger", "OFTInteger64", "OFTString", "OFTDate", "OFTDate"]:
        raise ValueError("source_physical_field_type_drift")
    if len(frame) != EXPECTED_COUNT or frame.crs is None or frame.crs.to_epsg() != 4267:
        raise ValueError("source_count_or_crs_drift")
    if frame.geometry.isna().any() or frame.geometry.is_empty.any() or set(frame.geom_type) != {"Point"}:
        raise ValueError("source_geometry_population_or_type_drift")
    if not frame.geometry.is_valid.all():
        raise ValueError("source_invalid_or_nonfinite_geometry")
    if frame["STRUCTURE_"].isna().any() or frame["COMPLEX_ID"].isna().any():
        raise ValueError("source_native_identifier_nulls")
    if frame[["COMPLEX_ID", "STRUCTURE_"]].drop_duplicates().shape[0] != EXPECTED_COUNT:
        raise ValueError("source_native_identifier_pair_collision")
    fields = [
        {"name":"STRUCTURE_", "source_name":"STRUCTURE_NUMBER", "type":"int32", "driver_type":"OFTInteger", "dbf_type":"N", "dbf_width":5, "dbf_scale":0, "source_type":"Short"},
        {"name":"COMPLEX_ID", "source_name":"COMPLEX_ID_NUM", "type":"int64", "driver_type":"OFTInteger64", "dbf_type":"N", "dbf_width":10, "dbf_scale":0, "source_type":"Long"},
        {"name":"STRUCTURE1", "source_name":"STRUCTURE_NAME", "type":"string", "driver_type":"OFTString", "dbf_type":"C", "dbf_width":15, "dbf_scale":0, "source_type":"Text"},
        {"name":"INSTALL_DA", "source_name":"INSTALL_DATE", "type":"date32", "driver_type":"OFTDate", "dbf_type":"D", "dbf_width":8, "dbf_scale":0, "source_type":"Date"},
        {"name":"REMOVAL_DA", "source_name":"REMOVAL_DATE", "type":"date32", "driver_type":"OFTDate", "dbf_type":"D", "dbf_width":8, "dbf_scale":0, "source_type":"Date"},
    ]
    audit = {"source_feature_count":len(frame), "source_fields":fields,
            "source_id_sha256":_sha(_canonical(sorted([[int(a),int(b)] for a,b in zip(frame.COMPLEX_ID,frame.STRUCTURE_)]))),
            "source_schema_sha256":_sha(_canonical(fields)), "source_crs_epsg":4267,
            "geometry_type":"Point", "null_geometry_count":0, "invalid_geometry_count":0,
            "identifier_pair_unique_count":EXPECTED_COUNT,
            "source_record_sha256":_sha(_canonical([[int(a),int(b),None if _null(c) else str(c),None if _null(d) else d.date().isoformat(),None if _null(e) else e.date().isoformat(),bytes(g.wkb).hex()] for a,b,c,d,e,g in frame.itertuples(index=False,name=None)])),
            "bounds":list(map(float,frame.total_bounds)), "source_metadata_publication_date":"2026-03-02",
            "source_metadata_period_end":"2026-04-01", "source_modified_is_not_release_date":True}
    return frame, audit


def _parquet_body(frame: gpd.GeoDataFrame) -> bytes:
    schema = pa.schema([
        pa.field("STRUCTURE_", pa.int32(), nullable=False),
        pa.field("COMPLEX_ID", pa.int64(), nullable=False),
        pa.field("STRUCTURE1", pa.string(), nullable=True),
        pa.field("INSTALL_DA", pa.date32(), nullable=True),
        pa.field("REMOVAL_DA", pa.date32(), nullable=True),
        pa.field("geometry", pa.binary(), nullable=False),
    ])
    columns = {
        "STRUCTURE_": pa.array(frame.STRUCTURE_.tolist(), type=pa.int32()),
        "COMPLEX_ID": pa.array(frame.COMPLEX_ID.tolist(), type=pa.int64()),
        "STRUCTURE1": pa.array([None if _null(v) else str(v) for v in frame.STRUCTURE1.tolist()], type=pa.string()),
        "INSTALL_DA": pa.array([None if _null(v) else v.date() for v in frame.INSTALL_DA.tolist()], type=pa.date32()),
        "REMOVAL_DA": pa.array([None if _null(v) else v.date() for v in frame.REMOVAL_DA.tolist()], type=pa.date32()),
        "geometry": pa.array([bytes(g.wkb) for g in frame.geometry], type=pa.binary()),
    }
    expected = {
        "STRUCTURE_": [int(v) for v in frame.STRUCTURE_.tolist()],
        "COMPLEX_ID": [int(v) for v in frame.COMPLEX_ID.tolist()],
        "STRUCTURE1": [None if _null(v) else str(v) for v in frame.STRUCTURE1.tolist()],
        "INSTALL_DA": [None if _null(v) else v.date() for v in frame.INSTALL_DA.tolist()],
        "REMOVAL_DA": [None if _null(v) else v.date() for v in frame.REMOVAL_DA.tolist()],
        "geometry": [bytes(g.wkb) for g in frame.geometry],
    }
    if any(columns[name].to_pylist() != values for name,values in expected.items()):
        raise ValueError("source_native_field_or_geometry_value_roundtrip_failed")
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"],"crs":pyproj.CRS.from_epsg(4267).to_json_dict(),"edges":"planar"}}}
    sink=pa.BufferOutputStream(); pq.write_table(pa.Table.from_pydict(columns,schema=schema.with_metadata({b"geo":_canonical(geo)})),sink,compression="zstd")
    return sink.getvalue().to_pybytes()


def _live_captures(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures = {}
    with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/bsee-ocs-platforms-draft"}) as client:
        for name, url, limit in [("mapping_page",MAPPING_URL,2_000_000),("platform_metadata",METADATA_URL,2_000_000),("platform",SOURCE_URL,MAX_BODY_BYTES)]:
            body, receipt = capture_get(client,url,root,name,max_bytes=limit)
            captures[name]=(body,receipt)
    return captures


def capture_bsee_ocs_platforms(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, object]:
    """Create or reuse one complete, unapproved source candidate."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    local=Path(storage.local_dir).absolute(); local.mkdir(parents=True,exist_ok=True)
    if ".." in local.parts or any(p.is_symlink() for p in (local,*local.parents)) or shutil.disk_usage(local).free < 256_000_000:
        raise ValueError("unsafe_or_low_space_staging_root")
    root=Path(capture_directory).absolute() if capture_directory else Path(tempfile.mkdtemp(prefix="bsee-ocs-capture-"))
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)): raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True,exist_ok=True)
    captures: dict[str,tuple[bytes,dict[str,object]]]={}
    try:
        if capture_directory:
            for name in ("mapping_page","platform_metadata","platform"): captures[name]=_read_capture(name,root)
        else: captures=_live_captures(root)
        frame,audit=_validate_capture(captures)
        raw_evidence={name:{"body_sha256":_sha(body),"wire_sha256":receipt["wire_sha256"]} for name,(body,receipt) in sorted(captures.items())}
        audit["raw_evidence"]=raw_evidence
        audit["source_evidence_sha256"]=_sha(_canonical(raw_evidence))
        identity=_code_identity()
        parquet=_parquet_body(frame)
        version=_candidate_version(audit["source_record_sha256"],audit["source_schema_sha256"],raw_evidence,identity)
        prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
        raw_objects={f"raw/{name}.body":body for name,(body,_) in captures.items()}
        raw_objects.update({f"raw/{name}.wire":(root/f"{name}.wire").read_bytes() for name in captures})
        receipts={name:receipt for name,(_,receipt) in sorted(captures.items())}
        description="BSEE Platforms point Shapefile for Gulf of Mexico federal OCS waters. Its metadata states updates are monthly and warns GIS coordinates are approximate and not an official record; currentness and release approvals remain pending."
        manifest={"schema_version":1,"approved":False,"title":"Platforms","description":description,"publisher":PUBLISHER,"agency":"Bureau of Safety and Environmental Enforcement","source_url":SOURCE_URL,"license":"https://www.usa.gov/publicdomain/label/1.0/","source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":MAPPING_URL if n=="mapping_page" else METADATA_URL if n=="platform_metadata" else SOURCE_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"https://www.usa.gov/publicdomain/label/1.0/","attribution":PUBLISHER+"; source use and rights review remain pending.","release_date":None,"release_date_unknown_reason":"BSEE page publication/currentness metadata is not independently accepted as a release contract."} for n,r in receipts.items()],"transformations":[{"operation":"BSEE_native_shapefile_to_GeoParquet_candidate","parameters":{"source_fields":"all five physical Shapefile fields retained with source names, values and types; metadata logical names documented","source_identifiers":"STRUCTURE_ and COMPLEX_ID retained unchanged; their pair is unique in this capture but identifier contract remains pending","geometry":"source NAD27 Point WKB retained without reprojection or coordinate repair","candidate_status":"unapproved; freshness, source contract, rights and release pending"}}],"lineage":"Official BSEE Mapping page advertises Platforms shapefile and describes all database platforms in GOA; official metadata states only Gulf of Mexico federal OCS platforms are included, monthly updates, and GIS coordinates are approximate and not an official record.","prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native point coordinates and all source Shapefile attributes retained without generalization; approval gates remain pending.","objects":[{"path":f"geoparquet/{DATASET_SLUG}.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display generalization approved."}}
        validate_release_provenance(manifest); validate_output_policy(manifest)
        audit.update({"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"source_release_accepted":False})
        ready={"approved":False,"version":version,"objects":{f"geoparquet/{DATASET_SLUG}.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_evidence_sha256":audit["source_evidence_sha256"],"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
        outputs={f"geoparquet/{DATASET_SLUG}.parquet":parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n","metadata/candidate_ready.json":_canonical(ready)+b"\n"}
        outputs.update(raw_objects); outputs.update({f"raw/{n}.receipt.json":_canonical(r)+b"\n" for n,r in receipts.items()})
    except Exception as exc:
        receipts={}
        for p in sorted(root.glob("*.receipt.json")):
            try: receipts[p.name.removesuffix(".receipt.json")]=json.loads(p.read_bytes())
            except (ValueError,OSError): receipts[p.name]={"receipt_readback_failed":True}
        roster=[]
        for p in sorted(root.iterdir()):
            if p.is_file(): roster.append({"name":p.name,"bytes":p.stat().st_size,"sha256":sha256_file(p)})
        (root/"capture_manifest.json").write_bytes(_canonical({"complete":False,"error_code":type(exc).__name__,"retained_artifacts":roster,"receipts":receipts})+b"\n")
        if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    fingerprint=_sha(_canonical(identity)); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fingerprint)); payload=bundle.get("payload")
        if not isinstance(payload,dict) or payload.get("source_record_sha256")!=audit["source_record_sha256"] or payload.get("source_schema_sha256")!=audit["source_schema_sha256"]:
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        # Preserve the original receipt timestamps on byte-identical replay.
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],payload["objects"],payload["capture_receipts"])
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,"source_evidence_sha256":audit["source_evidence_sha256"],"release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False}
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,d in descriptors.items():
        snap=storage.object_snapshot(f"{prefix}/{name}")
        if snap is None or snap.size!=d["bytes"] or storage.sha256_key(f"{prefix}/{name}")!=d["sha256"] or storage.object_snapshot(f"{prefix}/{name}")!=snap: raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    (root/"capture_manifest.json").write_bytes(_canonical({"complete":True,"feature_count":EXPECTED_COUNT,"source_evidence_sha256":audit["source_evidence_sha256"],"receipts":receipts})+b"\n")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"candidate_ready":prefix+"/metadata/candidate_ready.json","source_evidence_sha256":audit["source_evidence_sha256"],"release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"capture_directory":str(root)}
