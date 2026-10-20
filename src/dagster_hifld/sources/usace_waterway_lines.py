"""Unapproved, native FileGDB capture of the BTS NTAD waterway network lines."""
from __future__ import annotations
import hashlib, json, subprocess, os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import httpx
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.arcgis_filegdb import canonical_json, capture_filegdb_replica, extract_filegdb, sha256, stage_filegdb_archive, verify_staged_filegdb
from dagster_hifld.sources.epa_frs_common import write_immutable
from dagster_hifld.validation import sha256_file
DATASET_SLUG="navigable-waterway-network-lines"; FILE_SLUG=DATASET_SLUG
BTS_ITEM_ID="06037f2b23ff401aa1d379c2632a58f6"; USACE_SERVICE_ITEM_ID="ace7645d305647448a84492a3b909d48"
BTS_ITEM_URL=f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
USACE_ITEM_URL=f"https://www.arcgis.com/sharing/rest/content/items/{USACE_SERVICE_ITEM_ID}?f=json"
SERVICE_URL="https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Waterway_Networks/FeatureServer"
LAYER_ID=1; LAYER_URL=f"{SERVICE_URL}/{LAYER_ID}"
COUNT_URL=f"{LAYER_URL}/query?where=1%3D1&returnCountOnly=true&f=json"; IDS_URL=f"{LAYER_URL}/query?where=1%3D1&returnIdsOnly=true&f=json"
EXPECTED_COUNT=6859
OBSERVED_ID_ROSTER_SHA256="09538edf8a7c443674802f19e3c6bf5d93b89dbea693c45aa7594ebdc45e2efb"
OBSERVED_COVERAGE={"xmin":-179.99999999999997,"ymin":-56.022125452999944,"xmax":180.0000000000001,"ymax":72.19972000000007}
EXPECTED_FIELD_TYPES=("esriFieldTypeOID","esriFieldTypeInteger","esriFieldTypeDouble","esriFieldTypeSmallInteger","esriFieldTypeDouble","esriFieldTypeInteger","esriFieldTypeInteger","esriFieldTypeString","esriFieldTypeString","esriFieldTypeDouble","esriFieldTypeDouble","esriFieldTypeDouble","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeInteger","esriFieldTypeString","esriFieldTypeInteger","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeString","esriFieldTypeBlob","esriFieldTypeString","esriFieldTypeString","esriFieldTypeDouble","esriFieldTypeDouble","esriFieldTypeDouble")
EXPECTED_FIELDS=("OBJECTID","ID","LENGTH","DIR","LINKNUM","ANODE","BNODE","LINKNAME","RIVERNAME","AMILE","BMILE","LENGTH1","LENGTH_SRC","SHAPE_SRC","LINKTYPE","WATERWAY","GEO_CLASS","FUNC_CLASS","WTWY_TYPE","CHART_ID","NUM_PAIRS","WHO_MOD","DATE_MOD","HEADING","STATE","FIPS","FIPS2","NON_US","SE_ANNO_CAD_DATA","KEY_ID","WTWY_UNIQ","MIN_MEAS","MAX_MEAS","Shape__Length")

def _capture(client,url,directory,name):
    body_path,wire_path=directory/f"{name}.body",directory/f"{name}.wire"; h,wh=hashlib.sha256(),hashlib.sha256(); n=wn=0
    def put(path,digest,data,wire=False):
        nonlocal n,wn
        with path.open("ab") as f:f.write(data)
        digest.update(data)
        if wire:wn+=len(data)
        else:n+=len(data)
    body_path.unlink(missing_ok=True);wire_path.unlink(missing_ok=True)
    with client.stream("GET",url,headers={"Accept-Encoding":"identity"}) as r:
        result=capture_http_response(r,max_wire_bytes=8_000_000,max_decoded_bytes=8_000_000,reserve=lambda _p:None,
          retain_wire=lambda b:put(wire_path,wh,b,True),retain_decoded=lambda b:put(body_path,h,b))
        final=urlsplit(str(r.url))
    receipt={"method":"GET","url":url,"final_host":final.hostname,"final_path":final.path,"status":result.status_code,
      "complete_body":result.complete,"bytes":n,"sha256":h.hexdigest(),"wire_bytes":wn,"wire_sha256":wh.hexdigest(),"observed_at":datetime.now(timezone.utc).isoformat()}
    (directory/f"{name}.receipt.json").write_bytes(canonical_json(receipt)+b"\n");body=body_path.read_bytes()
    if not result.complete or result.status_code!=200 or len(body)!=n or sha256(body)!=receipt["sha256"]:raise ValueError("incomplete_source_capture:"+name)
    return body,receipt

def _identity():
    root=Path(__file__).parents[3];files={str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath("src/dagster_hifld").rglob("*.py"))}
    files.update({x:sha256_file(root/x) for x in ("pyproject.toml","uv.lock")})
    return {"code_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip(),"files":files,"python":os.sys.version.split()[0]}

def _validate_before(bts,item,service,layer,count,ids):
    if bts.get("id")!=BTS_ITEM_ID or bts.get("owner")!="USDOT_BTS" or (bts.get("url") or "").casefold()!=LAYER_URL.casefold():raise ValueError("BTS_official_item_identity_or_layer_link_drift")
    if item.get("id")!=USACE_SERVICE_ITEM_ID or item.get("owner")!="usace_iwr_cac" or (item.get("url") or "").rstrip("/").casefold()!=SERVICE_URL.casefold():raise ValueError("USACE_actual_service_owner_or_item_drift")
    if service.get("serviceItemId")!=USACE_SERVICE_ITEM_ID or "Extract" not in service.get("capabilities","").split(","):raise ValueError("USACE_extract_capability_or_lineage_drift")
    if "filegdb" not in str(service.get("supportedExportFormats","")).lower().split(","):raise ValueError("USACE_filegdb_export_no_longer_supported")
    if layer.get("id")!=LAYER_ID or layer.get("name")!="Waterway_Network" or layer.get("geometryType")!="esriGeometryPolyline":raise ValueError("USACE_exact_layer_or_geometry_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not True:raise ValueError("USACE_ZM_contract_drift")
    if layer.get("extent",{}).get("spatialReference",{}).get("latestWkid")!=4269:raise ValueError("USACE_CRS_drift")
    fields=layer.get("fields")
    if not isinstance(fields,list) or [x.get("name") for x in fields]!=list(EXPECTED_FIELDS):raise ValueError("USACE_field_roster_drift")
    if tuple(x.get("type") for x in fields)!=EXPECTED_FIELD_TYPES:raise ValueError("USACE_field_type_drift")
    extent=layer.get("extent",{})
    if any(extent.get(key)!=value for key,value in OBSERVED_COVERAGE.items()):raise ValueError("USACE_coverage_extent_drift")
    if next((x for x in fields if x.get("name")=="SE_ANNO_CAD_DATA"),{}).get("type")!="esriFieldTypeBlob":raise ValueError("USACE_blob_contract_drift")
    vals=ids.get("objectIds")
    if count.get("count")!=EXPECTED_COUNT or not isinstance(vals,list) or len(vals)!=EXPECTED_COUNT or len(set(vals))!=EXPECTED_COUNT or any(type(x)is not int for x in vals):raise ValueError("USACE_population_or_ID_drift")
    vals=sorted(vals)
    if sha256(canonical_json(vals))!=OBSERVED_ID_ROSTER_SHA256:raise ValueError("USACE_observed_ID_roster_drift")
    return fields,vals

def capture_usace_waterway_lines(storage:StagingStorageResource,*,capture_directory:str|None=None)->dict:
    """Capture one complete, native, unapproved FileGDB source candidate."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:raise ValueError("explicit_local_staging_resource_required")
    root=Path(capture_directory or "/tmp/hifld-native-reconstruction/178/captures")/uuid4().hex;root.mkdir(parents=True,exist_ok=False);captured={}
    urls={"bts_item":BTS_ITEM_URL,"usace_item":USACE_ITEM_URL,"service_before":SERVICE_URL+"?f=json","layer_before":LAYER_URL+"?f=json","count_before":COUNT_URL,"ids_before":IDS_URL}
    with httpx.Client(follow_redirects=True,timeout=60,headers={"User-Agent":"hifld-next-datasets/usace-waterway-draft"}) as client:
        for name,url in urls.items():captured[name]=_capture(client,url,root,name)
        docs={k:json.loads(v[0]) for k,v in captured.items()};fields,ids=_validate_before(docs["bts_item"],docs["usace_item"],docs["service_before"],docs["layer_before"],docs["count_before"],docs["ids_before"])
        archive_path,archive_receipt,replica_doc,submission_receipt=capture_filegdb_replica(client,SERVICE_URL,LAYER_ID,f"hifld-178-{uuid4().hex}",root)
        captured["replica_submission"]=((root/"replica_submission.body").read_bytes(),submission_receipt);captured["filegdb_export"]=(archive_path.read_bytes(),archive_receipt)
        for name,url in (("layer_after",urls["layer_before"]),("count_after",COUNT_URL),("ids_after",IDS_URL)):captured[name]=_capture(client,url,root,name)
    after={k:json.loads(v[0]) for k,v in captured.items() if k.endswith("_after")}
    if after["layer_after"]!=docs["layer_before"] or after["count_after"].get("count")!=EXPECTED_COUNT or sorted(after["ids_after"].get("objectIds",[]))!=ids:raise ValueError("USACE_source_changed_during_capture")
    archive_sha=archive_receipt["sha256"];version="v"+hashlib.sha256(canonical_json({"archive":archive_sha,"schema":fields,"ids":ids})).hexdigest()[:32]
    import tempfile
    with tempfile.TemporaryDirectory(prefix="hifld-178-",dir="/tmp") as scratch:
        gdb,gdb_root,integrity=extract_filegdb(archive_path,Path(scratch),expected_layer="Waterway_Network",
          expected_property_names=[x for x in EXPECTED_FIELDS if x not in {"OBJECTID","Shape__Length"}]+["SHAPE_Length"],expected_count=EXPECTED_COUNT,expected_ids=ids,expected_zm=True,binary_fields=("SE_ANNO_CAD_DATA",))
        prefix=f"{DATASET_SLUG}/{FILE_SLUG}/{version}";objects=stage_filegdb_archive(storage,prefix,archive_path,gdb,gdb_root)
    archive_descriptor=next(x for x in objects if x["path"]=="raw/source_filegdb.zip");receipts={k:r for k,(_,r) in sorted(captured.items())}
    identity=_identity();fingerprint=hashlib.sha256(canonical_json({"version":version,"identity":identity,"objects":objects})).hexdigest()
    state_key=f"_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}/{fingerprint}.json"
    payload={"processing_identity":identity,"archive_sha256":archive_sha,"objects":objects,"capture_receipts":receipts}
    bundle=canonical_construction_bundle(version,fingerprint,payload)
    sealed=seal_first_construction(storage,state_key,version,fingerprint,bundle)
    replayed=sealed!=bundle
    if replayed:
        original=json.loads(sealed).get("payload",{})
        if any(original.get(key)!=payload[key] for key in ("processing_identity","archive_sha256","objects")):
            raise ValueError("cached_filegdb_first_construction_identity_drift")
        validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
        receipts=original.get("capture_receipts")
        if not isinstance(receipts,dict):raise ValueError("cached_filegdb_original_receipts_missing")
    if not replayed:
        for name,(body,_) in captured.items():
            if name!="filegdb_export":write_immutable(storage,f"{prefix}/raw/{name}.body",body)
    manifest={"schema_version":1,"approved":False,"title":"Navigable Waterway Network Lines",
      "description":"BTS NTAD listed National Waterway Network lines, retained as a native measured FileGDB source candidate. Contract and downstream mapping remain pending.",
      "publisher":"USDOT Bureau of Transportation Statistics (BTS); hosted ArcGIS item owner usace_iwr_cac","agency":"U.S. Army Corps of Engineers (USACE)",
      "source_url":LAYER_URL,"license":"Unknown; rights review pending.","source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":_identity()["code_commit"],
      "inputs":[{"input_id":name,"producer":"BTS NTAD / USACE hosted Feature Service","host":urlsplit(r["url"]).hostname,"role":"official_distributor","canonical_url":LAYER_URL,
         "retrieval_url":r["url"],"observed_final_host":r.get("final_host"),"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],
         "release_date":None,"release_date_unknown_reason":"Portal/service edit timestamps do not establish a named source release date.","license":"Unknown; rights review pending.","attribution":"Provisional attribution is recorded in BTS/USACE portal metadata; rights and attribution review remain pending."} for name,(_,r) in sorted(captured.items())],
      "transformations":[{"operation":"ArcGIS syncModel=none FileGDB export; exact layer 1; no geometry or attribute conversion","parameters":{"source_fields":len(fields),"source_CRS":"EPSG:4269","hasZ":True,"hasM":True,"blob_field":"SE_ANNO_CAD_DATA","feature_count":EXPECTED_COUNT,"archive_sha256":archive_sha,"candidate_status":"unapproved"}}],
      "lineage":"BTS NTAD item 06037f2b23ff401aa1d379c2632a58f6 links layer 1. Service item ace7645d305647448a84492a3b909d48 is owned by usace_iwr_cac. BTS describes the network as periodically updated by USACE and jointly created for BTS, USACE, Census, and USCG. Item and data edit timestamps do not establish a source release date.",
      "unsupported_output_formats":{"geoparquet":"Shared native GeoParquet has no approved preservation contract for source M ordinates and SE_ANNO_CAD_DATA BLOB.","geopackage":"No accepted mapping for all 34 source fields, IDs, measured Z/M lines, and BLOB.","shapefile_zip":"Shapefile cannot preserve the source's measured Z/M geometry, BLOB, and complete field names/types.","pmtiles":"No approved display generalization or loss policy exists for this analytical navigation network."}}
    validate_release_provenance(manifest);validate_output_policy(manifest);provenance_projection(manifest)
    integrity_after=verify_staged_filegdb(storage,prefix,objects)
    write_immutable(storage,f"{prefix}/metadata/source_manifest.json",canonical_json(manifest)+b"\n")
    audit={"source_feature_count":EXPECTED_COUNT,"source_id_sha256":sha256(canonical_json(ids)),"observed_source_id_roster_sha256":OBSERVED_ID_ROSTER_SHA256,"source_schema_sha256":sha256(canonical_json(fields)),"source_fields":fields,"source_field_types":EXPECTED_FIELD_TYPES,"source_extent_observation":OBSERVED_COVERAGE,
      "source_archive_sha256":archive_sha,"source_archive_bytes":archive_descriptor["bytes"],"source_hasZ":True,"source_hasM":True,**integrity,"staged_filegdb_integrity":integrity_after,
      "source_release_date":None,"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"baseline_accepted":False,"capture_receipts":receipts}
    write_immutable(storage,f"{prefix}/metadata/source_audit.json",canonical_json(audit)+b"\n")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"candidate_ready":True,"reused":replayed,
      "source_archive_sha256":archive_sha,"source_archive_bytes":archive_descriptor["bytes"],"native_Z_M_and_BLOB_preserved":True,"source_null_geometry_count":0,
      "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
