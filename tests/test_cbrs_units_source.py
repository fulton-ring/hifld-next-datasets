"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import os
import gzip
import threading
import json
import shutil
import tempfile
import sqlite3
import struct
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.fws_units import fws_cbrs_units_draft_refresh_schedule, fws_cbrs_units
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import cbrs_units as s

CAPTURE=Path(os.environ['HIFLD_CBRS_UNITS_CAPTURE_DIR']) if os.environ.get('HIFLD_CBRS_UNITS_CAPTURE_DIR') else None
REAL_CAPTURE=unittest.skipUnless(CAPTURE is not None,'complete real source requires HIFLD_CBRS_UNITS_CAPTURE_DIR')


def synthetic_captures(storage=None):
    """Parser/state fixtures only: these are NOT official source or a valid ZIP."""
    bodies={'official_zip':b'EXPLICIT_SYNTHETIC_NONARCHIVE',
        'fws_data':b'EXPLICIT_SYNTHETIC_LANDING',
        'fws_download':b'EXPLICIT_SYNTHETIC /sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip',
        'fws_units_metadata':b'EXPLICIT_SYNTHETIC 20241219 CBRS_Polygons'}
    result={name:(body,{'requested_url':s.URLS[name],'final_url':s.URLS[name],'status':200,
        'byte_length':len(body),'sha256':s.sha(body),'observed_at':'2000-01-01T00:00:00+00:00',
        'fixture':'EXPLICIT_SYNTHETIC_NOT_SOURCE_AUTHORITY'}) for name,body in bodies.items()}
    if storage:
        for name,(body,receipt) in result.items():
            s.immutable(storage,f'_raw-captures/{s.SLUG}/{s.sha(body)}/{name}.body',body)
    return result


def synthetic_context(storage):
    captures=synthetic_captures(storage);manifest=s.provenance(captures,prebuilt_body=b'EXPLICIT_SYNTHETIC_POLICY')
    manifest['processing_fingerprint']='1'*64
    return captures,manifest,s.processing_identity()['files']


class CBRSTests(unittest.TestCase):
    def test_small_physical_dbf_preserves_leading_utf8_and_declarations(self):
        # Two fabricated rows, not a reduced real source capture.
        header=32+32*len(s.NATIVE_SCHEMA)+1
        width=1+sum(int(kind.split(':')[1].split('.')[0]) for kind in s.NATIVE_SCHEMA.values())
        body=bytearray(header);body[0]=3;struct.pack_into('<IHH',body,4,2,header,width)
        expected=[]
        for index,(name,kind) in enumerate(s.NATIVE_SCHEMA.items()):
            field=32+32*index;encoded=name.encode();body[field:field+len(encoded)]=encoded
            body[field+11]=ord('C' if kind.startswith('str') else 'F')
            body[field+16]=int(kind.split(':')[1].split('.')[0]);body[field+17]=0 if kind.startswith('str') else 11
        body[header-1]=13
        for i in range(2):
            row={'Unit':'  A'+str(i),'Name':' Démo '+str(i),'Unit_Type':'System Unit',
                 **{name:0.12345678901 for name,kind in s.NATIVE_SCHEMA.items() if kind.startswith('float')}}
            expected.append(row);body.extend(b' ')
            for name,kind in s.NATIVE_SCHEMA.items():
                width=int(kind.split(':')[1].split('.')[0])
                value=row[name].encode('utf-8') if kind.startswith('str') else format(row[name],'.11f').encode('ascii')
                body.extend(value.ljust(width,b' ') if kind.startswith('str') else value.rjust(width,b' '))
        body.extend(b'\x1a');rows,declarations=s.decode_physical_dbf(bytes(body),b'UTF-8')
        self.assertEqual(rows,expected);self.assertEqual(declarations[1]['width'],254)
        for changed in [bytes(body[:-5]),bytes(body[:32]),bytes(body[:32])+b'bad']:
            with self.assertRaises(ValueError):s.decode_physical_dbf(changed,b'UTF-8')
        bad=bytearray(body);bad[32+11]=ord('N')
        with self.assertRaisesRegex(ValueError,'declarations'):s.decode_physical_dbf(bytes(bad),b'UTF-8')

    def test_small_native_writers_schema_geometry_policy_and_gpkg_guards(self):
        rows=[]
        for i in range(2):
            rows.append({'type':'Feature','properties':{'Unit':' A'+str(i),'Name':' Démo '+str(i),'Unit_Type':'System Unit',
                **{name:0.12345678901 for name,kind in s.NATIVE_SCHEMA.items() if kind.startswith('float')},'__source_fid':str(i)},
                'geometry':{'type':'Polygon','coordinates':[[[-71.0,40.0],[-70.0,40.0],[-70.0,41.0],[-71.0,40.0]]]}})
        audit={'bounds':[-71,40,-70,41]};table=s.typed_table(rows,audit)
        self.assertEqual(str(table.schema.field('Fast_Acres').type),'double')
        self.assertFalse(table.schema.field('Name').nullable)
        for item in table.to_pylist():
            self.assertEqual(s.mapping(s.shapely.from_wkb(item['geometry'])),s.mapping(s.shape(rows[int(item['__source_fid'])]['geometry'])))
        parquet=s.derive_native_parquet_bytes(rows,audit);manifest=s.provenance(synthetic_captures(),prebuilt_body=parquet)
        self.assertEqual(manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]['sha256'],s.sha(parquet))
        with tempfile.TemporaryDirectory() as d:
            gp=Path(d,'synthetic.gpkg');s.write_geopackage(gp,rows);s.validate_geopackage_metadata(gp,rows)
            with sqlite3.connect(gp) as db:db.execute('UPDATE gpkg_contents SET srs_id=4326')
            with self.assertRaisesRegex(ValueError,'container_bytes_differ'):s.validate_geopackage_metadata(gp,rows)

    def test_synthetic_complete_private_flow_late_seal_cache_and_permanent_loss(self):
        # Only the source-decoder boundary is synthetic. 942 small generated rows
        # exercise real native writers, reports, private readback, Git/state,
        # cache reconciliation and whole quarantine without a 32MB Git fixture.
        rows=[]
        for i in range(942):
            polygon={'type':'Polygon','coordinates':[[[-71.0,40.0],[-70.0,40.0],[-70.0,41.0],[-71.0,40.0]]]}
            geometry=polygon if i<818 else {'type':'MultiPolygon','coordinates':[polygon['coordinates']]}
            rows.append({'type':'Feature','properties':{'Unit':'T'+str(i),'Name':' EXPLICIT SYNTHETIC '+str(i),
                'Unit_Type':'System Unit' if i<592 else 'Otherwise Protected Area',
                **{name:0.12345678901 for name,kind in s.NATIVE_SCHEMA.items() if kind.startswith('float')},'__source_fid':str(i)},
                'geometry':geometry})
        audit={'bounds':[-71,40,-70,41],'fixture':'EXPLICIT_SYNTHETIC_NOT_AUTHORITY'}
        members={name:b'EXPLICIT_SYNTHETIC_MEMBER' for name in s.MEMBERS}
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=synthetic_captures(storage)
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'inspect_archive',return_value=(rows,audit,members)):
                result=s.stage_cbrs_units_candidate(storage);version=result['version'];prefix=f'{s.SLUG}/{s.SLUG}/{version}'
                root=Path(d,prefix);key=s.construction_record_key(version)
                original=storage.read_key(key);before=s.candidate_inventory(root)
                self.assertEqual(result['feature_count'],942)
                self.assertEqual(s.stage_cbrs_units_candidate(storage),result)
                self.assertEqual(s.candidate_inventory(root),before)
                forged=json.loads(original)
                for item in forged['payload']['captures'].values():item['receipt']['observed_at']='2001-01-01T00:00:00+00:00'
                for item in forged['payload']['manifest']['inputs']:item['retrieved_at']='2001-01-01T00:00:00+00:00'
                Path(d,key).write_bytes(s.canonical_construction_bundle(version,result['processing_fingerprint'],forged['payload']))
                with self.assertRaisesRegex(ValueError,'observed_original_trust_loss'):
                    s.stage_cbrs_units_candidate(storage)
                self.assertFalse(root.exists())
                retained=list(Path(d,'_quarantine',s.SLUG,'versions').glob('*/candidate'))
                self.assertEqual(len(retained),1);self.assertEqual(s.candidate_inventory(retained[0]),before)
                Path(d,key).write_bytes(original)
                for retry in range(2):
                    with self.assertRaisesRegex(ValueError,'permanent_observed'):
                        s.stage_cbrs_units_candidate(storage)
                self.assertFalse(root.exists());self.assertTrue(Path(d,s.incident_directory(version)).is_dir())

    @REAL_CAPTURE
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),942);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['Unit_Type'],{'System Unit':592,'Otherwise Protected Area':350})
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(942)})

    @REAL_CAPTURE
    def test_all_original_binary_dbf_values_and_declarations(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        dbf=members[s.STEM+'.dbf'];header,width=struct.unpack_from('<HH',dbf,8)
        fields=[]
        for position in range(32,header-1,32):
            field=dbf[position:position+32]
            fields.append((field[:11].split(b'\x00')[0].decode('ascii'),chr(field[11]),field[16],field[17]))
        self.assertEqual(fields,[(k,'C' if v.startswith('str') else 'F',int(v.split(':')[1].split('.')[0]),0 if v.startswith('str') else 11) for k,v in s.NATIVE_SCHEMA.items()])
        self.assertNotIn('D',[f[1] for f in fields]);self.assertNotIn('OBJECTID',[f[0] for f in fields])
        for index,row in enumerate(rows):
            self.assertEqual(row['properties']['__source_fid'],str(index));offset=header+index*width+1
            for name,kind,length,decimals in fields:
                original=dbf[offset:offset+length];offset+=length
                expected=original.decode('utf-8').rstrip(' ') if kind=='C' else float(original.decode('ascii'))
                self.assertEqual(row['properties'][name],expected,(index,name))
        self.assertEqual(rows[679]['properties']['Name'],' Brigantine')
        self.assertEqual(rows[680]['properties']['Name'],' Island Beach')
        # Test the general decoder, not just the two names found by review.
        changed=bytearray(dbf);changed[header+1:header+9]=b'  AB    '
        self.assertEqual(s.decode_physical_dbf(bytes(changed),b'UTF-8')[0][0]['Unit'],'  AB')
        self.assertEqual(audit['physical_dbf_fields'][1]['width'],254)
        for bad in [dbf[:-100],bytes(changed[:32]),b'']:
            with self.assertRaises(ValueError):s.decode_physical_dbf(bad,b'UTF-8')

    def test_first_construction_preserves_original_and_denies_timestamp_coforgery(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures,manifest,files=synthetic_context(storage)
            version='v0.0.0-draft-synthetic-original';key=s.construction_record_key(version)
            with s.candidate_lock(storage,f'{s.SLUG}/{s.SLUG}/{version}'):
                first=s.original_construction_bundle(storage,version,captures,manifest,files,create=True)
                later={name:(body,dict(receipt,observed_at='2001-01-01T00:00:00+00:00')) for name,(body,receipt) in captures.items()}
                latest=s.provenance(later,prebuilt_body=b'EXPLICIT_SYNTHETIC_POLICY');latest['processing_fingerprint']='1'*64
                self.assertEqual(s.original_construction_bundle(storage,version,later,latest,files),first)
                original=storage.read_key(key);bundle=json.loads(original)
                for item in bundle['payload']['captures'].values():item['receipt']['observed_at']='2001-01-01T00:00:00+00:00'
                for item in bundle['payload']['manifest']['inputs']:item['retrieved_at']='2001-01-01T00:00:00+00:00'
                Path(d,key).write_bytes(s.canonical_construction_bundle(version,'1'*64,bundle['payload']))
                with self.assertRaisesRegex(ValueError,'construction_'):
                    s.original_construction_bundle(storage,version,later,latest,files)
                Path(d,key).write_bytes(original)
                self.assertEqual(s.original_construction_bundle(storage,version,later,latest,files),first)

    def test_original_bundle_git_authenticity_rejects_forgery_and_wrong_real_commit(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures,manifest,files=synthetic_context(storage)
            version='v0.0.0-draft-synthetic-code';key=s.construction_record_key(version)
            for commit,reason in [('0'*40,'construction_invalid_commit'),('f7669766e2cd9168f06f833b70c64fbbf7747edf','construction_processing_blob_(hash_mismatch|missing)')]:
                changed=copy.deepcopy(manifest);changed['code_commit']=commit
                with self.subTest(commit=commit),self.assertRaisesRegex(ValueError,reason):
                    s.original_construction_bundle(storage,version,captures,changed,files,create=True)
                self.assertFalse(s.state_present(storage,version))
            s.original_construction_bundle(storage,version,captures,manifest,files,create=True)
            original=storage.read_key(key);Path(d,key).unlink()
            for retry in range(2):
                with self.assertRaisesRegex(ValueError,'construction_'):
                    s.original_construction_bundle(storage,version,captures,manifest,files,create=True)
                self.assertFalse(Path(d,key).exists())
            Path(d,key).write_bytes(original)
            self.assertEqual(s.original_construction_bundle(storage,version,captures,manifest,files)[1],manifest)

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_units for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_units_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_units_draft_refresh_schedule.cron_schedule,'0 15 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_units_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_all_receipts_preflight_strict_timestamp_url_pins(self):
        captures=synthetic_captures()
        self.assertEqual(len(s.provenance(captures)['inputs']),4)
        for name in s.URLS:
            for field,value in [('requested_url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19T00:00:00+99:00'),('observed_at',None),('byte_length',True),('sha256','0'*64),('status',True)]:
                changed={n:(body,dict(receipt)) for n,(body,receipt) in captures.items()};changed[name][1][field]=value
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=changed),self.assertRaises((ValueError,TypeError)):
                    try:s.stage_cbrs_units_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
                    finally:self.assertFalse(Path(d,s.SLUG).exists())

    def test_packaged_code_identity_is_manifested_and_validated_without_git(self):
        captures=synthetic_captures()
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'src').mkdir()
            shutil.copytree(Path(s.__file__).parents[1],root/'src/dagster_hifld')
            for name in ('pyproject.toml','uv.lock'):shutil.copy(Path(__file__).parents[1]/name,root/name)
            self.assertFalse((root/'.git').exists())
            identity=s.packaged_processing_identity(root)
            manifest=s.provenance(captures,identity=identity,prebuilt_body=b'EXPLICIT_SYNTHETIC_PARQUET')
            self.assertEqual(manifest['code_identity_scheme'],'sha256-packaged-dagster-hifld-v1')
            self.assertEqual(manifest['code_commit'],identity['code_commit'])
            s.validate_construction_code_identity(root,identity['code_commit'],identity['files'],identity=identity)

    def test_strict_json_error_array_nested_duplicate_nonfinite(self):
        for body in [b'[]',b'{"error":{}}',b'{"a":1,"a":2}',b'{"a":{"b":1,"b":2}}',b'{"a":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

    def test_bad_receipt_exact_bytes_retained_and_no_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            capture=Path(d,'capture');capture.mkdir();body=b'EXPLICIT_SYNTHETIC_ATTEMPTED_BODY'
            (capture/'official_zip.body').write_bytes(body);bad=b'{"malformed":'
            (capture/'official_zip.receipt.json').write_bytes(bad);stage=Path(d,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())

    def test_partial_each_attempt_retained(self):
        class Response:
            status_code=200;url=s.URL;headers={}
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def iter_bytes(self,**kw):yield b'partial';raise httpx.ReadError('reset')
        class Client:
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def stream(self,*a):return Response()
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',return_value=Client()) as c:
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(c.call_count,3);self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
            self.assertFalse(Path(d,s.SLUG).exists())

    def test_atomic_interrupt_and_oserror_never_expose_version(self):
        for failure in [OSError('disk'),KeyboardInterrupt()]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                target='cbrs-units-2/cbrs-units-2/v0.0.0-draft-fixture'
                with patch.object(s.os,'rename',side_effect=failure),self.assertRaises(type(failure)):
                    s.commit_complete_candidate(storage,target,{'geopackage/test.gpkg':b'fixture'}, {'version':'v0.0.0-draft-fixture'},validate_private=lambda root:None,seal=lambda:None)
                self.assertFalse(Path(d,target).exists())
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))

    def test_archive_pin_and_zip_roster_changes_fail_before_decode(self):
        for changed in [b'EXPLICIT_SYNTHETIC_NOT_A_ZIP',b'error',b'']:
            with tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open') as opened,self.assertRaisesRegex(ValueError,'changed_truncated'):
                s.inspect_archive(changed,Path(d))
            opened.assert_not_called()

    @REAL_CAPTURE
    def test_missing_extra_type_id_and_geometry_drift(self):
        body=(CAPTURE/'official_zip.body').read_bytes()
        with tempfile.TemporaryDirectory() as d:
            rows,_,_=s.inspect_archive(body,Path(d))
        class Row(dict):
            def __init__(self,r):
                super().__init__(properties={k:v for k,v in r['properties'].items() if k!='__source_fid'},geometry={'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]})
                self.id=r['properties']['__source_fid']
        class Stream:
            schema={'properties':s.NATIVE_SCHEMA,'geometry':'Polygon'};crs='EPSG:4269'
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def __iter__(self):return iter(self.rows)
        for mode in ['missing','extra','type','null','id','geometry','coverage']:
            mutated=[Row(r) for r in rows]
            if mode=='missing':del mutated[0]['properties']['Shape_Leng']
            if mode=='extra':mutated[0]['properties']['surprise']=1
            if mode=='type':mutated[0]['properties']['Shape_Leng']=20
            if mode=='null':mutated[0]['properties']['Shape_Leng']=None
            if mode=='id':mutated[0].id=mutated[1].id
            if mode=='geometry':mutated[0]['geometry']=None
            if mode=='coverage':mutated[0]['properties']['Unit_Type']='Unreviewed'
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive(body,Path(d))

    def test_atomic_target_does_not_replace_existing_release(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'prior'}, {'version':'v0.0.0-fixture'},validate_private=lambda root:None,seal=lambda:None)
            with self.assertRaises(ValueError):s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'new'}, {'version':'v0.0.0-fixture'},validate_private=lambda root:None,seal=lambda:None)
            self.assertEqual(Path(d,prefix,'geopackage/a.gpkg').read_bytes(),b'prior')

    @REAL_CAPTURE
    def test_cache_metadata_guards_complete_gpkg_and_typed_parquet(self):
        import fiona
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rows,audit,_=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),root)
            gp=root/'source.gpkg'
            s.write_geopackage(gp,rows)
            s.validate_geopackage_metadata(gp,rows)
            original=gp.read_bytes()
            changes=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_geometry_columns SET srs_id=4326", "UPDATE gpkg_contents SET min_x=0", "UPDATE gpkg_spatial_ref_sys SET definition='undefined' WHERE srs_id=4269", "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269", "UPDATE gpkg_geometry_columns SET z=1", 'UPDATE "rtree_'+s.SLUG+'_geom" SET maxx=0 WHERE id=1', 'ALTER TABLE "'+s.SLUG+'" ADD COLUMN injected TEXT']
            for sql in changes:
                gp.write_bytes(original)
                with sqlite3.connect(gp) as db:db.execute(sql)
                with self.subTest(sql=sql),self.assertRaisesRegex(ValueError,'container_bytes_differ_from_original_reconstruction'):
                    s.validate_geopackage_metadata(gp,rows)
            gp.write_bytes(original)
            with sqlite3.connect(gp) as db:
                db.create_function('ST_IsEmpty',1,lambda x:False);db.create_function('ST_MinX',1,lambda x:0);db.create_function('ST_MaxX',1,lambda x:0);db.create_function('ST_MinY',1,lambda x:0);db.create_function('ST_MaxY',1,lambda x:0)
                fid,blob=db.execute('SELECT fid,geom FROM "'+s.SLUG+'" LIMIT 1').fetchone()
                changed=blob[:4]+struct.pack('<i',4326)+blob[8:]
                db.execute('UPDATE "'+s.SLUG+'" SET geom=? WHERE fid=?',(changed,fid))
            with self.assertRaisesRegex(ValueError,'container_bytes_differ_from_original_reconstruction'):
                s.validate_geopackage_metadata(gp,rows)
            table=s.typed_table(rows,audit)
            self.assertEqual(str(table.schema.field('Shore_Mile').type),'double')
            self.assertEqual(table.column('Unit').null_count,0)


    @REAL_CAPTURE
    def test_full_source_reports_sidecars_and_unapproved_claims_are_rederived(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rows,_,_=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),root)
            records=b''.join(s.encode(row) for row in rows)
            expected=s.source_verification_payloads(records)
            for name,body in expected.items():
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
            s.validate_source_verification_payloads(root,records)
            mutations=[('metadata/geopackage_source_fidelity.json',('shared_verifier','populations','baseline','rows'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','populations','candidate','rows'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','retained_rows_compared'),1),
                       ('metadata/geopackage_source_fidelity.json',('shared_verifier','baseline','approved'),True),
                       ('metadata/geopackage_source_fidelity.json',('canonical_full_records_byte_identical',),False),
                       ('metadata/parquet_source_fidelity.json',('shared_verifier','rows'),1),
                       ('metadata/baseline_comparison.json',('approved',),True),
                       ('metadata/baseline_comparison.json',('status',),'pass'),
                       ('metadata/baseline_comparison.json',('acceptance',),'approved')]
            for name,keys,value in mutations:
                changed=json.loads(expected[name]);current=changed
                for key in keys[:-1]:current=current[key]
                current[keys[-1]]=value;(root/name).write_bytes(s.encode(changed))
                with self.subTest(name=name,keys=keys),self.assertRaisesRegex(ValueError,'evidence_or_unapproved_baseline_claim_drift'):
                    s.validate_source_verification_payloads(root,records)
                (root/name).write_bytes(expected[name])
            for name in ['metadata/baseline_field_changes.jsonl','metadata/baseline_differences.jsonl']:
                (root/name).write_bytes(expected[name]+b'altered')
                with self.subTest(sidecar=name),self.assertRaisesRegex(ValueError,'evidence_or_unapproved_baseline_claim_drift'):
                    s.validate_source_verification_payloads(root,records)
                (root/name).write_bytes(expected[name])

    def test_missing_outside_state_journal_precedes_detach_and_blocks_cold_rebase(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures,manifest,files=synthetic_context(storage)
            identity=s.processing_identity();fingerprint=s.sha(s.encode(identity));version='v0.0.0-draft-journal'
            prefix=f'{s.SLUG}/{s.SLUG}/{version}';target=Path(d,prefix)
            target.mkdir(parents=True);(target/'prior.bin').write_bytes(b'EXPLICIT_SYNTHETIC_PRIOR')
            before=s.candidate_inventory(target)
            with patch.object(s,'record_observed_loss',side_effect=OSError('synthetic journal failure')):
                with self.assertRaises(s.IncidentWriteError):
                    s.reuse_existing_candidate(storage,prefix,version,b'',fingerprint,[],{'bounds':[-71,40,-70,41]},captures,identity,{})
            self.assertEqual(s.candidate_inventory(target),before)
            with self.assertRaisesRegex(ValueError,'observed_original_trust_loss'):
                s.reuse_existing_candidate(storage,prefix,version,b'',fingerprint,[],{'bounds':[-71,40,-70,41]},captures,identity,{})
            self.assertFalse(target.exists())
            retained=list(Path(d,'_quarantine',s.SLUG,'versions').glob('*/candidate'))
            self.assertEqual(len(retained),1);self.assertEqual(s.candidate_inventory(retained[0]),before)
            # Even complete removal of every primary outside witness cannot rebase
            # once this negative observation has been durably recorded.
            for retry in range(2):
                with self.assertRaisesRegex(ValueError,'permanent_observed'):
                    s.reuse_existing_candidate(storage,prefix,version,b'',fingerprint,[],{'bounds':[-71,40,-70,41]},captures,identity,{})
            self.assertFalse(s.state_present(storage,version))

    def test_live_transport_counts_wire_bytes_when_server_ignores_identity(self):
        payload=b'complete original decoded bytes '*100
        compressed=gzip.compress(payload)
        seen=[]
        def handler(request):
            self.assertEqual(request.headers['Accept-Encoding'],'identity');seen.append(str(request.url))
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed))
        original_client=httpx.Client
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:original_client(transport=httpx.MockTransport(handler),**kw)):
            captures=s.acquire(StagingStorageResource(local_dir=d),None)
        self.assertEqual(set(seen),set(s.URLS.values()))
        for body,receipt in captures.values():
            self.assertEqual(body,payload);self.assertEqual(receipt['wire_bytes_downloaded'],len(compressed))
            self.assertEqual(receipt['byte_length'],len(payload));self.assertEqual(receipt['content_encoding'],'gzip')

    def test_real_httpx_partial_chunks_retained_without_decode_buffer_loss(self):
        payload=b'every decoded byte received before reset'
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):
                yield payload
                raise httpx.ReadError('synthetic interrupted response')
        def handler(request):return httpx.Response(200,headers={'Content-Length':str(len(payload)+1)},stream=Interrupted())
        original_client=httpx.Client
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=lambda **kw:original_client(transport=httpx.MockTransport(handler),**kw)):
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):
                s.acquire(StagingStorageResource(local_dir=d),None)
            responses=list(Path(d).rglob('response.body'));self.assertEqual(len(responses),3)
            self.assertTrue(all(p.read_bytes()==payload for p in responses))

    def test_late_seal_private_failures_and_short_write_leave_no_outside_binding(self):
        for fault in ['short_write','validation','seal_interrupt']:
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d);version='v0.0.0-draft-private';prefix=f'{s.SLUG}/{s.SLUG}/{version}'
                events=[]
                def validate(root):
                    events.append('validated');self.assertEqual((root/'raw/body').read_bytes(),b'EXPLICIT_SYNTHETIC')
                    if fault=='validation':raise ValueError('synthetic full validation failure')
                def seal():
                    events.append('seal');raise KeyboardInterrupt()
                with patch.object(s.os,'write',return_value=1) if fault=='short_write' else patch.object(s,'sha',wraps=s.sha):
                    with self.assertRaises((ValueError,KeyboardInterrupt)):
                        s.commit_complete_candidate(storage,prefix,{'raw/body':b'EXPLICIT_SYNTHETIC'}, {'version':version},validate_private=validate,seal=seal)
                self.assertFalse(Path(d,prefix).exists());self.assertFalse(s.state_present(storage,version))
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))
                self.assertEqual(events,[] if fault=='short_write' else ['validated'] if fault=='validation' else ['validated','seal'])

    def test_atomic_install_lock_preserves_winning_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);version='v0.0.0-draft-race';prefix=f'{s.SLUG}/{s.SLUG}/{version}'
            held=threading.Event();release=threading.Event();failures=[]
            def validate(root):held.set();self.assertTrue(release.wait(5))
            def writer(body,validator):
                try:s.commit_complete_candidate(storage,prefix,{'raw/body':body},{'version':version},validate_private=validator,seal=lambda:None)
                except ValueError as exc:failures.append(str(exc))
            a=threading.Thread(target=writer,args=(b'first',validate));a.start();self.assertTrue(held.wait(5))
            b=threading.Thread(target=writer,args=(b'second',lambda root:None));b.start();release.set();a.join(5);b.join(5)
            self.assertFalse(a.is_alive() or b.is_alive());self.assertEqual(len(failures),1)
            self.assertEqual(Path(d,prefix,'raw/body').read_bytes(),b'first')

    @REAL_CAPTURE
    def test_native_prebuilt_capability_is_closed_and_pins_corrected_source_text(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        with tempfile.TemporaryDirectory() as d:
            rows,audit,_=s.inspect_archive(captures['official_zip'][0],Path(d))
            body=s.derive_native_parquet_bytes(rows,audit)
        self.assertEqual(len(body),6555610)
        self.assertEqual(s.sha(body),'64f1bb9b98ff27b76bcfbeb8f6dc049815c0a828f022c87f261b1f797707520a')
        manifest=s.provenance(captures,prebuilt_body=body)
        self.assertNotIn('geoparquet',manifest['unsupported_output_formats'])
        self.assertEqual(set(manifest['unsupported_output_formats']),{'shapefile_zip','pmtiles'})
        self.assertEqual(manifest['prebuilt_output_policy']['schema_version'],1)
        self.assertEqual(manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'],
            [{'path':'geoparquet/cbrs-units-2.parquet','sha256':s.sha(body),'bytes':len(body)}])
        self.assertFalse(manifest['approved'])
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='test/test/v0.0.0-native'
            storage.write_key(prefix+'/geoparquet/cbrs-units-2.parquet',body)
            self.assertEqual(s.prebuilt_output_keys(manifest,storage,prefix,'geoparquet'),[prefix+'/geoparquet/cbrs-units-2.parquet'])
            storage.write_key(prefix+'/geoparquet/extra.parquet',b'unapproved shard')
            with self.assertRaisesRegex(ValueError,'roster'):s.prebuilt_output_keys(manifest,storage,prefix,'geoparquet')

    @REAL_CAPTURE
    def test_invalid_native_capability_fails_before_canonical_namespace(self):
        captures={n:((CAPTURE/(n+'.body')).read_bytes(),s.strict_json((CAPTURE/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
        bad={'schema_version':True,'formats':{'geoparquet':{'reason':'invalid bool version','objects':[]}}}
        with tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=captures),patch.object(s,'native_prebuilt_policy',return_value=bad):
            with self.assertRaisesRegex(ValueError,'prebuilt_output_policy'):
                s.stage_cbrs_units_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
            self.assertFalse(Path(d,s.SLUG).exists())

if __name__=='__main__':unittest.main()
