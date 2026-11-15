"""Regressions for the distinct current-USGS uranium/vanadium candidate."""
import importlib.util
import unittest

class CommodityTests(unittest.TestCase):
    def test_exact_case_insensitive_commodity_tokens(self):
        name='dagster_hifld.sources.usgs_critical_mineral_deposits'
        self.assertIsNotNone(importlib.util.find_spec(name), 'current USGS source adapter missing')
        from dagster_hifld.sources.usgs_critical_mineral_deposits import commodity_tokens
        self.assertEqual(commodity_tokens(' REE, Uranium, VANADIUM '), {'ree','uranium','vanadium'})
        self.assertNotIn('uranium',commodity_tokens('uranium-bearing, uranium vanadium'))
        self.assertEqual(commodity_tokens(None),set())

import hashlib
import json
from unittest.mock import patch
import pyarrow as pa
from shapely.geometry import Point

FIELD_NAMES=['Deposit_na','Lat_WGS84','Long_WGS84','State','MinSystem','DepType','CritMin','DepCat','Rank','Prduction1','Prduction2','Prduction3','Resources1','Resources2','Resources3','Resources4','Comments1','Comments2','Comments3','References','Links']

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def fixture_table():
    cols={'OGC_FID':pa.array([0,1,2],pa.int64())}
    for name in FIELD_NAMES:
        typ=pa.float64() if name in ['Lat_WGS84','Long_WGS84'] else pa.int64() if name=='Rank' else pa.string()
        values=[1.,2.,3.] if pa.types.is_floating(typ) else [1,2,3] if pa.types.is_integer(typ) else ['native value',None,'00123']
        if name=='CritMin':values=['Uranium, VANADIUM','uranium','vanadium']
        cols[name]=pa.array(values,type=typ)
    cols['wkb_geometry']=pa.array([Point(-100+i,40+i).wkb for i in range(3)],pa.binary())
    return pa.table(cols)

def fixture_pins(table):
    rows=[{**r,'wkb_geometry':r['wkb_geometry'].hex()} for r in table.to_pylist()]
    return {'EXPECTED_SOURCE_COUNT':3,'EXPECTED_SELECTED_COUNT':3,'EXPECTED_URANIUM_COUNT':2,'EXPECTED_VANADIUM_COUNT':2,'EXPECTED_BOTH_COUNT':1,
            'EXPECTED_SOURCE_RECORD_SHA256':digest(rows),'EXPECTED_SELECTED_RECORD_SHA256':digest(rows),'EXPECTED_SELECTED_ID_SHA256':digest([0,1,2])}

class NativeContractTests(unittest.TestCase):
    def setUp(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits
        self.source=usgs_critical_mineral_deposits

    def test_selection_preserves_every_native_field_value_type_fid_and_wkb(self):
        t=fixture_table()
        self.assertTrue(hasattr(self.source,'select_deposits'),'native selection implementation missing')
        with patch.multiple(self.source,**fixture_pins(t)):
            selected,audit=self.source.select_deposits(t)
        self.assertTrue(selected.equals(t))
        self.assertEqual(audit['source_feature_count'],3)
        self.assertEqual(audit['both_count'],1)

    def test_schema_id_value_and_geometry_drift_fail_closed(self):
        t=fixture_table()
        self.assertTrue(hasattr(self.source,'select_deposits'),'native selection implementation missing')
        changed=[t.drop(['Rank']),t.set_column(0,'OGC_FID',pa.array([0,0,2],pa.int64())),t.set_column(t.schema.get_field_index('Deposit_na'),'Deposit_na',pa.array(['changed',None,'00123'])),t.set_column(t.schema.get_field_index('wkb_geometry'),'wkb_geometry',pa.array([None,Point(0,0).wkb,Point(1,1).wkb],pa.binary()))]
        with patch.multiple(self.source,**fixture_pins(t)):
            for bad in changed:
                with self.subTest(schema=bad.schema):
                    with self.assertRaises(ValueError):self.source.select_deposits(bad)

    def test_metadata_requires_original_usgs_identity_release_and_exact_file(self):
        s=self.source
        self.assertTrue(hasattr(s,'parse_metadata'),'source metadata validation missing')
        file={'name':'critical_mineral_deposits_v3.zip','downloadUri':'https://www.sciencebase.gov/catalog/file/get/6464de5bd34ec179a83d9e6c?f=original','size':272122,'checksum':{'type':'MD5','value':'cd0368daa43c1534f57e52213bc2d5d3'}}
        doc={'id':'6464de5bd34ec179a83d9e6c','title':'Critical mineral deposits of the United States (ver. 3.0, September 2026)','citation':'USGS data release, https://doi.org/10.5066/P9K1HBNT.','contacts':[{'type':'Publisher','name':'U.S. Geological Survey'}],'dates':[{'type':'Publication','dateString':'2023-07-18'},{'type':'lastRevision','dateString':'2026-09-09'}],'files':[file]}
        self.assertEqual(s.parse_metadata(canonical(doc))['download_url'],file['downloadUri'])
        for key,value in [('id','foreign'),('title','historical MRDS'),('contacts',[])]:
            with self.subTest(key=key),self.assertRaises(ValueError):s.parse_metadata(canonical({**doc,key:value}))
        file['downloadUri']='https://mirror.example/data.zip'
        with self.assertRaises(ValueError):s.parse_metadata(canonical(doc))

class SourceCRSTests(unittest.TestCase):
    def test_native_esri_wgs84_wkt_retains_longitude_first_axes(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        self.assertTrue(hasattr(s,'read_source_crs'),'native WKT CRS interpretation missing')
        wkt='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
        crs=s.read_source_crs(wkt.encode())
        self.assertEqual(crs.to_epsg(),4326)
        self.assertEqual(crs.axis_info[0].direction,'east')
        with self.assertRaises(ValueError):s.read_source_crs(b'not a projection')

class CaptureAvailabilityTests(unittest.TestCase):
    def test_capture_adapter_and_module_registration_exist(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        self.assertTrue(hasattr(s,'capture_deposits'),'registered source capture missing')
        self.assertIsNotNone(importlib.util.find_spec('dagster_hifld.assets.usgs_critical_mineral_deposits'))

import io
import struct
import tempfile
from pathlib import Path
import zipfile
import pyproj


def fixture_archive():
    """Small native Shapefile, independently encoded from the declared DBF fields."""
    from dagster_hifld.sources import usgs_critical_mineral_deposits as s
    table=fixture_table()
    rows=table.to_pylist()
    width=1+sum(f[2] for f in s.DBF_FIELDS)
    header=32+32*len(s.DBF_FIELDS)+1
    dbf=bytearray(header)
    dbf[0]=3
    struct.pack_into('<IHH',dbf,4,len(rows),header,width)
    for i,(name,kind,size,scale) in enumerate(s.DBF_FIELDS):
        off=32+32*i
        dbf[off:off+len(name)]=name.encode()
        dbf[off+11]=ord(kind)
        dbf[off+16:off+18]=bytes([size,scale])
    dbf[-1]=13
    for row in rows:
        dbf.extend(b' ')
        for name,kind,size,scale in s.DBF_FIELDS:
            value=row[name]
            text='' if value is None else str(value) if kind!='F' else f'{value:.11f}'
            dbf.extend(text.encode().ljust(size,b' ') if kind=='C' else text.encode().rjust(size,b' '))
    dbf.extend(b'\x1a')
    def shp_header(length):
        value=bytearray(100)
        struct.pack_into('>i',value,0,9994)
        struct.pack_into('>i',value,24,length//2)
        struct.pack_into('<ii',value,28,1000,1)
        struct.pack_into('<dddd',value,36,-100.,40.,-98.,42.)
        return value
    shp=shp_header(100+28*len(rows));shx=shp_header(100+8*len(rows))
    for fid,row in enumerate(rows):
        shp.extend(struct.pack('>ii',fid+1,10)+struct.pack('<idd',1,-100.+fid,40.+fid))
        shx.extend(struct.pack('>ii',(100+28*fid)//2,10))
    parts={s.STEM+'.shp':bytes(shp),s.STEM+'.shx':bytes(shx),s.STEM+'.dbf':bytes(dbf),s.STEM+'.cpg':b'UTF-8',
        s.STEM+'.prj':pyproj.CRS.from_epsg(4326).to_wkt('WKT1_ESRI').encode(),s.STEM+'.sbn':b'',s.STEM+'.sbx':b'',s.STEM+'.shp.xml':b'<metadata/>'}
    return table,parts


def zip_parts(parts):
    sink=io.BytesIO()
    with zipfile.ZipFile(sink,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for name,body in parts.items():z.writestr(name,body)
    return sink.getvalue()


def archive_pins(table,parts):
    body=zip_parts(parts)
    return {**fixture_pins(table),'ARCHIVE_BYTES':len(body),'ARCHIVE_SHA256':hashlib.sha256(body).hexdigest(),
        'ARCHIVE_MD5':hashlib.md5(body).hexdigest(),'MEMBER_SIZES':{n:len(b) for n,b in parts.items()}}


class ArchiveTests(unittest.TestCase):
    def test_full_native_decoding_preserves_values_nulls_types_per_file_fids_and_geometry(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        table,parts=fixture_archive()
        with patch.multiple(s,**archive_pins(table,parts)):
            result,audit=s.decode_archive(zip_parts(parts))
        self.assertEqual(result.to_pylist(),table.to_pylist())
        self.assertEqual(audit['source_feature_count'],3)
        self.assertEqual(result['Deposit_na'].to_pylist(),['native value',None,'00123'])

    def test_truncation_member_schema_and_geometry_changes_are_refused(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        table,parts=fixture_archive()
        with patch.multiple(s,**archive_pins(table,parts)),self.assertRaisesRegex(ValueError,'archive_bytes'):
            s.decode_archive(zip_parts(parts)[:-1])
        changed=dict(parts);changed['surprise.txt']=b'new field'
        pins=archive_pins(table,changed);pins['MEMBER_SIZES']={n:len(b) for n,b in parts.items()}
        with patch.multiple(s,**pins),self.assertRaisesRegex(ValueError,'member_roster'):
            s.decode_archive(zip_parts(changed))
        for suffix,offset,replacement in [('.dbf',32,b'X'),('.shp',108,struct.pack('<i',3)),('.shx',100,struct.pack('>i',999))]:
            changed=dict(parts);body=bytearray(changed[s.STEM+suffix]);body[offset:offset+len(replacement)]=replacement;changed[s.STEM+suffix]=bytes(body)
            with self.subTest(component=suffix),patch.multiple(s,**archive_pins(table,changed)),self.assertRaises(ValueError):
                s.decode_archive(zip_parts(changed))

from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.catalog import summarize_staged_catalog,write_catalog_metadata
from dagster_hifld.source_manifest import load_resolved_source_manifest,snapshot_source_metadata
import os


def captured_fixture(table,parts):
    from dagster_hifld.sources import usgs_critical_mineral_deposits as s
    archive=zip_parts(parts)
    metadata=canonical({'id':s.ITEM_ID,'title':s.TITLE,'citation':'https://doi.org/'+s.DOI,
        'contacts':[{'type':'Publisher','name':'U.S. Geological Survey'}],
        'dates':[{'type':'Publication','dateString':'2023-07-18'},{'type':'lastRevision','dateString':'2026-09-09'}],
        'files':[{'name':s.ARCHIVE_NAME,'downloadUri':'https://www.sciencebase.gov/catalog/file/get/'+s.ITEM_ID+'?f=fixture',
            'size':len(archive),'checksum':{'type':'MD5','value':hashlib.md5(archive).hexdigest()}}]})
    bodies={'metadata_before':metadata,'metadata_after':metadata,'archive':archive}
    return bodies


def fake_get(bodies):
    def capture(client,url,root,name,**kwargs):
        body=bodies[name];hashed=hashlib.sha256(body).hexdigest()
        receipt={'method':'GET','url':url,'final_url':url,'observed_at':'2026-11-15T00:00:00+00:00',
            'status':200,'complete_body':True,'bytes':len(body),'sha256':hashed,
            'wire_bytes':len(body),'wire_sha256':hashed,'content_length':len(body),'content_encoding':None,'error_code':None}
        (root/f'{name}.body').write_bytes(body);(root/f'{name}.wire').write_bytes(body)
        (root/f'{name}.receipt.json').write_bytes(canonical(receipt)+b'\n')
        return body,receipt
    return capture


class CaptureTests(unittest.TestCase):
    def test_complete_capture_catalog_cache_and_publication_refusal(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        table,parts=fixture_archive();bodies=captured_fixture(table,parts)
        with tempfile.TemporaryDirectory() as temporary,patch.multiple(s,**archive_pins(table,parts)),patch.object(s,'capture_get',side_effect=fake_get(bodies)):
            root=Path(temporary);staging=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
            first=s.capture_deposits(staging,capture_directory=str(root/'captures'))
            prefix=f'{s.DATASET_SLUG}/{s.FILE_SLUG}/{first["version"]}'
            original=staging.read_key(prefix+'/metadata/source_manifest.json')
            manifest=json.loads(original)
            self.assertFalse(manifest['approved']);self.assertIn('worldwide',manifest['lineage'])
            snapshot_source_metadata(staging,s.DATASET_SLUG,s.FILE_SLUG,first['version'])
            resolved=load_resolved_source_manifest(staging,s.DATASET_SLUG,s.FILE_SLUG,first['version']).metadata
            summary=summarize_staged_catalog(staging,s.DATASET_SLUG,s.FILE_SLUG,first['version'],s.DATASET_SLUG,source_metadata=resolved)
            self.assertTrue(summary.quality_manifest['quality_check_passed'])
            self.assertEqual(summary.quality_manifest['population_quality_scope'],'all_rows')
            write_catalog_metadata(staging,dataset_slug=s.DATASET_SLUG,file_slug=s.FILE_SLUG,version=first['version'],quality_dict=summary.quality_manifest,dictionary_dict=summary.data_dictionary)
            second=s.capture_deposits(staging,capture_directory=str(root/'captures'))
            self.assertTrue(second['reused']);self.assertEqual(second['version'],first['version'])
            self.assertEqual(staging.read_key(prefix+'/metadata/source_manifest.json'),original)
            published=PublishedStorageResource(use_local=True,local_dir=str(root/'published'),prefix='')
            published.write('existing','accepted','v1','geoparquet/accepted.parquet',b'accepted sentinel')
            before={k:(published.object_snapshot(k),published.read_key(k)) for k in published.list_prefix()}
            with patch.dict(os.environ,{'HIFLD_RELEASE_APPROVALS':''}),self.assertRaises(ReleaseApprovalError):
                _copy_version_files(staging,published,s.DATASET_SLUG,s.FILE_SLUG,first['version'])
            self.assertEqual({k:(published.object_snapshot(k),published.read_key(k)) for k in published.list_prefix()},before)

    def test_incomplete_source_retained_in_quarantine_without_candidate(self):
        from dagster_hifld.sources import usgs_critical_mineral_deposits as s
        table,parts=fixture_archive();bodies=captured_fixture(table,parts);bodies['archive']=bodies['archive'][:-1]
        with tempfile.TemporaryDirectory() as temporary,patch.multiple(s,**archive_pins(table,parts)),patch.object(s,'capture_get',side_effect=fake_get(bodies)):
            root=Path(temporary);staging=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
            with self.assertRaisesRegex(ValueError,'archive_bytes'):s.capture_deposits(staging,capture_directory=str(root/'captures'))
            keys=staging.list_prefix()
            self.assertTrue(keys);self.assertTrue(all(k.startswith('_quarantine/') for k in keys))
            archive_key=next(k for k in keys if k.endswith('/raw/archive.body'))
            self.assertEqual(staging.read_key(archive_key),bodies['archive'])

class WiringTests(unittest.TestCase):
    def test_registration_is_distinct_and_schedule_stopped(self):
        from dagster_hifld.assets import usgs_critical_mineral_deposits as a
        from dagster_hifld.assets.discovery import discovered
        from dagster import DefaultScheduleStatus
        self.assertEqual(a.INGEST_ASSETS[0].key.to_user_string(),'ingest/usgs-critical-mineral-deposits/uranium-vanadium-deposits')
        self.assertIn(a.INGEST_ASSETS[0],discovered('INGEST_ASSETS'))
        self.assertEqual(a.SCHEDULES[0].default_status,DefaultScheduleStatus.STOPPED)
