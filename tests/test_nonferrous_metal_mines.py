"""USGS successor candidate fixtures grant no source/contract acceptance."""
import io
import json
import os
import struct
import tarfile
import tempfile
import unittest
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import nonferrous_metal_mines as s

CAPTURE_ARCHIVE_ENV='HIFLD_NONFERROUS_METAL_MINES_TEST_CAPTURE_ARCHIVE'
ARCHIVE=Path(os.environ[CAPTURE_ARCHIVE_ENV]) if os.environ.get(CAPTURE_ARCHIVE_ENV) else None
STATES13=('Alaska','Arizona','California','Colorado','Idaho','Missouri','Montana','Nevada','New Mexico','South Dakota','Tennessee','Utah','Washington')


def subset_rows(count=123):
    """The 123 producer mines-scope rows with pinned counts, states and ranges."""
    rows=[];ordinal=0
    for commodity,expected in s.NONFERROUS_COMMODITY_COUNTS.items():
        for _ in range(expected):
            latitude=31.429+ordinal*(68.070-31.429)/122
            longitude=-162.865+ordinal*(-83.407+162.865)/122
            rows.append({'id':Decimal(ordinal+1),'COMMODITY':commodity,'SITE_NAME':'Synthetic Site %03d'%ordinal,
                         'COMPANY_NA':'Synthetic Company %03d'%ordinal,'STATE_LOCA':STATES13[ordinal%13],
                         'COUNTY':'Synthetic County %02d'%(ordinal%13),'LATITUDE':Decimal('%.5f'%latitude),
                         'LONGITUDE':Decimal('%.5f'%longitude),'PLANT_MIN':'M/P' if ordinal>=108 else 'M'})
            ordinal+=1
    assert len(rows)==count
    return rows


def full_dbf_and_txt():
    """A complete synthetic mineplant DBF/TXT/SHP/SHX with the pinned layout."""
    subset=subset_rows();rows=list(subset)
    for ordinal in range(123,s.MINEPLANT_RECORDS):
        rows.append({'id':Decimal(ordinal+1),'COMMODITY':'Iron' if ordinal%2 else 'Sand and Gravel',
                     'SITE_NAME':'Filler Site %04d'%ordinal,'COMPANY_NA':'Filler Company %04d'%ordinal,
                     'STATE_LOCA':'Ohio','COUNTY':'Filler County','LATITUDE':Decimal('39.00000'),
                     'LONGITUDE':Decimal('-100.00000'),'PLANT_MIN':'P'})
    header=321;width=244;body=bytearray(header+s.MINEPLANT_RECORDS*width+1)
    struct.pack_into('<IHH',body,4,s.MINEPLANT_RECORDS,header,width)
    for ordinal,(name,kind,size,scale) in enumerate(s.MINEPLANT_DBF_FIELDS):
        offset=32+ordinal*32;body[offset:offset+len(name)]=name.encode('ascii')
        body[offset+11]=ord(kind);body[offset+16]=size;body[offset+17]=scale
    body[header-1]=13;body[-1]=26;points=[]
    for ordinal,row in enumerate(rows):
        offset=header+ordinal*width;body[offset]=32;cursor=offset+1
        for name,kind,size,scale in s.MINEPLANT_DBF_FIELDS:
            raw=(str(row[name]) if name!='id' else str(int(row['id']))).encode('ascii')
            body[cursor:cursor+size]=raw.ljust(size,b' ') if kind=='C' else raw.rjust(size,b' ');cursor+=size
        points.append((float(row['LONGITUDE']),float(row['LATITUDE'])))
    shp=bytearray(100+28*len(rows));shx=bytearray(100+8*len(rows))
    for value in (shp,shx):
        struct.pack_into('>i',value,0,9994);struct.pack_into('>i',value,24,len(value)//2)
        struct.pack_into('<ii',value,28,1000,1)
    xs=[point[0] for point in points];ys=[point[1] for point in points]
    for value in (shp,shx):
        struct.pack_into('<4d',value,36,min(xs),min(ys),max(xs),max(ys))
    for ordinal,(x,y) in enumerate(points):
        offset=100+ordinal*28;struct.pack_into('>ii',shp,offset,ordinal+1,10)
        struct.pack_into('<idd',shp,offset+8,1,x,y);struct.pack_into('>ii',shx,100+ordinal*8,offset//2,10)
    txt=['\t'.join(['ID','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN'])]
    for ordinal,row in enumerate(rows):
        txt.append('\t'.join([str(int(row['id'])),row['COMMODITY'],row['SITE_NAME'],row['COMPANY_NA'],
                              row['STATE_LOCA'],row['COUNTY'],'%.5f'%float(row['LATITUDE']),
                              '%.5f'%float(row['LONGITUDE']),row['PLANT_MIN']]))
    return bytes(body),bytes(shp),bytes(shx),('\n'.join(txt)+'\n').encode('ascii'),rows


def atlas_xml():
    commodity_keys=''.join('<themekey>%s</themekey>'%commodity for commodity in s.NONFERROUS_COMMODITIES)
    return ('<?xml version="1.0" encoding="UTF-8"?><metadata><idinfo><citation><citeinfo>'
            '<origin>U.S. Geological Survey</origin><pubdate>200512</pubdate><title>Nonferrous Metal Mines - Direct Download</title>'
            '<pubinfo><pubplace>Reston, VA</pubplace><publish>National Atlas of the United States</publish></pubinfo>'
            '<onlink>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop6x020_nt00023.tar.gz</onlink>'
            '</citeinfo></citation><timeperd><timeinfo><sngdate><caldate>2003</caldate></sngdate></timeinfo></timeperd>'
            '<spdom><bounding><westbc>-163.0</westbc><eastbc>-83.0</eastbc><northbc>68.5</northbc><southbc>30.5</southbc></bounding></spdom>'
            '<keywords><theme><themekey>economy</themekey><themekey>Nonferrous metals</themekey>'
            + commodity_keys +
            '</theme></keywords><accconst>None</accconst></idinfo>'
            '<dataqual><lineage><procstep><procdesc>extraction</procdesc><procdate>200511</procdate></procstep></lineage></dataqual>'
            '<spdoinfo><direct>Vector</direct><ptvctinf><sdtsterm><sdtstype>Entity point</sdtstype><ptvctcnt>123</ptvctcnt></sdtsterm></ptvctinf></spdoinfo>'
            '<spref><horizsys><geograph><geogunit>Decimal degrees</geogunit></geograph><geodetic>'
            '<horizdn>North American Datum of 1983</horizdn></geodetic></horizsys></spref>'
            '<eainfo><detailed><enttyp><enttypl>Nonferrous Metal Mines</enttypl>'
            '<enttypd>Facilities that extract nonferrous metals, or that extract and process nonferrous metals.</enttypd></enttyp>'
            '<attr><attrlabl>Minop6x020</attrlabl><attrdef>Internal feature number</attrdef><rdom><rdommin>1</rdommin><rdommax>123</rdommax></rdom></attr>'
            '<attr><attrlabl>Commodity</attrlabl><attrdef>The mineral(s) produced.</attrdef></attr>'
            '<attr><attrlabl>Plant_min</attrlabl><attrdef>Whether the facility is a plant or a mine.</attrdef>'
            '<edom><edomv>M</edomv></edom><edom><edomv>M/P</edomv></edom></attr>'
            '<attr><attrlabl>Latitude</attrlabl><rdom><rdommin>31.429</rdommin><rdommax>68.070</rdommax></rdom></attr>'
            '<attr><attrlabl>Longitude</attrlabl><rdom><rdommin>-162.865</rdommin><rdommax>-83.407</rdommax></rdom></attr>'
            '</detailed></eainfo><distinfo><stdorder><digform><digtopt><onlinopt><computer><networka>'
            '<networkr>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop6x020_nt00023.tar.gz</networkr>'
            '</networka></computer></onlinopt></digtopt></digform></stdorder></distinfo>'
            '<metainfo><metd>20060713</metd></metainfo></metadata>').encode('utf-8')


def baseline_world():
    """Synthetic published-copy world: 136 published rows over 141 successor rows."""
    subset=subset_rows();plant_rows=[]
    ordinal=1000
    for _ in range(18):
        plant_rows.append({'id':Decimal(ordinal),'COMMODITY':'Copper','SITE_NAME':'Synthetic Plant %04d'%ordinal,
                           'COMPANY_NA':'Synthetic Company %04d'%ordinal,'STATE_LOCA':'Ohio','COUNTY':'Plant County',
                           'LATITUDE':Decimal('39.50000'),'LONGITUDE':Decimal('-101.00000'),'PLANT_MIN':'P'})
        ordinal+=1
    seven=subset+plant_rows
    published=subset+plant_rows[:13]
    return subset,seven,published


def baseline_table(published):
    geometry=[shapely.Point(float(row['LONGITUDE']),float(row['LATITUDE'])).wkb for row in published]
    bbox=pa.array([{'xmin':float(row['LONGITUDE']),'ymin':float(row['LATITUDE']),'xmax':float(row['LONGITUDE']),'ymax':float(row['LATITUDE'])} for row in published],
                  type=pa.struct([pa.field('xmin',pa.float64()),pa.field('ymin',pa.float64()),pa.field('xmax',pa.float64()),pa.field('ymax',pa.float64())]))
    table=pa.Table.from_pylist([{'geometry':geometry[ordinal],'MINERAL_OPERATIONS_ID':float(ordinal+1),
        'COMMODITY':row['COMMODITY'],'SITE_NAME':row['SITE_NAME'],'COMPANY_NAME':row['COMPANY_NA'],
        'STATE':row['STATE_LOCA'],'COUNTY':row['COUNTY'],'LATITUDE':float(row['LATITUDE']),
        'LONGITUDE':float(row['LONGITUDE']),'PLANT_MINE':row['PLANT_MIN']} for ordinal,row in enumerate(published)],
        schema=pa.schema([pa.field('geometry',pa.binary(),nullable=False),pa.field('MINERAL_OPERATIONS_ID',pa.float64()),
                          pa.field('COMMODITY',pa.string()),pa.field('SITE_NAME',pa.string()),pa.field('COMPANY_NAME',pa.string()),
                          pa.field('STATE',pa.string()),pa.field('COUNTY',pa.string()),pa.field('LATITUDE',pa.float64()),
                          pa.field('LONGITUDE',pa.float64()),pa.field('PLANT_MINE',pa.string()),pa.field('bbox',bbox.type)]))
    sink=pa.BufferOutputStream();pq.write_table(table,sink);return sink.getvalue().to_pybytes()


def baseline_geojson(published):
    return (json.dumps({'type':'FeatureCollection','crs':{'type':'name'},'features':[
        {'type':'Feature','id':ordinal+1,'geometry':{'type':'Point','coordinates':[float(row['LONGITUDE']),float(row['LATITUDE'])]},
         'properties':{'OBJECTID':ordinal+1,'MINERAL_OPERATIONS_ID':ordinal+1,'COMMODITY':row['COMMODITY'],'SITE_NAME':row['SITE_NAME'],
                       'COMPANY_NAME':row['COMPANY_NA'],'STATE':row['STATE_LOCA'],'COUNTY':row['COUNTY'],
                       'LATITUDE':float(row['LATITUDE']),'LONGITUDE':float(row['LONGITUDE']),'PLANT_MINE':row['PLANT_MIN']}}
        for ordinal,row in enumerate(published)]},sort_keys=True)+'\n').encode('utf-8')


def captures_for(published):
    return {'baseline_geoparquet':(baseline_table(published),{'url':'x','final_url':'x','status':200,'complete_body':True,'bytes':0,'sha256':'0'*64,'observed_at':'2026-11-13T00:00:00+00:00'}),
            'baseline_geojson':(baseline_geojson(published),{'url':'x','final_url':'x','status':200,'complete_body':True,'bytes':0,'sha256':'0'*64,'observed_at':'2026-11-13T00:00:00+00:00'})}


class StructuralNonferrousMinesTests(unittest.TestCase):
    def setUp(self):
        self.dbf,self.shp,self.shx,self.txt,self.rows=full_dbf_and_txt()
        self.subset=s.mines_rows(self.rows);self.seven=s.seven_commodity_rows(self.rows)

    def test_synthetic_dbf_txt_geometry_decode_exact_decimal_and_identity(self):
        rows=s.decode_mineplant_dbf(self.dbf);self.assertEqual(len(rows),s.MINEPLANT_RECORDS)
        s.crosscheck_mineplant_txt(self.txt,rows)
        geometry=s.decode_points(self.shp,self.shx,s.MINEPLANT_RECORDS);s.verify_mineplant_geometry_values(rows,geometry)
        self.assertEqual(rows[0]['id'],Decimal('1'));self.assertEqual(rows[0]['LATITUDE'],Decimal('31.42900'))
        self.assertEqual(rows[0]['LATITUDE'].as_tuple().exponent,-5)
        self.assertEqual(rows[122]['LATITUDE'],Decimal('68.07000'));self.assertEqual(rows[122]['LONGITUDE'],Decimal('-83.40700'))
        self.assertEqual([row['source_shp_record_number'] for row in rows],[row['source_fid']+1 for row in rows])
        self.assertEqual(self.subset[0]['COMMODITY'],'Beryllium');self.assertEqual(len(self.subset),123)
        self.assertEqual(len(self.seven),123);self.assertEqual(dict(sorted(__import__('collections').Counter(r['COMMODITY'] for r in self.subset).items())),dict(sorted(s.NONFERROUS_COMMODITY_COUNTS.items())))
        self.assertEqual(len({row['STATE_LOCA'] for row in self.subset}),13)

    def test_mineplant_zip_member_roster_failures(self):
        members={'mineplant.dbf':self.dbf,'mineplant.shp':self.shp,'mineplant.shx':self.shx,'mineplant.txt':self.txt,'mineplant.prj':(s.MINEPLANT_PRJ+'\n').encode('ascii')}
        def zipped(changed):
            stream=io.BytesIO()
            with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
                for name,body in changed.items():archive.writestr(name,body)
            return stream.getvalue()
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({**members,'rogue.txt':b'x'}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({k:v for k,v in members.items() if k!='mineplant.shx'}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({**members,'mineplant.txt':members['mineplant.txt'][:-2]}))

    def test_dbf_renamed_extra_type_scale_count_duplicate_and_deleted_fail_closed(self):
        for label in ('renamed','count','deleted','nonASCII','scale','duplicate','tail','truncated'):
            body=bytearray(self.dbf)
            if label=='renamed':body[32]=ord('X')
            elif label=='count':struct.pack_into('<I',body,4,6784)
            elif label=='deleted':body[321]=42
            elif label=='nonASCII':body[321+1+8]=0xE9
            elif label=='scale':body[321+219:321+229]=b'31.4290000'
            elif label=='duplicate':body[321+123*244:321+123*244+9]=body[321+1:321+10]
            elif label=='tail':body[-1]=0
            else:body=body[:-30]
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_mineplant_dbf(bytes(body))

    def test_txt_value_drift_fails_closed(self):
        mutated=self.txt.replace(b'Synthetic Site 001',b'Synthetic Site 00X',1)
        with self.assertRaisesRegex(ValueError,'txt_dbf_value_drift'):s.crosscheck_mineplant_txt(mutated,s.decode_mineplant_dbf(self.dbf))

    def test_native_SHP_SHX_failures(self):
        for label in ('truncated','record','type','index','nan','extra'):
            body=bytearray(self.shp);index=bytearray(self.shx)
            if label=='truncated':body=body[:-28]
            elif label=='record':struct.pack_into('>i',body,100,99)
            elif label=='type':struct.pack_into('<i',body,108,3)
            elif label=='index':struct.pack_into('>i',index,100,51)
            elif label=='nan':struct.pack_into('<d',body,112,float('nan'))
            else:body+=b'junk'
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_points(bytes(body),bytes(index),s.MINEPLANT_RECORDS)

    def test_geometry_attribute_value_tie_fails_closed(self):
        rows=s.decode_mineplant_dbf(self.dbf);geometry=s.decode_points(self.shp,self.shx,s.MINEPLANT_RECORDS)
        s.verify_mineplant_geometry_values(rows,geometry)
        shifted=bytearray(self.shp);record_offset=100+5*28
        struct.pack_into('<d',shifted,record_offset+12,struct.unpack_from('<d',shifted,record_offset+12)[0]+0.01)
        with self.assertRaisesRegex(ValueError,'geometry_attribute_value_drift'):
            s.verify_mineplant_geometry_values(rows,s.decode_points(bytes(shifted),self.shx,s.MINEPLANT_RECORDS))

    def test_subset_selection_fail_closed(self):
        s.mines_rows(self.rows)
        mutated=[dict(row) for row in self.rows]
        mutated[123]['COMMODITY']='Copper';mutated[123]['PLANT_MIN']='M'
        with self.assertRaisesRegex(ValueError,'subset_completeness_drift'):s.mines_rows(mutated)
        mutated=[dict(row) for row in self.rows];mutated[0]['COMMODITY']='Copper'
        with self.assertRaisesRegex(ValueError,'seven_commodity_operations_drift'):s.mines_rows(mutated)
        mutated=[dict(row) for row in self.rows];mutated[122]['LATITUDE']=Decimal('68.06900')
        with self.assertRaisesRegex(ValueError,'coordinate_range_drift'):s.mines_rows(mutated)
        mutated=[dict(row) for row in self.rows];mutated[0]['STATE_LOCA']='Ohio'
        with self.assertRaisesRegex(ValueError,'state_coverage_drift'):s.mines_rows(mutated)

    def test_atlas_scope_pins_declared_producer_scope(self):
        subset=self.subset;scope=s.atlas_scope(atlas_xml(),subset)
        self.assertEqual(scope['declared_entity_points'],123);self.assertTrue(scope['subset_matches_declared_scope'])
        for label,mutation in (('ptvctcnt','<ptvctcnt>124</ptvctcnt>'),('domain','<edomv>P</edomv>'),
                               ('latitude','<rdommin>31.430</rdommin>'),('commodity','<themekey>Iron</themekey>'),
                               ('pubdate','<pubdate>200511</pubdate>'),('enttypd','<enttypd>Facilities that process nonferrous metals.</enttypd>'),
                               ('onlink','<networkr>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop6x020_nt00022.tar.gz</networkr>')):
            mutated=atlas_xml()
            if label=='ptvctcnt':mutated=mutated.replace(b'<ptvctcnt>123</ptvctcnt>',mutation.encode())
            elif label=='domain':mutated=mutated.replace(b'<edom><edomv>M/P</edomv></edom>',b'<edom><edomv>M/P</edomv></edom><edom><edomv>P</edomv></edom>')
            elif label=='latitude':mutated=mutated.replace(b'<rdommin>31.429</rdommin>',mutation.encode())
            elif label=='commodity':mutated=mutated.replace(b'<themekey>Zinc</themekey>',b'<themekey>Zinc</themekey><themekey>Iron</themekey>')
            elif label=='pubdate':mutated=mutated.replace(b'<pubdate>200512</pubdate>',mutation.encode())
            elif label=='enttypd':mutated=mutated.replace(b'<enttypd>Facilities that extract nonferrous metals, or that extract and process nonferrous metals.</enttypd>',mutation.encode())
            else:mutated=mutated.replace(b'minop6x020_nt00023.tar.gz',b'minop6x020_nt00022.tar.gz')
            with self.subTest(label=label),self.assertRaises(ValueError):s.atlas_scope(mutated,subset)

    def test_mineplant_metadata_scope_fails_closed(self):
        xml=('<?xml version="1.0"?><metadata><idinfo><citation><citeinfo>'
             '<title>Active Mines and Mineral Processing Plants in the United States in 2003</title></citeinfo></citation></idinfo>'
             '<eainfo><detailed>'+''.join('<attr><attrlabl>%s</attrlabl></attr>'%name for name in s.MINEPLANT_METADATA_ATTRS)+'</detailed></eainfo>'
             '<metainfo><metd>20201230</metd></metainfo><idinfo><accconst>None</accconst></idinfo></metadata>').encode()
        self.assertEqual(s.mineplant_metadata_scope(xml)['title'],s.MINEPLANT_METADATA_TITLE)
        with self.assertRaisesRegex(ValueError,'title_drift'):s.mineplant_metadata_scope(xml.replace(b'Active Mines',b'Retired Mines'))
        with self.assertRaisesRegex(ValueError,'attribute_roster'):s.mineplant_metadata_scope(xml.replace(b'<attrlabl>PLANT_MIN</attrlabl>',b'<attrlabl>PLANT_MINE</attrlabl>'))

    def test_published_comparison_proves_scope_and_documents_plant_only(self):
        subset,seven,published=baseline_world()
        captures=captures_for(published)
        result,published_ids=s.published_comparison(captures,subset,seven)
        self.assertFalse(result['approved']);self.assertEqual(result['published_rows'],136)
        self.assertEqual(len(result['published_plant_only_cases']),13)
        self.assertEqual(len(result['successor_operations_without_published_row']),5)
        self.assertTrue(all(item['PLANT_MINE']=='P' for item in result['published_plant_only_cases']))
        self.assertEqual(published_ids.num_rows,136)
        self.assertEqual(published_ids.column_names,['published_MINERAL_OPERATIONS_ID','published_OBJECTID','published_geojson_feature_id'])
        table=pq.read_table(pa.BufferReader(captures['baseline_geoparquet'][0]))
        self.assertEqual({column.name for column in table.schema},s.BASELINE_SCHEMA_NAMES)

    def test_published_comparison_fail_closed(self):
        subset,seven,published=baseline_world()
        for label,changed in (('short_roster',published[:-1]),('extra_unmatched',published+[dict(published[0],MINERAL_OPERATIONS_ID=137.0,OBJECTID=137,id=Decimal(9999),SITE_NAME='Unmatched Site')]),
                              ('role_composition',published+[dict(published[0],MINERAL_OPERATIONS_ID=137.0,OBJECTID=137,id=Decimal(9998),SITE_NAME='Extra Plant',PLANT_MIN='P')]),
                              ('subset_scope',None),('identity',None)):
            if label=='identity':
                captures={'baseline_geoparquet':(baseline_table(published),{'url':'x','final_url':'x','status':200,'complete_body':True,'bytes':0,'sha256':'0'*64,'observed_at':'2026-11-13T00:00:00+00:00'}),
                          'baseline_geojson':(baseline_geojson([dict(row,SITE_NAME=row['SITE_NAME']+' X') if ordinal==0 else row for ordinal,row in enumerate(published)]),{'url':'x','final_url':'x','status':200,'complete_body':True,'bytes':0,'sha256':'0'*64,'observed_at':'2026-11-13T00:00:00+00:00'})}
            elif label=='subset_scope':
                captures=captures_for(published);reduced=[row for row in subset if row['COMMODITY']!='Beryllium']
                with self.subTest(label=label),self.assertRaises(ValueError):
                    s.published_comparison(captures,reduced,seven)
                continue
            else:
                captures=captures_for(changed)
            with self.subTest(label=label),self.assertRaises(ValueError):
                s.published_comparison(captures,subset,seven)

    def test_acquire_requires_explicit_package_and_rejects_unsafe_staging(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),None,None)
            with self.assertRaisesRegex(ValueError,'unsafe_input_package_root'):s.acquire(StagingStorageResource(local_dir=tmp),str(Path(tmp)/'missing'),s.PACKAGE)
            for resource in [PublishedStorageResource(local_dir=tmp),StagingStorageResource(bucket='forbidden',use_local=False),StagingStorageResource(local_dir=tmp+'/../unsafe')]:
                with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_nonferrous_metal_mines(resource,capture_dir=str(tmp),package_id=s.PACKAGE)
                acquire.assert_not_called()
            root=Path(tmp)/'linked';root.symlink_to(Path(tmp),target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'aliased'):s.stage_nonferrous_metal_mines(StagingStorageResource(local_dir=str(root)),capture_dir=str(tmp),package_id=s.PACKAGE)

    def test_bounded_input_reads_exact_limit_and_rejects_oversize_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'body';path.write_bytes(b'1234');self.assertEqual(s.bounded_input(path,4),b'1234')
            with self.assertRaises(ValueError):s.bounded_input(path,3)
            linked=Path(tmp)/'linked';linked.symlink_to(path)
            with self.assertRaises(OSError):s.bounded_input(linked,4)
            hard=Path(tmp)/'hard';os.link(path,hard)
            with self.assertRaises(ValueError):s.bounded_input(path,4)


@unittest.skipUnless(ARCHIVE, 'complete original nonferrous-metal-mines captures opt-in required; see docs/dataset-sources/nonferrous-metal-mines/README.md')
class CompleteCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.package_tmp=tempfile.TemporaryDirectory();cls.package=Path(cls.package_tmp.name)
        with tarfile.open(ARCHIVE,'r:gz') as archive:
            for member in archive:
                if not member.isfile() or Path(member.name).name!=member.name:raise ValueError('unsafe test archive')
                (cls.package/member.name).write_bytes(archive.extractfile(member).read())
    @classmethod
    def tearDownClass(cls):cls.package_tmp.cleanup()
    def captures(self):return {name:((self.package/(name+'.body')).read_bytes(),s.strict_json((self.package/(name+'.receipt.json')).read_bytes())) for name in s.PINS}
    def stage(self,storage):return s.stage_nonferrous_metal_mines(storage,capture_dir=str(self.package),package_id=s.PACKAGE)
    def state(self,root,result):return Path(root)/s.construction_record_key(result['version'])

    def test_full123_usgs_native_values_types_ids_geometry_and_published_proof(self):
        captures=self.captures();table,published_ids,audit,comparison,scope=s.inspect_captures(captures)
        self.assertEqual(table.num_rows,123);self.assertEqual(published_ids.num_rows,136)
        self.assertEqual(audit['states'],13);self.assertEqual(scope['declared_entity_points'],123)
        self.assertEqual(table['id'].type,pa.decimal128(8,0));self.assertEqual(table['LATITUDE'].type,pa.decimal128(10,5));self.assertEqual(table['LONGITUDE'].type,pa.decimal128(10,5))
        self.assertEqual(table['source_shp_record_number'].to_pylist(),[fid+1 for fid in table['source_fid'].to_pylist()])
        self.assertEqual(len(set(table['id'].to_pylist())),123)
        self.assertIn('Red Dog Mine',table['SITE_NAME'].to_pylist());self.assertIn('Copper Queen Branch Mine',table['SITE_NAME'].to_pylist())
        self.assertEqual(audit['commodities'],dict(zip(s.NONFERROUS_COMMODITIES,(1,22,2,36,11,38,13))))
        self.assertIn('North_American_1927',audit['native_crs_claim'])
        members=s.decode_mineplant_zip(captures['usgs_mineplant_zip'][0]);raw=members['mineplant.shp']
        for i,fid in enumerate(table['source_fid'].to_pylist()):
            point=s.shapely.from_wkb(table['geometry'][i].as_py())
            self.assertEqual((point.x,point.y),struct.unpack_from('<dd',raw,100+fid*28+12))
            self.assertEqual(point.y,float(table['LATITUDE'][i].as_py()));self.assertEqual(point.x,float(table['LONGITUDE'][i].as_py()))
        self.assertEqual(comparison['published_rows'],136);self.assertEqual(len(comparison['published_plant_only_cases']),13)
        self.assertEqual(len(comparison['successor_operations_without_published_row']),5)
        self.assertEqual(scope['declared_latitude_range'],['31.429','68.070'])

    def test_complete_stage_reuse_and_forgery_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);self.assertFalse(result['reused']);self.assertEqual(result['feature_count'],123);self.assertTrue(self.stage(storage)['reused'])
            root=Path(tmp)/s.SLUG/s.SLUG/result['version']
            self.assertFalse((root/'shapefile').exists());self.assertTrue((root/f'geoparquet/{s.SLUG}.parquet').is_file())
            from dagster_hifld.definitions import _iter_staged_version_paths
            self.assertIn((s.SLUG,s.SLUG,result['version']),list(_iter_staged_version_paths(storage)))
            target=root/'candidate/published-identifiers.parquet';target.write_bytes(b'forged');ready=s.strict_json((root/s.READY).read_bytes());ready['objects']['candidate/published-identifiers.parquet']=s.digest(b'forged');(root/s.READY).write_bytes(s.encode(ready))
            with self.assertRaisesRegex(ValueError,'detached_intact'):self.stage(storage)
            self.assertFalse(root.exists());self.assertTrue(list((Path(tmp)/'_quarantine').rglob('published-identifiers.parquet')))
            self.assertFalse(self.stage(storage)['reused'])

    def test_pinned_drift_and_extra_package_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';package.mkdir();storage=StagingStorageResource(local_dir=tmp+'/stage')
            for name in s.PINS:
                body=self.captures()[name][0][:-1];receipt=dict(self.captures()[name][1],bytes=len(body),sha256=s.digest(body))
                (package/(name+'.body')).write_bytes(body);(package/(name+'.receipt.json')).write_bytes(s.encode(receipt))
            with self.assertRaisesRegex(ValueError,'unreviewed_package_bytes_require_explicit_new_package'):s.acquire(storage,str(package),s.PACKAGE)
            self.assertTrue(list((Path(tmp)/'stage/_quarantine').rglob('usgs_mineplant_zip.body')))
            for name in s.PINS:
                (package/(name+'.body')).write_bytes(self.captures()[name][0]);(package/(name+'.receipt.json')).write_bytes(s.encode(self.captures()[name][1]))
            (package/'extra').write_text('drift')
            with self.assertRaisesRegex(ValueError,'extra_explicit_package_roster'):s.acquire(storage,str(package),s.PACKAGE)


class RegistrationTests(unittest.TestCase):
    def test_asset_job_and_schedule_are_stopped_and_explicit(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets import nonferrous_metal_mines as module
        self.assertEqual(module.nonferrous_metal_mines_candidate.key.to_user_string(),'ingest/nonferrous-metal-mines/nonferrous-metal-mines')
        self.assertIs(module.nonferrous_metal_mines_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(module.nonferrous_metal_mines_draft_schedule.cron_schedule,'0 7 1 * *')
        self.assertEqual(s.SLUG,'nonferrous-metal-mines');self.assertTrue(s.PACKAGE.startswith('usgs-mrdata-mineplant-2003'))
        self.assertEqual(s.INPUT_NAMES,{'usgs_mineplant_zip','usgs_mineplant_metadata','legacy_metadata'})
        self.assertEqual(s.COMPARISON_NAMES,{'baseline_geoparquet','baseline_geojson'})
        for name,pin in s.PINS.items():
            self.assertTrue(pin['url'].startswith('https://') or pin['url'].startswith('http://'),name)
            self.assertGreater(pin['bytes'],0)
        self.assertEqual({urlsplit(pin['url']).hostname for name,pin in s.PINS.items() if name in s.INPUT_NAMES},{'mrdata.usgs.gov','thor-f5.er.usgs.gov'})
