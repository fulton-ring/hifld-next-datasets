"""Complete official NOAA tsunami-capable tide-station draft; no release accepted."""
from __future__ import annotations
import collections
import math
import urllib.parse
import shutil
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file
SLUG='tsunami-capable-tide-stations'
LAYER='NOAA_Tsunami_Capable_Tide_Stations'
LANDING='https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/tide-gauge-data'
ITEM='a3ae1f2bd8374e28900c43a0e69cc1b4'
WEBMAP='5cb326668189408e8c4c20ddd92aa2ed'
SERVICE='https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer'
LAYER_URL=SERVICE+'/0'
LICENSE='https://www.arcgis.com/sharing/rest/content/items/'+ITEM+'?f=pjson'
URLS={'ncei_tsunamis': 'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis', 'ncei_tide': 'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/tide-gauge-data', 'ncei_map': 'https://www.ncei.noaa.gov/maps/hazards/', 'map_js': 'https://www.ncei.noaa.gov/maps/hazards/assets/index-Cyvq2L9n.js', 'map_store': 'https://www.ncei.noaa.gov/maps/hazards/assets/store-CeovILYQ.js', 'webmap_item': 'https://www.arcgis.com/sharing/rest/content/items/5cb326668189408e8c4c20ddd92aa2ed?f=pjson', 'webmap': 'https://www.arcgis.com/sharing/rest/content/items/5cb326668189408e8c4c20ddd92aa2ed/data?f=pjson', 'source_item': 'https://www.arcgis.com/sharing/rest/content/items/a3ae1f2bd8374e28900c43a0e69cc1b4?f=pjson', 'source_layer_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0?f=pjson', 'source_count_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json', 'source_ids_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json', 'source_page_0000': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?objectIds=1630%2C1631%2C1632%2C1633%2C1634%2C1635%2C1636%2C1637%2C1638%2C1639%2C1640%2C1641%2C1642%2C1643%2C1644%2C1645%2C1646%2C1647%2C1648%2C1649%2C1650%2C1651%2C1652%2C1653%2C1654%2C1655%2C1656%2C1657%2C1658%2C1659%2C1660%2C1661%2C1662%2C1663%2C1664%2C1665%2C1666%2C1667%2C1668%2C1669%2C1670%2C1671%2C1672%2C1673%2C1674%2C1675%2C1676%2C1677%2C1678%2C1679%2C1680%2C1681%2C1682%2C1683%2C1684%2C1685%2C1686%2C1687%2C1688%2C1689%2C1690%2C1691%2C1692%2C1693%2C1694%2C1695%2C1696%2C1697%2C1698%2C1699%2C1700%2C1701%2C1702%2C1703%2C1704%2C1705%2C1706%2C1707%2C1708%2C1709%2C1710%2C1711%2C1712%2C1713%2C1714%2C1715%2C1716%2C1717%2C1718%2C1719%2C1720%2C1721%2C1722%2C1723%2C1724%2C1725%2C1726%2C1727%2C1728%2C1729&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&returnTrueCurves=true&orderByFields=STAT_OBJID+ASC&f=json', 'source_page_0100': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?objectIds=1730%2C1731%2C1732%2C1733%2C1734%2C1735%2C1736%2C1737%2C1738%2C1739%2C1740%2C1741%2C1742%2C1743%2C1744%2C1745%2C1746%2C1747%2C1748%2C1749%2C1750%2C1751%2C1752%2C1753%2C1754%2C1755%2C1756%2C1757%2C1758%2C1759%2C1760%2C1761%2C1762%2C1763%2C1764%2C1765%2C1766%2C1767%2C1768%2C1769%2C1770%2C1771%2C1772%2C1773%2C1774%2C1775%2C1776%2C1777%2C1778%2C1779%2C1780%2C1781%2C1782%2C1783%2C1784%2C1785%2C1786%2C1787%2C1788%2C1789%2C1790%2C1791%2C1792%2C1793%2C1794%2C1795%2C1796%2C1797%2C1798%2C1799%2C1800%2C1801%2C1802%2C1803%2C1804%2C1805%2C1806%2C1807%2C1808%2C1809%2C1810%2C1811%2C1812%2C1813%2C1814%2C1815%2C1816%2C1817%2C1818%2C1819%2C1820%2C1821%2C1822%2C1823%2C1824%2C1825%2C1826%2C1827%2C1828%2C1829&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&returnTrueCurves=true&orderByFields=STAT_OBJID+ASC&f=json', 'source_page_0200': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?objectIds=1830%2C1831%2C1832%2C1833%2C1834%2C1835%2C1836%2C1837%2C1838%2C1839%2C1840%2C1841%2C1842%2C1843%2C1844%2C1845%2C1846%2C1847%2C1848%2C1849%2C1850%2C1851%2C1852%2C1853%2C1854%2C1855%2C1856%2C1857%2C1858%2C1859%2C1860%2C1861%2C1862%2C1863%2C1864%2C1865%2C1866%2C1867%2C1868%2C1869%2C1870%2C1871%2C1872%2C1873%2C1874%2C1875%2C1876%2C1877%2C1878%2C1879%2C1880%2C1881%2C1882%2C1883%2C1884%2C1885%2C1886%2C1887%2C1888%2C1889%2C1890%2C1891%2C1892%2C1893%2C1894%2C1895%2C1896%2C1897%2C1898%2C1899%2C1900%2C1901%2C1902%2C1903%2C1904%2C1905%2C1906%2C1907%2C1908%2C1909%2C1910%2C1911%2C1912%2C1913%2C1914%2C1915%2C1916%2C1917%2C1918%2C1919%2C1920%2C1921%2C1922%2C1923%2C1924%2C1925%2C1926%2C1927%2C1928%2C1929&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&returnTrueCurves=true&orderByFields=STAT_OBJID+ASC&f=json', 'source_page_0300': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?objectIds=1930%2C1931%2C1932%2C1933%2C1934%2C1935%2C1936%2C1937%2C1938%2C1939%2C1940%2C1941%2C1942%2C1943%2C1944%2C1945%2C1946%2C1947%2C1948%2C1949%2C1950%2C1951%2C1952%2C1953%2C1954%2C1955%2C1956%2C1957%2C1958%2C1959%2C1960%2C1961%2C1962%2C1963%2C1964%2C1965%2C1966%2C1967%2C1968%2C1969%2C1970%2C1971%2C1972%2C1973%2C1974%2C1975%2C1976%2C1977%2C1978%2C1979%2C1980%2C1981%2C1982%2C1983%2C1984%2C1985%2C1986%2C1987%2C1988%2C1989%2C1990%2C1991%2C1992%2C1993%2C1994%2C1995%2C1996%2C1997%2C1998%2C1999%2C2000%2C2001%2C2002%2C2003&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&returnTrueCurves=true&orderByFields=STAT_OBJID+ASC&f=json', 'source_layer_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0?f=pjson', 'source_count_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json', 'source_ids_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Tsunami_Capable_Tide_Stations/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json'}
FIELD_DECLARATIONS=[{'name': 'STAT', 'type': 'esriFieldTypeInteger', 'alias': 'ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_NAME', 'type': 'esriFieldTypeString', 'alias': 'Name', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_ST', 'type': 'esriFieldTypeString', 'alias': 'State', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_LAT', 'type': 'esriFieldTypeDouble', 'alias': 'Latitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_LONG', 'type': 'esriFieldTypeDouble', 'alias': 'Longitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_OBJID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'STAT_URL_REAL', 'type': 'esriFieldTypeString', 'alias': 'URL (Real-Time)', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STAT_URL_RETRO', 'type': 'esriFieldTypeString', 'alias': 'URL (Retrospective)', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ORG_LIST', 'type': 'esriFieldTypeString', 'alias': 'Organization', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': True, 'domain': None, 'defaultValue': None}]
ORIGINAL_IDS=[1630, 1631, 1632, 1633, 1634, 1635, 1636, 1637, 1638, 1639, 1640, 1641, 1642, 1643, 1644, 1645, 1646, 1647, 1648, 1649, 1650, 1651, 1652, 1653, 1654, 1655, 1656, 1657, 1658, 1659, 1660, 1661, 1662, 1663, 1664, 1665, 1666, 1667, 1668, 1669, 1670, 1671, 1672, 1673, 1674, 1675, 1676, 1677, 1678, 1679, 1680, 1681, 1682, 1683, 1684, 1685, 1686, 1687, 1688, 1689, 1690, 1691, 1692, 1693, 1694, 1695, 1696, 1697, 1698, 1699, 1700, 1701, 1702, 1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717, 1718, 1719, 1720, 1721, 1722, 1723, 1724, 1725, 1726, 1727, 1728, 1729, 1730, 1731, 1732, 1733, 1734, 1735, 1736, 1737, 1738, 1739, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1750, 1751, 1752, 1753, 1754, 1755, 1756, 1757, 1758, 1759, 1760, 1761, 1762, 1763, 1764, 1765, 1766, 1767, 1768, 1769, 1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 1778, 1779, 1780, 1781, 1782, 1783, 1784, 1785, 1786, 1787, 1788, 1789, 1790, 1791, 1792, 1793, 1794, 1795, 1796, 1797, 1798, 1799, 1800, 1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815, 1816, 1817, 1818, 1819, 1820, 1821, 1822, 1823, 1824, 1825, 1826, 1827, 1828, 1829, 1830, 1831, 1832, 1833, 1834, 1835, 1836, 1837, 1838, 1839, 1840, 1841, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 1850, 1851, 1852, 1853, 1854, 1855, 1856, 1857, 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 1874, 1875, 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892, 1893, 1894, 1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929, 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003]
FIELDS=tuple(f['name']for f in FIELD_DECLARATIONS)+('wkb_geometry',)
TYPE_MAP={'esriFieldTypeOID':pa.int64(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
TYPES=tuple(TYPE_MAP[f['type']]for f in FIELD_DECLARATIONS)+(pa.binary(),)
SOURCE_FEATURES_SHA256='99b87ae4632d2731f2696f4fe4501a05bdc5af519995042d5cacf594738997bb'
PARQUET_PIN={'sha256':'a6d214fb2e60abc4e1eaaae97f4881aa3a65a842226ff0c2194faeca0601da84','bytes':38247}
READY='metadata/tide_stations_candidate_ready.json'
class TideStationQuarantineError(ValueError):
    """Unknown source/cache kept outside discovery until reviewed."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise TideStationQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise TideStationQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if shutil.disk_usage(Path(storage.local_dir) if storage.local_dir and Path(storage.local_dir).exists() else Path('/workspace')).free<300_000_000: raise TideStationQuarantineError('local_space_guard')
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise TideStationQuarantineError('draft_requires_explicit_isolated_local_staging')
    root=Path(storage.local_dir)
    if any(path.is_symlink() for path in [root,*root.parents]):
        raise TideStationQuarantineError('staging_path_symlink_refused')
    if root.exists() and any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):
        raise TideStationQuarantineError('staging_alias_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise TideStationQuarantineError('immutable_capture_changed:'+key)


def quarantine(storage,captures,reason):
    key=f'_quarantine/{SLUG}/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,key+'/'+name+'.body',body)
        immutable(storage,key+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,key+'/reason.json',encode({'approved':False,'reason':reason}))
    return key


def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}


def acquire(storage,capture_dir=None):
    local_only(storage);captures={}
    try:
        for name in URLS:
            if capture_dir is not None:
                with (Path(capture_dir)/(name+'.body')).open('rb') as stream: body=stream.read(3_000_001)
                # Retain attempted bytes before any receipt decoding. A malformed
                # local receipt is evidence, not permission to lose its body.
                key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
                immutable(storage,key,body)
                diagnostic={'url':URLS[name],'body_key':key,'bytes':len(body),
                    'sha256':digest(body),'complete_body':False,'receipt_validated':False}
                captures[name]=(body,diagnostic)
                if len(body)>3_000_000: raise TideStationQuarantineError('capture_body_limit')
                with (Path(capture_dir)/(name+'.receipt.json')).open('rb') as stream:
                    raw_receipt=stream.read(65_537)
                receipt_key=key+'.receipt-original-'+digest(raw_receipt)+'.bin'
                immutable(storage,receipt_key,raw_receipt)
                diagnostic['original_receipt_key']=receipt_key
                if len(raw_receipt)>65_536:raise TideStationQuarantineError('capture_receipt_limit')
                receipt=strict_json(raw_receipt)
                captures[name]=(body,receipt)
            else:
                url=URLS[name]
                limit=3_000_000
                for attempt in range(3):
                    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                    try:
                        with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',url) as response:
                            receipt.update(status=response.status_code,final_url=str(response.url),
                                requested_accept_encoding='identity',content_encoding=response.headers.get('content-encoding'),
                                content_length=response.headers.get('content-length'))
                            try:
                                for block in response.iter_bytes(None):
                                    body+=block
                                    if len(body)>limit: raise TideStationQuarantineError('oversized_or_changed_source')
                            finally:
                                receipt['wire_bytes_downloaded']=getattr(response,'num_bytes_downloaded',None)
                            receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
                            # HTTP Content-Length measures the encoded transport body.
                            # iter_bytes transparently decodes Content-Encoding, even
                            # when the server ignores our identity request.
                            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):
                                raise TideStationQuarantineError('non200_or_incomplete_http_response')
                        captures[name]=(bytes(body),receipt);break
                    except (httpx.HTTPError,OSError,ValueError) as exc:
                        receipt.update(bytes=len(body),sha256=digest(body),complete_body=False)
                        quarantine(storage,{name:(bytes(body),receipt)},str(exc))
                        if attempt==2: raise TideStationQuarantineError('bounded_fetch_exhausted:'+str(exc)) from exc
        return captures
    except (OSError,ValueError,TypeError) as exc:
        where=quarantine(storage,captures,str(exc))
        raise TideStationQuarantineError(f'{exc}; retained:{where}') from exc


def validate_receipt(name,body,receipt):
    url=URLS[name]
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body) or receipt.get('sha256')!=digest(body) or receipt.get('complete_body')is not True):
        raise TideStationQuarantineError('original_receipt_url_status_hash_length_or_completeness_mismatch:'+name)
    return {'input_id':name+':'+digest(body),'producer':'National Oceanic and Atmospheric Administration, National Centers for Environmental Information',
        'host':urllib.parse.urlsplit(url).hostname,'role':'producer_publication'if name.startswith('ncei_')or name.startswith('map_')else'official_distributor',
        'canonical_url':url,'retrieval_url':url,'observed_final_url':receipt['final_url'],'retrieved_at':receipt.get('fetched_at',receipt.get('observed_at')),
        'distributor':'NOAA NCEI (ncei_noaa) through ArcGIS Online','delegation_evidence':{'producer_page':LANDING,'webmap':WEBMAP,'publisher_account':'ncei_noaa','service_item':ITEM},
        'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,'attribution':'NOAA NCEI, CO-OPS, Pacific Tsunami Warning Center and National Tsunami Warning Center',
        'release_date_unknown_reason':'Continuously maintained station archive roster; no explicit dated dataset release established. Service edit, portal modified and fetch times are observations, not release dates.'}


def manifest_for(captures,identity):
    if set(captures)!=set(URLS):raise TideStationQuarantineError('missing_extra_source_publication_roster')
    inputs=[validate_receipt(n,*captures[n])for n in URLS]
    if b'https://www.ncei.noaa.gov/maps/hazards/?layers=6'not in captures['ncei_tide'][0]or b'./assets/index-Cyvq2L9n.js'not in captures['ncei_map'][0]or b'./store-CeovILYQ.js'not in captures['map_js'][0]or WEBMAP.encode()not in captures['map_store'][0]:
        raise TideStationQuarantineError('official_NOAA_delegation_missing_or_changed')
    item=strict_json(captures['source_item'][0]);webitem=strict_json(captures['webmap_item'][0])
    if (item.get('id')!=ITEM or item.get('owner')!='ncei_noaa'or item.get('orgId')!='C8EMgrsFcRFL6LrL'or item.get('url')!=SERVICE or item.get('access')!='public'
        or webitem.get('id')!=WEBMAP or webitem.get('owner')!='ncei_noaa'or webitem.get('orgId')!='C8EMgrsFcRFL6LrL'):
        raise TideStationQuarantineError('official_publisher_service_drift')
    def layers(values):
        for value in values:
            yield value
            yield from layers(value.get('layers',[]))
    matching=[x for x in layers(strict_json(captures['webmap'][0]).get('operationalLayers',[]))if x.get('id')==SLUG]
    if len(matching)!=1 or matching[0].get('url')!=LAYER_URL or matching[0].get('itemId')!=ITEM:raise TideStationQuarantineError('official_webmap_exact_layer_delegation_drift')
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Tsunami Capable Tide Stations — complete official NOAA roster draft','publisher':'NOAA National Centers for Environmental Information','agency':'National Oceanic and Atmospheric Administration','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':inputs,'transformations':[{'operation':'complete_native_Esri_point_extraction','parameters':{'roster':'374 original STAT_OBJID witnesses, ten exact original field declarations, four explicit100-ID pages, stable before/after count/IDs/schema/CRS/edit identity.',
            'physical_types':'Esri Integer int32, Double float64, String exact/null distinct. Original OID declaration retained; Arrow int64 is an explicit unapproved storage representation.',
            'geometry':'Every native EPSG3857 binary64 Point coordinate preserved; no outSR, quantization, precision request, reprojection, snapping or generated source ID.',
            'scope':'Complete NOAA published archival station roster, not assertion that every station is presently operational or all global stations are represented.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Retain every original field, identifier and native coordinate without generic schema regeneration.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**PARQUET_PIN}]}}},
        'unsupported_output_formats':{'shapefile_zip':'Original field names, nullable strings and types have no approved destructive mapping.','pmtiles':'Quantization, display schema and ID mapping unapproved.'}}
    validate_release_provenance(manifest);return manifest


def inspect_originals(captures):
    if set(captures)!=set(URLS):raise TideStationQuarantineError('missing_extra_source_publication_roster')
    docs={n:strict_json(body)for n,(body,_)in captures.items()if n not in('ncei_tsunamis','ncei_tide','ncei_map','map_js','map_store')}
    before=docs['source_layer_before'];after=docs['source_layer_after']
    for layer in [before,after]:
        if (encode(layer.get('fields'))!=encode(FIELD_DECLARATIONS)or layer.get('objectIdField')!='STAT_OBJID'or layer.get('geometryType')!='esriGeometryPoint'
            or layer.get('hasM')is not False or layer.get('hasZ')is not False or encode(layer.get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857})or layer.get('serviceItemId')!=ITEM):raise TideStationQuarantineError('native_source_schema_types_crs_or_service_drift')
    if encode(before.get('editingInfo'))!=encode(after.get('editingInfo')):raise TideStationQuarantineError('source_changed_during_extraction')
    for part in ['before','after']:
        count=docs['source_count_'+part];ids=docs['source_ids_'+part];roster=ids.get('objectIds')
        if (type(count.get('count'))is not int or count['count']!=374 or ids.get('objectIdFieldName')!='STAT_OBJID'or type(roster)is not list or len(roster)!=374
            or any(type(v)is not int for v in roster)or len(set(roster))!=374 or sorted(roster)!=ORIGINAL_IDS):raise TideStationQuarantineError('count_id_roster_completeness_or_coverage_drift')
    features=[]
    for offset in range(0,374,100):
        page=docs['source_page_'+str(offset).zfill(4)]
        expectedfields=[{k:v for k,v in field.items()if k not in('nullable','editable','isViewOverride')}for field in FIELD_DECLARATIONS]
        if (page.get('exceededTransferLimit',False)is not False or page.get('objectIdFieldName')!='STAT_OBJID'or page.get('geometryType')!='esriGeometryPoint'
            or encode(page.get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857})or encode(page.get('fields'))!=encode(expectedfields)):raise TideStationQuarantineError('truncated_wrong_schema_or_coordinate_system_page')
        rows=page.get('features')
        if type(rows)is not list or [x.get('attributes',{}).get('STAT_OBJID')for x in rows]!=ORIGINAL_IDS[offset:offset+100]:raise TideStationQuarantineError('missing_duplicate_extra_or_unordered_page_ids')
        features.extend(rows)
    if digest(encode(features))!=SOURCE_FEATURES_SHA256:raise TideStationQuarantineError('complete_original_attribute_geometry_or_coverage_values_drift')
    rows=[]
    for feature in features:
        if set(feature)!={'attributes','geometry'}or set(feature['attributes'])!=set(FIELDS[:-1]):raise TideStationQuarantineError('extra_missing_attribute_or_feature_members')
        row=dict(feature['attributes'])
        for field in FIELD_DECLARATIONS:
            value=row[field['name']];kind=field['type']
            if value is None:
                if not field['nullable']:raise TideStationQuarantineError('source_nonnullable_value_missing')
            elif kind in('esriFieldTypeInteger','esriFieldTypeOID'):
                if type(value)is not int or not -(2**31)<=value<2**31:raise TideStationQuarantineError('source_integer_width_or_type_drift')
            elif kind=='esriFieldTypeDouble':
                if type(value)not in(int,float)or not math.isfinite(value):raise TideStationQuarantineError('source_double_type_or_value_drift')
            elif type(value)is not str or len(value)>field['length']:raise TideStationQuarantineError('source_string_type_or_length_drift')
        geometry=feature['geometry']
        if type(geometry)is not dict or set(geometry)!={'x','y'}or any(type(v)not in(int,float)or not math.isfinite(v)for v in geometry.values()):raise TideStationQuarantineError('incomplete_or_changed_native_coordinates')
        row['wkb_geometry']=shapely.to_wkb(shapely.Point(geometry['x'],geometry['y']),byte_order=1);rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['nullable'],metadata={b'original_esri_field':encode(f)})for f in FIELD_DECLARATIONS]+[pa.field('wkb_geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist(rows,schema=schema);validate_table(table)
    audit={'original_fields':FIELD_DECLARATIONS,'feature_count':374,'all_original_attribute_geometry_values_sha256':SOURCE_FEATURES_SHA256,'native_point_count':374,'source_layer_editing_info':before['editingInfo'],
        'source_relationship':'Exact official NOAA archival station roster; current-operational interpretation and published compatibility unapproved.','declared_scope':'Complete producer roster of tide gauge archives; source release date unknown, not inferred from portal/service edits.',
        'organizations':dict(collections.Counter(table['ORG_LIST'].to_pylist())),'nulls':{n:table[n].null_count for n in FIELDS},'original_identifiers':{n:{'unique':len(set(table[n].to_pylist())),'nulls':table[n].null_count}for n in ['STAT','STAT_OBJID','OID']}}
    return table,audit


def validate_table(table):
    if tuple(table.column_names)!=FIELDS or tuple(f.type for f in table.schema)!=TYPES or table.num_rows!=374:raise TideStationQuarantineError('native_schema_physical_types_or_completeness_drift')
    if table['STAT_OBJID'].to_pylist()!=ORIGINAL_IDS or any(table[n].null_count or len(set(table[n].to_pylist()))!=374 for n in ['STAT','STAT_OBJID','OID']):raise TideStationQuarantineError('original_identifiers_or_coverage_drift')
    geoms=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if set(shapely.get_type_id(geoms).tolist())!={0}or any(shapely.is_empty(geoms))or not all(shapely.is_valid(geoms)):raise TideStationQuarantineError('empty_invalid_or_changed_native_geometry')
    return table


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(3857).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['wkb_geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def write_native_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='wkb_geometry',geometry_type='Point',crs='EPSG:3857',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'wkb_geometry'})
    with sqlite3.connect(path)as db:db.execute("UPDATE gpkg_contents SET last_change='2026-01-01T00:00:00.000Z'")


def reconcile_outputs(root,table):
    expected=expected_parquet_table(table);path=root/'geoparquet'/f'{SLUG}.parquet';parquet=pq.read_table(path)
    if sha256_file(path)!=PARQUET_PIN['sha256']or path.stat().st_size!=PARQUET_PIN['bytes']or not parquet.equals(expected,check_metadata=True):raise TideStationQuarantineError('cached_parquet_values_types_or_metadata_disagree_with_fresh_original')
    gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if gm['crs']!='EPSG:3857'or tuple(gt.column_names)!=FIELDS or tuple(f.type for f in gt.schema)!=TYPES or gt.to_pylist()!=table.to_pylist():raise TideStationQuarantineError('cached_geopackage_values_types_or_wkb_disagree_with_fresh_original')
    with tempfile.TemporaryDirectory(prefix='tsunami-tide-canonical-gpkg-')as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_native_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise TideStationQuarantineError('cached_geopackage_full_container_disagrees_with_fresh_original')


def source_payloads(captures,identity,table,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/tide_stations_source_audit.json':encode({'approved':False,'feature_count':374,'crs':'EPSG:3857','invalid_geometries':0,'native_points':374,'complete_original_publication_audit':audit,'all_values_ids_declared_types_native_points_roundtrip':True}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['source_baseline_id_schema_type_geometry_coverage_mapping_unapproved','current_operational_interpretation_and_release_date_not_established']})}
    for name,(body,receipt)in captures.items():values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values


def original_bundle(storage,key,version,identity,*,create=None):
    fingerprint=digest(encode(identity))
    if create is None:body=load_first_construction(storage,key,version,fingerprint)
    else:
        captures,manifest=create
        # Reject invalid original claims before creating permanent trust state.
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        payload={'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{n}.body','receipt':receipt}for n,(body,receipt)in captures.items()}}
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,payload))
    payload=strict_json(body)['payload']
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(URLS):raise TideStationQuarantineError('original_construction_bundle_roster_drift')
    original={}
    for name,item in payload['captures'].items():
        receipt=item['receipt'];expected=f"_raw-captures/{SLUG}/{receipt['sha256']}/{name}.body"
        if item['body_key']!=expected:raise TideStationQuarantineError('original_construction_body_key_drift')
        original[name]=(storage.read_key(expected),receipt)
    manifest=payload['manifest'];expected=manifest_for(original,identity)
    if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):raise TideStationQuarantineError('original_construction_manifest_drift')
    validate_release_provenance(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
    return original,manifest


def reconcile_cache(storage,prefix,table,audit,captures,identity,manifest):
    root=Path(storage.local_dir)/prefix;ready=strict_json((root/READY).read_bytes());expected=source_payloads(captures,identity,table,audit,manifest)
    expected_names={*expected,f'geopackage/{SLUG}.gpkg',f'geoparquet/{SLUG}.parquet'}
    if (ready.get('version')!=prefix.rsplit('/',1)[1]or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity))or set(ready['objects'])!=expected_names):raise TideStationQuarantineError('cached_ready_identity_or_roster_drift')
    for name,value in expected.items():
        if (root/name).read_bytes()!=value:raise TideStationQuarantineError('cached_source_owned_bytes_drift:'+name)
    actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()};snapshot='metadata/source/version_'+digest((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'
    derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    if actual-(expected_names|{READY}|derivatives)or not(expected_names|{READY}).issubset(actual):raise TideStationQuarantineError('cached_unknown_or_missing_object')
    for name,pin in ready['objects'].items():
        if sha256_file(root/name)!=pin:raise TideStationQuarantineError('cached_source_object_changed:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists()and(root/name).read_bytes()!=(root/'metadata/source_manifest.json').read_bytes():raise TideStationQuarantineError('cached_source_snapshot_drift')
    if(root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if resolved.get('inputs')!=manifest['inputs']or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved:raise TideStationQuarantineError('cached_resolved_provenance_drift')
    reconcile_outputs(root,table)


def stage_tide_stations(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);root=None
    try:
        identity=processing_identity();manifest=manifest_for(captures,identity);table,audit=inspect_originals(captures)
        fingerprint=digest(encode(identity));version='v0.0.0-draft-noaa-'+fingerprint[:16];prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir);root=base/prefix
        for name,(body,receipt)in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        record=f'_construction-state/{SLUG}/{fingerprint}.json';record_path=base/record
        locks=base/'_locks';locks.mkdir(exist_ok=True)
        with(locks/(SLUG+'.lock')).open('a+b')as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            has_history=record_path.exists()or Path(str(record_path)+'.first-construction.json').exists()or Path(str(record_path)+'.evidence').exists()
            if root.exists():
                try:
                    captures,manifest=original_bundle(storage,record,version,identity);table,audit=inspect_originals(captures)
                    reconcile_cache(storage,prefix,table,audit,captures,identity,manifest)
                except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise TideStationQuarantineError('invalid_cache_moved_intact_out_of_discovery:'+str(target)+':'+str(exc))from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':374,'approved':False,'reused':True}
            if has_history:
                captures,manifest=original_bundle(storage,record,version,identity);table,audit=inspect_originals(captures)
            root.parent.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='.private-tsunami-tide-',dir=root.parent)as tmp:
                private=Path(tmp);values=source_payloads(captures,identity,table,audit,manifest)
                for name,body in values.items():path=private/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
                (private/'geopackage').mkdir();(private/'geoparquet').mkdir();write_native_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg');pq.write_table(expected_parquet_table(table),private/'geoparquet'/f'{SLUG}.parquet',compression='zstd',version='2.6')
                reconcile_outputs(private,table)
                roster={str(p.relative_to(private)):sha256_file(p)for p in private.rglob('*')if p.is_file()}
                (private/READY).write_bytes(encode({'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':roster}))
                # Complete private file roster and bytes precede the shared
                # write-once first-state CAS/marker/alias protocol.
                assert all(sha256_file(private/name)==pin for name,pin in roster.items())
                original,original_manifest=original_bundle(storage,record,version,identity,create=(captures,manifest))
                if encode(original_manifest)!=encode(manifest)or any(original[n][0]!=captures[n][0]or encode(original[n][1])!=encode(captures[n][1])for n in URLS):raise TideStationQuarantineError('first_construction_winner_differs')
                if root.exists():raise TideStationQuarantineError('concurrent_candidate_winner_preserved')
                os.rename(private,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':374,'approved':False,'reused':False}
    except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
        where=quarantine(storage,captures,str(exc));raise TideStationQuarantineError(f'{exc}; retained:{where}')from exc
