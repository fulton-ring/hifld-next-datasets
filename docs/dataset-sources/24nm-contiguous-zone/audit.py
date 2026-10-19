"""Bounded anonymous NOAA recheck. Requires a fresh output directory; never ingests.

HTTP 200 ArcGIS errors remain failures. Use replay.py for committed offline evidence.
"""
import argparse
import datetime
import gzip
import hashlib
import json
import pathlib
import urllib.error
import urllib.request

BASE = 'https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/2/query'
QUERIES = {
    'ids-before': BASE + '?where=1%3D1&returnIdsOnly=true&f=json',
    'count': BASE + '?where=1%3D1&returnCountOnly=true&f=json',
    'features': BASE + '?where=1%3D1&outFields=%2A&f=geojson',
    'ids-after': BASE + '?where=1%3D1&returnIdsOnly=true&f=json&returnGeometry=false',
    'inport': 'https://www.fisheries.noaa.gov/inport/item/39963',
    'inport-xml': 'https://inport.nmfs.noaa.gov/inport-metadata/NOAA/NOS/OCS/iso19115/xml/39963.xml',
    'crosswalk': 'https://doi.org/10.3886/E241367V1',
    'archive': 'https://www.datalumos.org/datalumos/project/238492/version/V1/view',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    output = parser.parse_args().output
    output.mkdir(parents=True, exist_ok=False)
    log, captures = [], {}
    try:
        for name, url in QUERIES.items():
            entry = {'name': name, 'url': url, 'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            try:
                try:
                    with urllib.request.urlopen(url, timeout=45) as response:
                        body, status, final = response.read(), response.status, response.url
                except urllib.error.HTTPError as error:
                    body, status, final = error.read(), error.code, error.url
                entry.update(status=status, final_url=final, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                if name == 'features':
                    (output / 'features.json.gz').write_bytes(gzip.compress(body, mtime=0))
                else:
                    (output / (name + '.body')).write_bytes(body)
                if name in {'ids-before', 'ids-after', 'count', 'features'}:
                    obj = json.loads(body)
                    if status != 200 or 'error' in obj:
                        raise RuntimeError(f'HTTP{status}: {obj.get("error")}')
                    captures[name] = obj
            except Exception as error:
                entry['error'] = str(error)
            log.append(entry)
        missing = sorted({'ids-before', 'ids-after', 'count', 'features'} - captures.keys())
        if missing:
            raise RuntimeError(f'Incomplete extraction: failed required captures {missing}; see requests.json')
        before = captures['ids-before']['objectIds']
        after = captures['ids-after']['objectIds']
        features = captures['features']['features']
        actual = [f['properties']['OBJECTID'] for f in features]
        assert not captures['features'].get('exceededTransferLimit', False)
        assert len(features) == captures['count']['count'] == len(before) == len(set(before)) == len(after) == len(set(after)) == len(set(actual))
        assert None not in actual and set(before) == set(after) == set(actual)
        print(json.dumps({'features': len(features), 'independent_ids_reconciled': True, 'acceptance': 'UNAPPROVED'}))
    finally:
        (output / 'requests.json').write_text(json.dumps(log, indent=2) + '\n')


if __name__ == '__main__':
    main()
