"""Replay retained evidence; no HTTP calls or writes."""
import hashlib
import json
from pathlib import Path
from analyze_candidate import analyze

ROOT = Path(__file__).resolve().parent

def unique_json(path):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ValueError(f'Duplicate JSON key {key!r} in {path}')
            result[key]=value
        return result
    return json.loads(path.read_text(),object_pairs_hook=pairs)

def main():
    manifest=unique_json(ROOT/'evidence-checksums.json')
    for entry in manifest['files']:
        data=(ROOT/entry['path']).read_bytes()
        assert len(data)==entry['bytes'],entry['path']
        assert hashlib.sha256(data).hexdigest()==entry['sha256'],entry['path']
    for receipt in (ROOT/'evidence').glob('*.receipt.json'):
        value=unique_json(receipt); body=receipt.with_name(receipt.name.replace('.receipt.json','.body'))
        if body.exists():
            data=body.read_bytes();assert len(data)==value['bytes_read'],str(receipt)
            assert hashlib.sha256(data).hexdigest()==value['sha256'],str(receipt)
            assert value.get('request_url') and value.get('final_url'),str(receipt)
    pin=unique_json(ROOT/'evidence/published-candidate.receipt.json')
    data=(ROOT/'evidence/published-candidate.parquet').read_bytes()
    assert len(data)==pin['bytes']==pin['declared_asset']['file:size']
    assert hashlib.sha256(data).hexdigest()==pin['sha256']==pin['generation_read']['sha256']
    assert '?generation='+pin['generation'] in pin['generation_read']['request_url']
    assert pin['generation_read']['status']==pin['status']==200
    assert analyze(ROOT/'evidence/published-candidate.parquet')==unique_json(ROOT/'evidence/published-analysis.json')
    print(f"Verified {len(manifest['files'])} retained files and all 17,666 published candidate rows; no source approval inferred.")
if __name__=='__main__': main()
