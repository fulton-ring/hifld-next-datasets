"""Verify all committed evidence bytes and original response-body receipts offline."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
manifest = json.loads((ROOT / "evidence/pins.json").read_bytes())
for record in manifest["files"]:
    path = ROOT / record["path"]
    body = path.read_bytes()
    assert len(body) == record["bytes"], str(path)
    assert hashlib.sha256(body).hexdigest() == record["sha256"], str(path)
receipt_count = 0
for directory in ["discovery", "baseline"]:
    for receipt in (ROOT / "evidence" / directory).glob("*.receipt.json"):
        record = json.loads(receipt.read_bytes())
        body = receipt.with_name(receipt.name.removesuffix(".receipt.json") + ".body").read_bytes()
        assert len(body) == record["bytes"], str(receipt)
        assert hashlib.sha256(body).hexdigest() == record["sha256"], str(receipt)
        receipt_count += 1
print(json.dumps({"verified_files": len(manifest["files"]), "body_receipts": receipt_count}))
