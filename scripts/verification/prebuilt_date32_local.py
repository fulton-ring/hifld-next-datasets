"""Actual synthetic native Date32 pipeline; generic converter must not execute."""
import argparse
import json
from pathlib import Path
from unittest.mock import patch

from scripts.verification.native_synthetic_local import run


def execute(work):
    # Fault instrumentation only. Approval, storage, native comparison, registered
    # Dagster assets/checks and promotion execute their real implementations.
    with patch("dagster_hifld.assets.publish.process_layer_partitioned_geoparquet",
               side_effect=AssertionError("Forbidden generic converter invoked")) as converter:
        result = run(work, prebuilt_date32=True)
        assert converter.call_count == 0
        result["generic_converter_calls"] = converter.call_count
    (work / "prebuilt_result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    print(json.dumps(execute(parser.parse_args().work_dir.resolve())))
