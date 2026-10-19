"""Versioned output capability declarations; no format or release acceptance."""
from pathlib import PurePosixPath
import re

SUPPORTED_OUTPUT_EXCLUSIONS = frozenset({"shapefile_zip", "geopackage", "geoparquet", "pmtiles"})


def validate_output_policy(metadata: dict) -> None:
    banned = metadata.get("unsupported_output_formats", {})
    if not isinstance(banned, dict):
        raise ValueError("unsupported_output_formats must map format names to evidence reasons")
    for name, reason in banned.items():
        if name not in SUPPORTED_OUTPUT_EXCLUSIONS:
            raise ValueError("Unsupported output exclusion format")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Unsupported output format requires a concrete evidence reason")
    if "prebuilt_output_policy" not in metadata:
        return
    policy = metadata["prebuilt_output_policy"]
    if (not isinstance(policy, dict) or set(policy) != {"schema_version", "formats"}
            or type(policy["schema_version"]) is not int or policy["schema_version"] != 1
            or not isinstance(policy["formats"], dict) or not policy["formats"]):
        raise ValueError("Invalid versioned prebuilt_output_policy")
    for name, spec in policy["formats"].items():
        # Narrow initial adapter: other encodings require a reviewed capability.
        if name != "geoparquet" or name in banned:
            raise ValueError("Unsupported or wholly banned prebuilt output format")
        if (not isinstance(spec, dict) or set(spec) != {"reason", "objects"}
                or not isinstance(spec["reason"], str) or not spec["reason"].strip()
                or not isinstance(spec["objects"], list) or not spec["objects"]):
            raise ValueError("Prebuilt output requires evidence reason and complete object roster")
        seen = set()
        for obj in spec["objects"]:
            if not isinstance(obj, dict) or set(obj) != {"path", "sha256", "bytes"}:
                raise ValueError("Invalid prebuilt output object descriptor")
            path = obj["path"]
            if (not isinstance(path, str) or not path.startswith("geoparquet/") or not path.endswith(".parquet")
                    or PurePosixPath(path).as_posix() != path or ".." in PurePosixPath(path).parts
                    or any(c in path for c in "\\?#%") or path in seen):
                raise ValueError("Unsafe or duplicate prebuilt output object path")
            seen.add(path)
            if (not isinstance(obj["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", obj["sha256"])
                    or type(obj["bytes"]) is not int or obj["bytes"] < 0):
                raise ValueError("Invalid prebuilt output checksum or length")


def prebuilt_output_spec(metadata: dict, format_name: str) -> dict | None:
    validate_output_policy(metadata)
    return metadata.get("prebuilt_output_policy", {}).get("formats", {}).get(format_name)
