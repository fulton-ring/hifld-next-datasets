"""Helpers for staged FileGDB inputs."""

from __future__ import annotations

from pathlib import Path
import zipfile


def iter_file_geodatabases(search_dir: Path) -> list[Path]:
    """Return direct or zipped FileGDB directories under a format directory."""
    if not search_dir.is_dir():
        return []

    direct_geodatabases = [
        path
        for path in sorted(search_dir.rglob("*"))
        if path.is_dir()
        and path.suffix.lower() == ".gdb"
        and ".extracted" not in path.relative_to(search_dir).parts
    ]
    zip_paths = sorted(
        path
        for path in search_dir.rglob("*")
        if path.is_file()
        and path.suffix.lower() == ".zip"
        and ".extracted" not in path.relative_to(search_dir).parts
    )
    if not zip_paths:
        return direct_geodatabases
    geodatabases: list[Path] = []
    extract_root = search_dir / ".extracted"
    for zip_path in zip_paths:
        extracted_dir = extract_root / zip_path.stem
        _extract_zip_once(zip_path, extracted_dir)
        geodatabases.extend(
            path
            for path in sorted(extracted_dir.rglob("*"))
            if path.is_dir() and path.suffix.lower() == ".gdb"
        )
    return geodatabases


def _extract_zip_once(zip_path: Path, destination: Path) -> None:
    if destination.exists():
        return
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        destination_resolved = destination.resolve()
        for member in zf.infolist():
            target = (destination / member.filename).resolve()
            if target != destination_resolved and destination_resolved not in target.parents:
                raise ValueError(f"Unsafe zip member path: {member.filename}")
        zf.extractall(destination)
