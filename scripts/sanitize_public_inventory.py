"""Create the reduced inventory CSV shipped in the public runtime image."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

PUBLIC_INVENTORY_COLUMNS = (
    "path",
    "filename",
    "title",
    "description",
    "publisher",
    "agency",
    "office",
    "keywords",
    "Download Location",
    "date_issued",
    "date_modified",
)


def sanitize_inventory(input_path: Path, output_path: Path) -> None:
    """Copy only the columns consumed by the source-manifest fallback."""
    with input_path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        missing = [column for column in PUBLIC_INVENTORY_COLUMNS if column not in reader.fieldnames]
        if missing:
            raise ValueError(f"inventory is missing required columns: {', '.join(missing)}")

        with output_path.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.DictWriter(
                destination, fieldnames=PUBLIC_INVENTORY_COLUMNS, lineterminator="\n"
            )
            writer.writeheader()
            for row in reader:
                writer.writerow({column: row.get(column, "") for column in PUBLIC_INVENTORY_COLUMNS})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    sanitize_inventory(args.input, args.output)


if __name__ == "__main__":
    main()
