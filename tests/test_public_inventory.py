import csv
import tempfile
import unittest
from pathlib import Path

from scripts.sanitize_public_inventory import PUBLIC_INVENTORY_COLUMNS, sanitize_inventory


class PublicInventoryTests(unittest.TestCase):
    def test_sanitizes_inventory_to_source_manifest_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "full.csv"
            output_path = Path(tmpdir) / "public.csv"
            columns = ["Admin Notes", *PUBLIC_INVENTORY_COLUMNS, "Notes"]
            with input_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=columns)
                writer.writeheader()
                writer.writerow({column: f"value-{index}" for index, column in enumerate(columns)})

            sanitize_inventory(input_path, output_path)
            self.assertNotIn(b"\r", output_path.read_bytes())

            with output_path.open(newline="", encoding="utf-8") as output:
                rows = list(csv.DictReader(output))
            self.assertEqual(tuple(rows[0]), PUBLIC_INVENTORY_COLUMNS)
            self.assertEqual(rows[0]["path"], "value-1")
            self.assertNotIn("Admin Notes", rows[0])
            self.assertNotIn("Notes", rows[0])

    def test_tracked_inventory_contains_only_public_manifest_columns(self):
        input_path = Path(__file__).parents[1] / "HIFLD_Open_Inventory_12112025.csv"
        with input_path.open(newline="", encoding="utf-8-sig") as source:
            source_columns = tuple(csv.DictReader(source).fieldnames or ())

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "public.csv"
            sanitize_inventory(input_path, output_path)
            with output_path.open(newline="", encoding="utf-8") as output:
                public_columns = tuple(csv.DictReader(output).fieldnames or ())

        self.assertEqual(public_columns, PUBLIC_INVENTORY_COLUMNS)
        self.assertEqual(source_columns, PUBLIC_INVENTORY_COLUMNS)


if __name__ == "__main__":
    unittest.main()
