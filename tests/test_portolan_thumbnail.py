import tempfile
import unittest
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image
from shapely import LineString, Point, Polygon

from dagster_hifld.portolan.thumbnail import render_geoparquet_thumbnail


class PortolanThumbnailTests(unittest.TestCase):
    def test_renders_a_png_from_sampled_geoparquet_geometry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output = root / "geoparquet" / "data.parquet"
            output.parent.mkdir()
            pq.write_table(
                pa.table(
                    {
                        "geometry": [
                            Point(0, 0).wkb,
                            LineString([(0, 0), (2, 2)]).wkb,
                            Polygon([(0, 0), (2, 0), (1, 3), (0, 0)]).wkb,
                        ]
                    }
                ),
                output,
            )

            rendered = render_geoparquet_thumbnail(root, "geometry")

        self.assertIsNotNone(rendered)
        with Image.open(BytesIO(rendered)) as image:
            self.assertEqual(image.size, (512, 512))
            self.assertEqual(image.format, "PNG")

    def test_returns_none_when_every_geometry_is_null(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output = root / "geoparquet" / "data.parquet"
            output.parent.mkdir()
            pq.write_table(pa.table({"geometry": [None, None]}), output)

            self.assertIsNone(render_geoparquet_thumbnail(root, "geometry"))


if __name__ == "__main__":
    unittest.main()
