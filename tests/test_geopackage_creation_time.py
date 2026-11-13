"""GeoPackage bytes must not depend on ambient GDAL creation timestamps."""

import importlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import fiona
import fiona.env
import pyarrow as pa
import pyogrio
from dagster_hifld import gdal
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPolygon,
    Point,
    Polygon,
    mapping,
)

CASES = (
    (
        "colleges_universities",
        "write_native_gpkg",
        "wkb_geometry",
        "point",
        "2026-01-01T00:00:00.000Z",
    ),
    (
        "combined_statistical_areas",
        "write_native_gpkg",
        "SHAPE",
        "multi",
        "2024-01-01T00:00:00.000Z",
    ),
    (
        "metropolitan_divisions",
        "write_native_gpkg",
        "SHAPE",
        "multi",
        "2024-01-01T00:00:00.000Z",
    ),
    (
        "fema_regions",
        "write_geopackage",
        "geometry",
        "point",
        "2000-01-01T00:00:00.000Z",
    ),
    (
        "consolidated_cities",
        "write_native_gpkg",
        "wkb_geometry",
        "multi",
        "2026-01-01T00:00:00.000Z",
    ),
    (
        "ferry_routes",
        "write_native_gpkg",
        "wkb_geometry",
        "line",
        "2026-01-01T00:00:00.000Z",
    ),
    (
        "elementary_school_districts",
        "write_native_gpkg",
        "wkb_geometry",
        "multi",
        "2026-01-01T00:00:00.000Z",
    ),
    (
        "campus_preservation",
        "write_geopackage",
        "geometry",
        "point",
        "2022-07-18T00:00:00.000Z",
    ),
    (
        "census_designated_places",
        "write_gpkg",
        "SHAPE",
        "multi",
        "2024-01-01T00:00:00.000Z",
    ),
    ("epa_regions", "write_gpkg", "geometry", "multi", "2000-01-01T00:00:00.000Z"),
    (
        "historical_tsunami_events",
        "write_gpkg",
        "geometry",
        "point",
        "2000-01-01T00:00:00.000Z",
    ),
)
FIONA_CASES = (
    ("cbrs_units", "write_geopackage", True, "polygon", "2024-12-19T00:00:00.000Z"),
    (
        "cbrs_prohibitions",
        "write_geopackage",
        True,
        "polygon",
        "2024-12-19T00:00:00.000Z",
    ),
    (
        "cbrs_buffer",
        "write_native_geopackage",
        False,
        "polygon",
        "2024-12-19T00:00:00.000Z",
    ),
    (
        "canada_us_border",
        "write_canonical_geopackage",
        True,
        "line",
        "2018-04-20T00:00:00.000Z",
    ),
    (
        "current_dart",
        "write_canonical_geopackage",
        True,
        "point",
        "2000-01-01T00:00:00.000Z",
    ),
    (
        "ferry_terminals",
        "write_canonical_geopackage",
        True,
        "point",
        "2000-01-01T00:00:00.000Z",
    ),
)
OTHER_DATE = "2099-12-31T23:59:59.999Z"


def _write(module_name, function_name, geometry_name, geometry_kind, ambient, path):
    module = importlib.import_module("dagster_hifld.sources." + module_name)
    geometry = (
        MultiPolygon([Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])])
        if geometry_kind == "multi"
        else Point(0.123456789, 1.23456789)
    )
    if geometry_kind == "line":
        geometry = MultiLineString([[(0, 0), (1, 1)]])
    values = {"source_id": ["0001"], geometry_name: [geometry.wkb]}
    if module_name == "elementary_school_districts":
        values["source_archive"] = ["synthetic archive label"]
    table = pa.table(values)
    previous = pyogrio.get_gdal_config_option("OGR_CURRENT_DATE")
    try:
        pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": ambient})
        getattr(module, function_name)(table, Path(path))
        assert pyogrio.get_gdal_config_option("OGR_CURRENT_DATE") == ambient
    finally:
        pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": previous})


def _write_fiona(case, ambient, path):
    module_name, function_name, path_first, geometry_kind, _timestamp = case
    module = importlib.import_module("dagster_hifld.sources." + module_name)
    properties = {}
    for name, kind in module.SCHEMA.items():
        if kind.startswith("str"):
            width = int(kind.split(":")[1]) if ":" in kind else 10
            properties[name] = "0001"[:width]
        elif kind.startswith("int"):
            properties[name] = 1
        elif kind.startswith("date"):
            properties[name] = "2000-01-01"
        else:
            properties[name] = 0.125
    geometry = {
        "polygon": Polygon([(0, 0), (1, 0), (0, 1), (0, 0)]),
        "line": LineString([(0, 0), (1, 1)]),
        "point": Point(0, 0),
    }[geometry_kind]
    rows = [{"type": "Feature", "properties": properties, "geometry": mapping(geometry)}]
    previous = pyogrio.get_gdal_config_option("OGR_CURRENT_DATE")
    try:
        pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": ambient})
        with fiona.Env(OGR_CURRENT_DATE=ambient):
            arguments = (path, rows) if path_first else (rows, path)
            getattr(module, function_name)(*arguments)
            assert fiona.env.get_gdal_config("OGR_CURRENT_DATE") == ambient
            assert pyogrio.get_gdal_config_option("OGR_CURRENT_DATE") == ambient
    finally:
        pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": previous})


def _logical_dump(path):
    with sqlite3.connect(path) as database:
        return tuple(database.iterdump())


class GeoPackageCreationTimeTests(unittest.TestCase):
    def test_fixed_creation_time_gives_identical_bytes_for_different_ambient_dates(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            for case in CASES:
                with self.subTest(dataset=case[0]):
                    first = Path(directory) / (case[0] + "-first.gpkg")
                    second = Path(directory) / (case[0] + "-second.gpkg")
                    _write(*case[:-1], case[-1], first)
                    _write(*case[:-1], OTHER_DATE, second)
                    self.assertEqual(_logical_dump(first), _logical_dump(second))
                    self.assertTrue(
                        first.read_bytes() == second.read_bytes(),
                        "GeoPackage container bytes depend on ambient date",
                    )

    def test_creation_bytes_are_identical_across_processes(self):
        script = "import runpy,sys; runpy.run_path(sys.argv[1])['_write'](*sys.argv[2:])"
        with tempfile.TemporaryDirectory() as directory:
            for case in CASES[:3]:
                with self.subTest(dataset=case[0]):
                    paths = [
                        Path(directory) / (case[0] + suffix) for suffix in ("-a.gpkg", "-b.gpkg")
                    ]
                    for path, ambient in zip(paths, (case[-1], OTHER_DATE), strict=True):
                        result = subprocess.run(
                            [
                                sys.executable,
                                "-c",
                                script,
                                str(Path(__file__).resolve()),
                                *case[:-1],
                                ambient,
                                str(path),
                            ],
                            env=os.environ.copy(),
                            capture_output=True,
                            text=True,
                            timeout=60,
                            check=False,
                        )
                        self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertTrue(
                        paths[0].read_bytes() == paths[1].read_bytes(),
                        "GeoPackage container bytes differ across processes",
                    )

    def test_creation_time_context_restores_configuration_after_failure(self):
        self.assertTrue(hasattr(gdal, "with_geopackage_creation_time"))
        previous = pyogrio.get_gdal_config_option("OGR_CURRENT_DATE")
        try:
            pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": OTHER_DATE})
            with (
                self.assertRaisesRegex(RuntimeError, "writer failure"),
                gdal.with_geopackage_creation_time(CASES[0][-1]),
            ):
                self.assertEqual(pyogrio.get_gdal_config_option("OGR_CURRENT_DATE"), CASES[0][-1])
                raise RuntimeError("writer failure")
            self.assertEqual(pyogrio.get_gdal_config_option("OGR_CURRENT_DATE"), OTHER_DATE)
        finally:
            pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": previous})

    def test_fiona_creation_time_is_scoped_and_restored(self):
        with fiona.Env(OGR_CURRENT_DATE=OTHER_DATE):
            with gdal.with_geopackage_creation_time(CASES[0][-1]):
                self.assertEqual(fiona.env.get_gdal_config("OGR_CURRENT_DATE"), CASES[0][-1])
            self.assertEqual(fiona.env.get_gdal_config("OGR_CURRENT_DATE"), OTHER_DATE)
            with (
                self.assertRaisesRegex(RuntimeError, "writer failure"),
                gdal.with_geopackage_creation_time(CASES[0][-1]),
            ):
                raise RuntimeError("writer failure")
            self.assertEqual(fiona.env.get_gdal_config("OGR_CURRENT_DATE"), OTHER_DATE)

    def test_fiona_writers_preserve_full_bytes_across_ambient_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            for case in FIONA_CASES:
                with self.subTest(dataset=case[0]):
                    first = Path(directory) / (case[0] + "-first.gpkg")
                    second = Path(directory) / (case[0] + "-second.gpkg")
                    _write_fiona(case, case[-1], first)
                    _write_fiona(case, OTHER_DATE, second)
                    self.assertEqual(_logical_dump(first), _logical_dump(second))
                    self.assertTrue(
                        first.read_bytes() == second.read_bytes(),
                        "Fiona GeoPackage bytes depend on ambient date",
                    )

    def test_fiona_creation_bytes_are_identical_across_processes(self):
        script = (
            "import runpy,sys; from pathlib import Path; "
            "n=runpy.run_path(sys.argv[1]); "
            "c=next(c for c in n['FIONA_CASES'] if c[0]=='cbrs_prohibitions'); "
            "n['_write_fiona'](c,sys.argv[2],Path(sys.argv[3]))"
        )
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("first.gpkg", "second.gpkg")]
            for path, ambient in zip(paths, (FIONA_CASES[1][-1], OTHER_DATE), strict=True):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        script,
                        str(Path(__file__).resolve()),
                        ambient,
                        str(path),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(
                paths[0].read_bytes() == paths[1].read_bytes(),
                "Fiona GeoPackage bytes differ across processes",
            )
