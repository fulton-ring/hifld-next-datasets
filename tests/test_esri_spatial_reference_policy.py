"""Native declaration validators retain each source's drift contract."""
import unittest

from pyproj import CRS
from pyproj.exceptions import CRSError

from dagster_hifld.sources import esri_geometry as crs


class SpatialReferencePolicyTests(unittest.TestCase):
    def test_default_strict_4326_and_optional_empty(self):
        self.assertFalse(crs.validate_spatial_reference({}, 'page', required=False))
        for value in (None, {'wkid': True}, {'wkid': 4326, 'latestWkid': 3857},
                      {'wkt': ''}, {'wkt': 'invalid'}, {'wkt': CRS.from_epsg(3857).to_wkt()}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
                crs.validate_spatial_reference(value, 'page', required=False)

    def test_native_aliases_and_optional_inheritance(self):
        self.assertFalse(crs.validate_native_spatial_reference(None, 'feature', required=False))
        self.assertTrue(crs.validate_native_spatial_reference(
            {'wkid': 102100, 'latestWkid': 3857, 'wkt2': CRS.from_epsg(3857).to_wkt()}, 'page'))
        for value in (None, {'wkid': 4326}, {'wkid': 102100, 'wkt': CRS.from_epsg(4326).to_wkt()}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
                crs.validate_native_spatial_reference(value, 'page')

    def test_gnis_requires_latest_and_preserves_custom_errors(self):
        class SourceError(ValueError):
            pass
        def validate(value):
            return crs.validate_native_wkid_pair(
                value, 'page', native_wkid=102100, native_latest_wkid=3857,
                error_type=SourceError, missing_error='source_spatial_reference_missing:{label}',
                drift_error='source_spatial_reference_drift:{label}')
        self.assertIsNone(validate({'latestWkid': 3857, 'wkid': None, 'wkt': 'ignored'}))
        with self.assertRaisesRegex(SourceError, '^source_spatial_reference_missing:page$'):
            validate(None)
        for value in ({}, {'wkid': 102100}, {'latestWkid': 3857, 'wkid': 3857}):
            with self.subTest(value=value), self.assertRaisesRegex(SourceError, '^source_spatial_reference_drift:page$'):
                validate(value)

    def test_advertised_wkids_preserve_nad83_and_numeric_contract(self):
        self.assertIsNone(crs.validate_advertised_wkids(
            {'wkid': 4269.0, 'latestWkid': None, 'wkt': 'ignored'}, 'page', native_wkid=4269))
        self.assertIsNone(crs.validate_advertised_wkids({}, 'page', native_wkid=4269))
        with self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
            crs.validate_advertised_wkids({}, 'page', native_wkid=4269, required=True)
        with self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
            crs.validate_advertised_wkids({'wkid': 4326}, 'page', native_wkid=4269)
        with self.assertRaisesRegex(ValueError, '^page_crs_missing$'):
            crs.validate_advertised_wkids(None, 'page', native_wkid=4269)

    def test_pinned_pair_requires_a_code_even_with_valid_wkt(self):
        expected = {'wkid': 102100, 'latestWkid': 3857}
        self.assertIsNone(crs.validate_pinned_spatial_reference({'wkid': 102100}, 'page', expected))
        for value in ({'wkid': 3857}, {'wkt': CRS.from_epsg(3857).to_wkt()},
                      {'latestWkid': 3857, 'wkt': CRS.from_epsg(4326).to_wkt()}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
                crs.validate_pinned_spatial_reference(value, 'page', expected)

    def test_raw_wkt_errors_and_distinct_missing_error(self):
        with self.assertRaisesRegex(ValueError, '^page_crs_missing$'):
            crs.validate_spatial_reference_with_raw_wkt({}, 'page')
        with self.assertRaises(CRSError):
            crs.validate_spatial_reference_with_raw_wkt({'wkt': 'invalid'}, 'page')
        with self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
            crs.validate_spatial_reference_with_raw_wkt({'wkid': 3857, 'wkt': 'invalid'}, 'page')

    def test_validation_only_strict_contract(self):
        self.assertIsNone(crs.validate_required_spatial_reference(
            {'wkid': 102100}, 'page', wkids=(3857, 102100), source_crs=CRS.from_epsg(3857)))
        with self.assertRaisesRegex(ValueError, '^page_crs_drift$'):
            crs.validate_required_spatial_reference(
                {'wkid': 102100, 'latestWkid': 4326}, 'page',
                wkids=(3857, 102100), source_crs=CRS.from_epsg(3857))
