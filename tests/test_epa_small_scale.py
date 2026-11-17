"""Small Scale now delegates to the shared EPA 2023 regions adapter."""
import unittest
from unittest.mock import patch

from dagster_hifld.sources import epa_small_scale as f
from dagster_hifld.sources import epa_regions_2023 as shared


class SmallScaleTests(unittest.TestCase):
    def test_uses_the_shared_maintained_2023_layer(self):
        self.assertEqual(f.SLUG, 'epa-regions-small-scale-15000001')
        self.assertTrue(shared.BASE.endswith('/FeatureServer/13'))
        self.assertNotIn('EPA_Locations', ' '.join(shared.URLS.values()))

    def test_stage_delegates_with_its_dataset_identity(self):
        with patch.object(f, 'stage_epa_regions_2023', return_value={'ok': True}) as staged:
            self.assertEqual(f.stage_epa_small_scale('storage', capture_dir='inputs'), {'ok': True})
        staged.assert_called_once_with('storage', capture_dir='inputs', dataset=shared.SMALL_SCALE_IDENTITY)


if __name__ == '__main__':
    unittest.main()
