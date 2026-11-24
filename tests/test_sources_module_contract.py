"""Contract: no sources/ dataset module imports another dataset module, and no
adapter hand-writes a failure-path receipt rescan.

Shared primitives live in the declared shared modules so agents find and reuse
them instead of copying. A dataset adapter under ``src/dagster_hifld/sources/``
may import another ``sources/`` module only when that module is shared, or when
it is the adapter's own ``<stem>_spec`` companion: the dataset's pinned
replay/observation manifest, which must stay data-only (checked below). Shared
modules must not import dataset modules, so the dependency direction stays
leaf-to-shared.

Issue #1225: after a capture failure, ``capture_common.retain_failed_capture``
owns the discovery of what to retain; an adapter must not rescan
``*.receipt.json`` inside its own except handler, because each hand-written
variant decides differently what happens to an unreadable receipt. The
allowlist of unmigrated module stems below may only shrink: migrating a module
removes its stem here, and no PR may add stems.
"""
import ast
import unittest
from pathlib import Path

SOURCES_DIR = Path(__file__).parents[1] / "src" / "dagster_hifld" / "sources"

SHARED_SOURCES = frozenset({
    "arcgis_capture", "arcgis_capture_sequence", "arcgis_count_capture", "arcgis_filegdb", "arcgis_layer", "arcgis_raw_capture", "bts_intermodal_family",
    "candidate_alias", "candidate_transaction", "capture_common",
    "census_latest", "census_tiger_common",
    "eia_860m_common", "electric_retail_service_territories_common",
    "epa_capture", "epa_frs_national_archive", "epa_icis_wastewater_common", "epa_regions_common",
    "esri_common", "esri_geometry", "geoparquet_candidate", "arcgis_adaptive",
    "gnis_historical_points_family",
    "national_inventory_of_dams_common", "nces_edge_common", "usace_ienc_common",
    "wbd_hu_family", "nhd_capture", "nhd_transport", "wbd_capture",
})


# Issue #1225: adapters that still hand-write a failure-path
# ``*.receipt.json`` rescan in an except handler. Shrinks as each migration
# lands; never grows.
UNMIGRATED_RECEIPT_RESCAN_ALLOWLIST = frozenset({
    "antenna_structure_registration_asr", "bridge_areas_usace_ienc",
    "brs_ebs_transmitters", "bsee_ocs_platforms", "bts_intermodal_family",
    "bts_pipeline_terminals", "building_locations_usace_ienc",
    "built_up_areas_point_usace_ienc", "cable_areas_usace_ienc",
    "cellular_towers", "child_care_centers", "coastlines_usace_ienc",
    "colleges_universities", "courthouses_usgs",
    "ferry_terminals",
    "fhwa_tunnel_inventory", "floodwalls_usace_ienc",
    "fuds_irm_project_boundary", "fuds_irm_property_boundary",
    "fuds_property_boundaries", "fws_cmt_locations",
    "harbor_facility_locations_usace_ienc", "hf_areas_usace_ienc",
    "historic_geomac_perimeters",
    "land_mobile_commercial_transmission_towers",
    "land_mobile_private_transmission_towers", "land_region_areas_usace_ienc",
    "land_region_locations_usace_ienc", "land_surface_areas_usace_ienc",
    "lock_basins_usace_ienc",
    "lock_gate_lines_usace_ienc", "mexico_us_border", "microwave_service_towers",
    "mooring_facility_areas_usace_ienc", "mooring_facility_locations_usace_ienc",
    "narn_rail_lines", "narn_rail_nodes", "national_transit_map_agencies",
    "ncua_insured_credit_unions", "nfhl_lomas", "nursing_homes",
    "oil_and_natural_gas_platforms", "pad_us_manager_name",
    "pad_us_protection_mechanism_category", "paging_transmission_towers",
    "plate_boundaries", "principal_ports", "quaternary_fault_lines",
    "railroad_bridges", "railroad_grade_crossings", "railways_usace_ienc",
    "restricted_navigation_areas_usace_ienc", "retrospective_bpr_deployments",
    "roads_usace_ienc", "runways", "sea_area_locations_usace_ienc",
    "secondary_roads_census", "shipping_fairways",
    "shoreline_construction_areas_usace_ienc",
    "shoreline_construction_lines_usace_ienc",
    "shoreline_construction_locations_usace_ienc", "sloped_ground_areas_usace_ienc",
    "sloped_ground_lines_usace_ienc", "small_craft_facility_areas_usace_ienc",
    "small_craft_facility_locations_usace_ienc",
    "special_purpose_buoys_usace_ienc", "state_capitols",
    "storage_tank_silo_areas_usace_ienc", "storage_tank_silo_locations_usace_ienc",
    "submarine_cable_lines_usace_ienc", "submarine_pipeline_lines_usace_ienc",
    "submarine_pipeline_locations_usace_ienc", "terminal_areas_usace_ienc",
    "terminal_points_usace_ienc", "tmas_stations", "trails_usgs",
    "underwater_rock_areas_usace_ienc", "us_courts_appeals_circuits",
    "us_district_court_jurisdictions", "us_landslide_regions",
    "usace_administrative_areas", "usace_airport_area", "usace_berths_areas",
    "usace_caution_areas", "usace_conveyor_areas", "usace_crane_locations",
    "usace_dam_lines", "usace_data_coverage_areas", "usace_data_quality_areas",
    "usace_daymark_locations", "usace_depth_areas", "usace_dry_docks",
    "usace_dumping_ground_areas", "usace_dynamic_buoys", "usace_floating_docks",
    "usace_harbor_administrative_areas", "usace_hulke_areas",
    "usace_ienc_isolated_danger", "usace_lakes", "usace_landmark_areas",
    "usace_landmark_locations", "usace_lateral_beacons",
    "usace_lateral_buoys", "usace_lock_gate_areas",
    "usace_lock_traffic_signal_stations", "usace_military_districts",
    "usace_military_divisions", "usace_mooring_facility_lines",
    "usace_nautical_publication_information_areas", "usace_navigation_lights",
    "usace_navigation_marks_areas", "usace_obstruction_areas",
    "usace_obstruction_lines", "usace_obstruction_locations",
    "usace_overhead_cables", "usace_overhead_pipeline_lines",
    "usace_owned_reservoirs", "usace_pile_locations", "usace_pipeline_areas",
    "usace_pontoon_areas", "usace_pylon_areas", "uscg_sectors",
    "usgs_critical_mineral_deposits", "vha_medical_facilities",
    "warning_signal_stations_usace_ienc", "wbd_10_digit_hu", "wbd_hu8_subbasin",
    "wbd_hu_family", "wbd_wbdline", "wfigs_interagency_fire_perimeters",
    "wreck_areas_usace_ienc", "wrecks_usace_ienc",
})


def _except_handler_receipt_rescan_modules() -> set[str]:
    """Stems whose except handlers still rescan ``*.receipt.json`` (issue #1225)."""
    stems = set()
    for path in sorted(SOURCES_DIR.glob("*.py")):
        if path.stem == "capture_common":
            continue
        tree = ast.parse(path.read_text())
        for handler in ast.walk(tree):
            if not isinstance(handler, ast.ExceptHandler):
                continue
            if any(isinstance(node, (ast.For, ast.comprehension))
                   and "receipt.json" in ast.unparse(node.iter)
                   for node in ast.walk(ast.Module(body=handler.body, type_ignores=[]))):
                stems.add(path.stem)
                break
    return stems


def _source_import_targets(tree: ast.AST) -> set[str]:
    """Return every ``sources/<module>`` name the tree imports."""
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                parts = node.module.split(".")
                if node.module == "dagster_hifld.sources":
                    targets.update(alias.name for alias in node.names)
                elif len(parts) >= 3 and ".".join(parts[:2]) == "dagster_hifld.sources":
                    targets.add(parts[2])
            elif node.level == 1:
                if node.module:
                    targets.add(node.module.split(".")[0])
                else:
                    targets.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if len(parts) >= 3 and ".".join(parts[:2]) == "dagster_hifld.sources":
                    targets.add(parts[2])
    return targets


class SourcesModuleContractTests(unittest.TestCase):
    def test_shared_module_roster_exists(self):
        missing = sorted(name for name in SHARED_SOURCES if not (SOURCES_DIR / f"{name}.py").is_file())
        self.assertEqual(missing, [])

    def test_no_dataset_module_imports_another_dataset_module(self):
        violations = []
        modules = {path.stem for path in SOURCES_DIR.glob("*.py")}
        for path in sorted(SOURCES_DIR.glob("*.py")):
            module = path.stem
            targets = _source_import_targets(ast.parse(path.read_text()))
            for target in sorted(targets):
                if target not in modules or target == module:
                    continue
                if target in SHARED_SOURCES:
                    continue
                companion = target == f"{module}_spec" or module == f"{target}_spec"
                if companion:
                    continue
                violations.append(f"{module} imports dataset module {target}")
        self.assertEqual(violations, [])

    def test_shared_modules_import_only_shared_modules(self):
        violations = []
        for name in sorted(SHARED_SOURCES):
            path = SOURCES_DIR / f"{name}.py"
            targets = _source_import_targets(ast.parse(path.read_text()))
            for target in sorted(targets):
                if target != name and target in {p.stem for p in SOURCES_DIR.glob("*.py")} - SHARED_SOURCES:
                    violations.append(f"shared module {name} imports dataset module {target}")
        self.assertEqual(violations, [])

    def test_spec_companions_are_data_only(self):
        violations = []
        for path in sorted(SOURCES_DIR.glob("*_spec.py")):
            tree = ast.parse(path.read_text())
            definitions = [node.name for node in tree.body
                           if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
            if definitions:
                violations.append(f"{path.name} defines {definitions}")
        self.assertEqual(violations, [])

    def test_failure_path_receipt_rescans_match_shrinking_allowlist(self):
        # Issue #1225: capture_common.retain_failed_capture owns failure-path
        # evidence discovery. The exact match keeps the allowlist honest in
        # both directions: a migration removes its stem, and no new hand
        # written rescan may appear.
        unmigrated = _except_handler_receipt_rescan_modules()
        self.assertEqual(unmigrated, UNMIGRATED_RECEIPT_RESCAN_ALLOWLIST)


if __name__ == "__main__":
    unittest.main()
