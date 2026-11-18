"""Contract: no sources/ dataset module imports another dataset module.

Shared primitives live in the declared shared modules so agents find and reuse
them instead of copying. A dataset adapter under ``src/dagster_hifld/sources/``
may import another ``sources/`` module only when that module is shared, or when
it is the adapter's own ``<stem>_spec`` companion: the dataset's pinned
replay/observation manifest, which must stay data-only (checked below). Shared
modules must not import dataset modules, so the dependency direction stays
leaf-to-shared.
"""
import ast
import unittest
from pathlib import Path

SOURCES_DIR = Path(__file__).parents[1] / "src" / "dagster_hifld" / "sources"

SHARED_SOURCES = frozenset({
    "arcgis_filegdb", "arcgis_layer", "candidate_alias", "capture_common",
    "census_latest", "census_tiger_common",
    "eia_860m_common", "electric_retail_service_territories_common",
    "epa_frs_national_archive", "epa_icis_wastewater_common", "epa_regions_common",
    "esri_common", "esri_geometry",
    "national_inventory_of_dams_common", "nces_edge_common", "wbd_hu_family",
})


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


if __name__ == "__main__":
    unittest.main()
