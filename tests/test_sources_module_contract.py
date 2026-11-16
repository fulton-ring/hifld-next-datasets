"""Contract: no sources/ dataset module imports another dataset module.

Shared primitives live in the declared shared modules so agents find and reuse
them instead of copying (issue #995). A dataset adapter under
``src/dagster_hifld/sources/`` may import another ``sources/`` module only when
that module is shared, or when it is the adapter's own ``<stem>_spec``
companion: the dataset's pinned replay/observation manifest, which must stay
data-only (checked below). Shared modules must not import dataset modules, so
the dependency direction stays leaf-to-shared.
"""
import ast
import unittest
from pathlib import Path

SOURCES_DIR = Path(__file__).parents[1] / "src" / "dagster_hifld" / "sources"

# Shared modules and family packages, by dotted module name below sources/.
SHARED_MODULES = frozenset({
    "arcgis_filegdb", "arcgis_layer", "capture_common", "census_tiger_common",
    "epa_facilities_common", "epa_icis_wastewater_common", "esri_common",
    "esri_geometry", "national_inventory_of_dams_common", "wbd_hu_family",
})
SHARED_PACKAGES = frozenset({"nfhl"})

SKIP_DIRS = {"__pycache__"}


def _source_files():
    for path in sorted(SOURCES_DIR.rglob("*.py")):
        if SKIP_DIRS & set(path.parts):
            continue
        yield path


def _module_id(path: Path) -> str:
    return path.relative_to(SOURCES_DIR).with_suffix("").as_posix().replace("/", ".")


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
                    targets.add(".".join(parts[2:]))
            elif node.level == 1:
                if node.module:
                    targets.add(node.module)
                else:
                    targets.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if len(parts) >= 3 and ".".join(parts[:2]) == "dagster_hifld.sources":
                    targets.add(".".join(parts[2:]))
    return targets


def _is_shared(target: str) -> bool:
    return target in SHARED_MODULES or target.split(".")[0] in SHARED_PACKAGES


class SourcesModuleContractTests(unittest.TestCase):
    def test_shared_module_roster_exists(self):
        missing = sorted(
            name for name in sorted(SHARED_MODULES | SHARED_PACKAGES)
            if not (SOURCES_DIR / f"{name.replace('.', '/')}.py").is_file()
            and not (SOURCES_DIR / name.replace(".", "/")).is_dir()
        )
        self.assertEqual(missing, [])

    def test_no_dataset_module_imports_another_dataset_module(self):
        violations = []
        modules = {_module_id(path) for path in _source_files()}
        for path in _source_files():
            module = _module_id(path)
            targets = _source_import_targets(ast.parse(path.read_text()))
            for target in sorted(targets):
                if target not in modules or target == module:
                    continue
                if _is_shared(target):
                    continue
                leaf_module, leaf_target = module.rsplit(".", 1)[-1], target.rsplit(".", 1)[-1]
                companion = leaf_target == f"{leaf_module}_spec" or leaf_module == f"{leaf_target}_spec"
                if companion:
                    continue
                violations.append(f"{module} imports dataset module {target}")
        self.assertEqual(violations, [])

    def test_shared_modules_import_only_shared_modules(self):
        violations = []
        dataset_modules = {_module_id(path) for path in _source_files()} \
            - SHARED_MODULES - {f"{name}.{sub}" for name in SHARED_PACKAGES
                                for sub in [p.stem for p in (SOURCES_DIR / name).glob("*.py")]}
        for name in sorted(SHARED_MODULES | SHARED_PACKAGES):
            paths = [SOURCES_DIR / f"{name}.py"] if "." not in name and (SOURCES_DIR / f"{name}.py").is_file() \
                else sorted((SOURCES_DIR / name).glob("*.py"))
            for path in paths:
                if not path.is_file():
                    continue
                targets = _source_import_targets(ast.parse(path.read_text()))
                for target in sorted(targets):
                    if target != _module_id(path) and target in dataset_modules:
                        violations.append(f"shared module {_module_id(path)} imports dataset module {target}")
        self.assertEqual(violations, [])

    def test_spec_companions_are_data_only(self):
        violations = []
        for path in sorted(SOURCES_DIR.rglob("*_spec.py")):
            if SKIP_DIRS & set(path.parts):
                continue
            tree = ast.parse(path.read_text())
            definitions = [node.name for node in tree.body
                           if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
            if definitions:
                violations.append(f"{path} defines {definitions}")
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
