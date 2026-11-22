"""Acquisition algorithms belong to reviewed protocol owners, including nested calls."""
import ast
from pathlib import Path
import unittest

SOURCES = Path(__file__).parents[1] / 'src/dagster_hifld/sources'
# capture_http_response consumes an existing response; it does not send HTTP.
CAPTURE_CALLS = {'capture_get', 'capture_with_retry',
                 'fetch_capture', 'fetch_capture_retry', 'capture_page'}
HTTP_VERBS = {'get', 'post', 'put', 'patch', 'delete', 'head', 'options', 'request', 'stream'}
HTTP_FUNCTIONS = {f'{module}.{verb}' for module in ('httpx', 'requests') for verb in HTTP_VERBS}
HTTP_CLIENT_TYPES = {'httpx.Client', 'httpx.AsyncClient', 'requests.Session', 'requests.sessions.Session'}
CLIENT_NAMES = {'client', 'active', 'doc_client', 'alias_client', 'session', 'httpx', 'requests'}


def _own_nodes(nodes):
    for node in nodes:
        yield node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield from _own_nodes(ast.iter_child_nodes(node))


def _qualified_imports(tree, module, *, is_package=False):
    module = module + ".__init__" if is_package else module
    imports = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            prefix = node.module or ''
            if node.level:
                package = module.split('.')[:-node.level]
                prefix = '.'.join([*package, *([prefix] if prefix else [])])
            for alias in node.names:
                imports[alias.asname or alias.name] = f'{prefix}.{alias.name}'
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name.split('.')[0]] = alias.name if alias.asname else alias.name.split('.')[0]

    return imports


def _capture_analysis(source, *, module='', is_package=False, known_network=frozenset()):
    """Resolve static callable aliases and lexical helpers, without executing source."""
    tree = ast.parse(source)
    bindings, functions, parents, parameters, annotations = {'': {}}, {}, {}, {}, {}
    imports = _qualified_imports(tree, module, is_package=is_package)
    scoped_imports = {'': imports}

    def collect(nodes, scope=''):
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f'{scope}.{node.name}' if scope else node.name
                functions[name], parents[name] = node, scope
                arguments = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
                parameters[name] = {arg.arg for arg in arguments}
                annotations[name] = {arg.arg: arg.annotation for arg in arguments}
                bindings[name] = {}
                local = ast.Module(body=[child for child in _own_nodes(node.body)
                                         if isinstance(child, (ast.Import, ast.ImportFrom))], type_ignores=[])
                scoped_imports[name] = _qualified_imports(local, module, is_package=is_package)
                collect(node.body, name)
            elif isinstance(node, ast.ClassDef):
                collect(node.body, f'{scope}.{node.name}' if scope else node.name)
            else:
                if isinstance(node, (ast.Assign, ast.AnnAssign)):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    for target in targets:
                        if isinstance(target, ast.Name) and node.value is not None:
                            bindings.setdefault(scope, {}).setdefault(target.id, []).append(node.value)
                collect(ast.iter_child_nodes(node), scope)
    collect(tree.body)
    functions['<module>'] = tree
    parents['<module>'] = ''
    parameters['<module>'] = set()
    network = set()

    def callback(name):
        return name in {'capture', 'fetch', 'request', 'operation', 'retained_or_fetch', 'get_extra'} or name.startswith(('capture_', 'fetch_', 'request_', 'operation_'))

    def imported_target(name, scope):
        while True:
            if name in scoped_imports.get(scope, {}):
                return scoped_imports[scope][name]
            if name in parameters.get(scope, set()):
                return name
            if not scope:
                return name
            scope = parents.get(scope, '')

    def qualified_name(expr, scope):
        if isinstance(expr, ast.Name):
            return imported_target(expr.id, scope)
        if isinstance(expr, ast.Attribute):
            parent = qualified_name(expr.value, scope)
            return f'{parent}.{expr.attr}' if parent else None
        return None

    def client_annotation(expr, scope):
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            try:
                return client_annotation(ast.parse(expr.value, mode='eval').body, scope)
            except SyntaxError:
                return False
        if isinstance(expr, ast.Name):
            return imported_target(expr.id, scope) in HTTP_CLIENT_TYPES
        if isinstance(expr, ast.Attribute):
            return qualified_name(expr, scope) in HTTP_CLIENT_TYPES
        if isinstance(expr, ast.BinOp):
            return client_annotation(expr.left, scope) or client_annotation(expr.right, scope)
        if isinstance(expr, ast.Subscript):
            return client_annotation(expr.slice, scope)
        return False

    def typed_client(name, scope):
        while scope:
            if name in annotations.get(scope, {}):
                return client_annotation(annotations[scope][name], scope)
            scope = parents.get(scope, '')
        return False

    def callable_network(expr, scope, seen=frozenset()):
        if isinstance(expr, (ast.BoolOp, ast.IfExp)):
            choices = expr.values if isinstance(expr, ast.BoolOp) else [expr.body, expr.orelse]
            return any(callable_network(choice, scope, seen) for choice in choices)
        if isinstance(expr, ast.Lambda):
            return any(callable_network(n.func, scope, seen) for n in _own_nodes([expr.body]) if isinstance(n, ast.Call))
        if isinstance(expr, ast.Call):
            if isinstance(expr.func, ast.Name) and imported_target(expr.func.id, scope).split('.')[-1] == 'partial' and expr.args:
                return callable_network(expr.args[0], scope, seen)
            return False
        if isinstance(expr, ast.Name):
            name, current = expr.id, scope
            while True:
                token = (current, name)
                if token in seen:
                    return False
                if name in parameters.get(current, set()):
                    return callback(name)
                if name in bindings.get(current, {}):
                    return any(callable_network(value, current, seen | {token}) for value in bindings[current][name])
                candidate = f'{current}.{name}' if current else name
                if candidate in functions:
                    return candidate in network
                if not current:
                    break
                current = parents.get(current, '')
            imported = imported_target(name, scope)
            return imported in known_network or imported in HTTP_FUNCTIONS or imported.split('.')[-1] in CAPTURE_CALLS
        if isinstance(expr, ast.Attribute):
            chain, root = [expr.attr], expr.value
            while isinstance(root, ast.Attribute):
                chain.insert(0, root.attr)
                root = root.value
            root_name = root.id if isinstance(root, ast.Name) else ''
            qualified = '.'.join([imported_target(root_name, scope), *chain])
            if qualified in known_network or expr.attr in CAPTURE_CALLS:
                return True
            if root_name in {'self', 'cls'} and '.' in scope:
                method = scope.rsplit('.', 1)[0] + '.' + expr.attr
                if method in network:
                    return True
            if expr.attr in {'capture', 'fetch', 'request', 'operation'}:
                return True
            client = root_name in CLIENT_NAMES or root_name.endswith('_client') or typed_client(root_name, scope) or any(part.endswith('_client') or part in CLIENT_NAMES for part in chain[:-1])
            return expr.attr in HTTP_VERBS and (client or imported_target(root_name, scope) in {'httpx', 'requests'})
        return False

    while True:
        more = {name for name, node in functions.items() if name not in network
                if any(callable_network(call.func, name) for call in _own_nodes(node.body) if isinstance(call, ast.Call))}
        if not more:
            break
        network.update(more)

    violations = []
    for name, node in functions.items():
        for loop in _own_nodes(node.body):
            if isinstance(loop, (ast.For, ast.AsyncFor, ast.While)):
                nodes = list(_own_nodes(loop.body))
                if isinstance(loop, ast.While):
                    nodes += list(_own_nodes([loop.test]))
            elif isinstance(loop, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
                # The first iterable is acquired once. Delegating an iterator to
                # a shared owner is consumption, not a request per yielded row.
                values = [loop.key, loop.value] if isinstance(loop, ast.DictComp) else [loop.elt]
                values += [condition for generator in loop.generators for condition in generator.ifs]
                values += [generator.iter for generator in loop.generators[1:]]
                nodes = list(_own_nodes(values))
            else:
                continue
            if any(callable_network(call.func, name) for call in nodes if isinstance(call, ast.Call)):
                violations.append((name, loop.lineno))
    return sorted(set(violations)), network, imports


def capture_loops(source):
    """Find requests inside loops through callbacks, aliases and lexical helpers."""
    return _capture_analysis(source)[0]


# Shared source modules are scanned, never exempted as whole files.
ARCGIS_OWNERS = frozenset({
    'arcgis_adaptive', 'arcgis_capture', 'arcgis_capture_sequence', 'arcgis_filegdb',
    'arcgis_layer', 'arcgis_raw_capture', 'capture_common', 'epa_capture',
    'epa_frs_arcgis_table', 'epa_frs_national_archive', 'epa_icis_wastewater_common',
    'epa_regions_common', 'esri_common', 'esri_geometry', 'nhd_capture', 'nhd_transport',
    'usace_ienc_common', 'wbd_capture', 'wbd_hu_family',
})

def arcgis_source_corpus(root=SOURCES):
    """Include endpoint/spec declarations, ArcGIS imports and protocol owners."""
    texts = {path.relative_to(root).as_posix(): path.read_text() for path in root.rglob('*.py')}
    trees = {name: ast.parse(text) for name, text in texts.items()}

    def relevant(name, tree):
        if Path(name).stem in ARCGIS_OWNERS or name.startswith('nfhl/'):
            return True
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if any(marker in node.value.lower() for marker in
                       ('featureserver', 'mapserver', '/arcgis/', 'returnidsonly', 'createreplica')):
                    return True
            if isinstance(node, ast.ImportFrom) and node.module:
                parts = node.module.split('.')
                if node.module.startswith('dagster_hifld.sources') and any(part in ARCGIS_OWNERS - {'capture_common', 'esri_common'} or part == 'nfhl' for part in parts):
                    return True
            if isinstance(node, ast.Import):
                if any(alias.name.startswith('dagster_hifld.sources.') and
                       any(part in ARCGIS_OWNERS - {'capture_common', 'esri_common'} or part == 'nfhl' for part in alias.name.split('.'))
                       for alias in node.names):
                    return True
        return False

    selected = {name for name, tree in trees.items() if relevant(name, tree)}
    for name in texts:
        paired = str(Path(name).with_name(Path(name).stem + '_spec.py'))
        if paired in selected:
            selected.add(name)
    # A selected declaration may live in an imported pinned manifest rather
    # than the conventional paired name. Follow those source-spec imports too.
    for name, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith('dagster_hifld.sources.'):
                target = node.module.removeprefix('dagster_hifld.sources.').replace('.', '/') + '.py'
                if target in selected and target.endswith('_spec.py'):
                    selected.add(name)
    module_files = {'dagster_hifld.sources.' + name.removesuffix('.py').replace('/', '.').removesuffix('.__init__'): name
                    for name in texts}
    while True:
        dependencies = set()
        for name in selected:
            module = 'dagster_hifld.sources.' + name.removesuffix('.py').replace('/', '.').removesuffix('.__init__')
            for node in ast.walk(trees[name]):
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                targets = _qualified_imports(ast.Module(body=[node], type_ignores=[]), module,
                                             is_package=name.endswith('__init__.py'))
                for target in targets.values():
                    while target.startswith('dagster_hifld.sources.'):
                        if target in module_files:
                            dependencies.add(module_files[target])
                            break
                        target = target.rpartition('.')[0]
        if dependencies <= selected:
            break
        selected.update(dependencies)
    return {name: texts[name] for name in sorted(selected)}


def corpus_capture_loops(corpus):
    """Propagate actual shared acquisition callees across source-module imports."""
    modules = {name: 'dagster_hifld.sources.' + name.removesuffix('.py').replace('/', '.').removesuffix('.__init__')
               for name in corpus}
    known = set()
    while True:
        analyses = {name: _capture_analysis(text, module=modules[name], is_package=name.endswith('__init__.py'), known_network=known,
                                           )
                    for name, text in corpus.items()}
        more = {f'{modules[name]}.{function}' for name, (_loops, network, _imports) in analyses.items()
                for function in network}
        more.update(f'{modules[name]}.{alias}' for name, (_loops, _network, imports) in analyses.items()
                    for alias, target in imports.items() if target in known)
        if more <= known:
            return {name: loops for name, (loops, _network, _imports) in analyses.items() if loops}
        known.update(more)


# Approval is attached to a specific function, never an entire source file.
# Adding another loop owner requires a reviewed protocol boundary and tests.
REVIEWED_PROTOCOL_OWNERS = {
    ('arcgis_adaptive.py', '_capture_adaptive_layer'): 'Before/page/closing witnesses and resumable family policy.',
    ('arcgis_adaptive.py', '_capture_slices'): 'Ordered refusal bisection and accepted-page checkpoints.',
    ('arcgis_adaptive.py', 'capture_document_group'): 'Ordered native-receipt document acquisition.',
    ('arcgis_capture.py', 'capture_layer'): 'Disk-backed discovery, ID slices and closing population witnesses.',
    ('arcgis_capture_sequence.py', 'capture_request_sequence.acquire'): 'Recursive source-declared request sequence.',
    ('arcgis_capture_sequence.py', 'capture_request_steps'): 'Execute ordered protocol request steps.',
    ('arcgis_capture_sequence.py', 'capture_validated_document'): 'Bounded document validation and retry.',
    ('arcgis_capture_sequence.py', 'retry_capture_request'): 'Source-policy retry with attempt custody.',
    ('arcgis_layer.py', 'capture_layer_roster'): 'Execute a declared layer roster and its population witnesses.',
    ('arcgis_layer.py', 'capture_with_retry'): 'Shared transient/error-envelope retry and evidence.',
    ('arcgis_layer.py', 'fetch_capture_retry'): 'Budgeted IENC retry and retained attempts.',
    ('arcgis_layer.py', 'poll_layer_release'): 'Small source release witness polling.',
    ('arcgis_raw_capture.py', 'capture_exact_id_population'): 'Exact-ID populations and closing source witnesses.',
    ('arcgis_raw_capture.py', 'request_raw'): 'Bounded raw GET/POST retry and sink custody.',
    ('epa_capture.py', 'capture_id_windows'): 'EPA exact-ID window capture.',
    ('epa_capture.py', 'capture_keyset_layer'): 'EPA keyset discovery, pages and closing witnesses.',
    ('epa_capture.py', 'capture_offset_pages'): 'EPA source-count offset paging.',
    ('nfhl/capture.py', 'LayerCapture.windows'): 'NFHL aligned OBJECTID windows with shared recursive refusal handling.',
    ('nhd_capture.py', 'capture_documents'): 'Native-document transport selection and ordered acquisition.',
    ('nhd_capture.py', 'capture_fixed_keyset_layer'): 'Fixed keyset checkpoints and empty-tail completeness witness.',
    ('nhd_capture.py', 'capture_id_window_layer'): 'NHD discovery, ID windows, refused attributes and closing documents.',
    ('nhd_capture.py', 'capture_id_windows'): 'NHD exact-ID windows and transfer/refusal handling.',
    ('nhd_capture.py', 'capture_keyset_layer'): 'Adaptive NHD keyset, alias rescue and tail witness.',
    ('nhd_capture.py', 'capture_roster_layer'): 'Resumable NHD before/page/closing acquisition.',
    ('nhd_capture.py', 'fetch_segments'): 'NHD checkpointed roster segment bisection.',
    ('nhd_transport.py', 'capture_document'): 'Bounded NHD transport retries and attempt evidence.',
    ('nhd_transport.py', 'capture_refused_attributes'): 'Exact refused-ID attribute acquisition and identity checks.',
    ('nhd_transport.py', 'fetch_keyset_page'): 'Keyset transport refusal classification and retention.',
    ('nhd_transport.py', 'rescue_single_record'): 'Witness original and alias producer hosts before rescue promotion.',
    ('wbd_capture.py', 'capture_document_group'): 'Ordered WBD producer documents with source-specific validation.',
    ('wbd_capture.py', 'capture_ordered_windows'): 'WBD roster plus recursive part acquisition and refusal bookkeeping.',
    ('wbd_capture.py', 'capture_window_receipts.walk'): 'WBD recursive native-URL windows and singleton retry policy.',
}

# These are native download protocols, not alternate FeatureServer JSON pagers.
# They stay visible to the analyzer and are exempted only at the reviewed
# function owning that protocol. New functions in the same files still fail.
NATIVE_PROTOCOL_OWNERS = {
    ('census_latest.py', 'discover_geodatabase'): 'Shared TIGER annual directory/vintage discovery for national FileGDB products.',
    ('census_latest.py', 'discover_shapefile'): 'Shared TIGER annual directory/vintage discovery for national/state ZIP products.',
    ('epa_frs_national_archive.py', '_stream_range_get'): 'Bounded HTTP Range reads of the agency national ZIP and its range receipts.',
    ('gnis_antarctica.py', 'capture_gnis_antarctica'): 'Agency S3 before/after listings plus native GPKG/text ZIP parity; no ArcGIS feature pages.',
    ('locks_usace_ienc.py', '_capture_live'): 'Asynchronous createReplica job polling and native FileGDB result custody.',
    ('locks_usace_ienc.py', '_perform'): 'Native replica download redirect-hop validation and transport evidence.',
    ('national_inventory_of_dams_native.py', '_capture_native_export'): 'Official native FileGDB createReplica/status/download transaction and bracketing witnesses.',
    ('nfhl/bulk.py', '_capture_download_location'): 'FEMA bulk product redirect resolution, with independent redirect receipts.',
    ('nfhl/bulk.py', 'capture_listing'): 'Shared FEMA bulk listing retrieval/retry, not MapServer pages.',
    ('nfhl/bulk.py', 'capture_product_zip'): 'Native FEMA product ZIP retry, CRC checks and partial artifact retention.',
    ('nfhl/bulk.py', 'refresh_products'): 'Shared bulk product roster refresh across reusable ZIP products.',
    ('noaa_nautical_navaids.py', 'capture_noaa_nautical_navaids'): 'Published original GPKG ZIP plus layer/ID witnesses; no JSON feature paging.',
    ('tsunami_observations.py', 'acquire'): 'Packages two retained original-HTTP archive groups through the shared request-step driver.',
    ('usace_waterway_lines.py', 'capture_usace_waterway_lines'): 'Native FileGDB replica through arcgis_filegdb, bracketed by source document/ID witnesses.',
    ('usace_waterway_nodes.py', 'capture_usace_waterway_nodes'): 'Native FileGDB replica through arcgis_filegdb, bracketed by source document/ID witnesses.',
}


def unowned_capture_loops(corpus, owners):
    return [(name, function, line) for name, loops in corpus_capture_loops(corpus).items()
            for function, line in loops if (name, function) not in owners]


class ArcGISSourceOwnershipTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = arcgis_source_corpus()
        cls.observed = corpus_capture_loops(cls.corpus)
        cls.owners = REVIEWED_PROTOCOL_OWNERS | NATIVE_PROTOCOL_OWNERS

    def test_source_adapters_do_not_own_acquisition_loops(self):
        violations = [(name, function, line) for name, loops in self.observed.items()
                      for function, line in loops if (name, function) not in self.owners]
        self.assertEqual(violations, [], 'Delegate acquisition to a reviewed protocol owner: ' +
            ', '.join(f'{name}:{line} {function}' for name, function, line in violations))

    def test_owner_exemptions_are_exact_reviewed_functions_and_remain_used(self):
        self.assertFalse(REVIEWED_PROTOCOL_OWNERS.keys() & NATIVE_PROTOCOL_OWNERS.keys())
        observed = {(name, function) for name, loops in self.observed.items() for function, _line in loops}
        self.assertEqual(self.owners.keys() - observed, set(), 'Remove stale protocol exemptions after a migration.')
        for (name, function), reason in self.owners.items():
            with self.subTest(owner=(name, function)):
                self.assertIn(name, self.corpus)
                self.assertTrue(function and reason)
                self.assertNotIn('*', name + function)

    def test_new_functions_in_reviewed_files_and_copied_owner_names_are_rejected(self):
        code = 'def _capture_slices(urls, capture):\n    for url in urls:\n        capture(url)\n'
        corpus = {'adapter.py': code, 'arcgis_adaptive.py': code.replace('_capture_slices', 'new_pager')}
        self.assertEqual(unowned_capture_loops(corpus, REVIEWED_PROTOCOL_OWNERS), [
            ('adapter.py', '_capture_slices', 2), ('arcgis_adaptive.py', 'new_pager', 2)])


class CaptureOwnershipAnalyzerTests(unittest.TestCase):
    def test_public_nested_and_aliased_capture_loops_are_found(self):
        code = '''
from common import capture_get as download
from functools import partial
bounded = partial(download, max_bytes=8)
def capture_dataset(client, ids):
    def request(url):
        return bounded(client, url)
    for oid in ids:
        request(str(oid))
'''
        self.assertEqual(capture_loops(code), [('capture_dataset', 8)])

    def test_aliases_and_comprehensions_are_not_an_escape(self):
        code = '''
import requests as web
def capture_pages(urls):
    return [web.get(url) for url in urls]
'''
        self.assertEqual(capture_loops(code), [('capture_pages', 4)])

    def test_validation_declarations_and_thin_delegate_have_no_capture_loop(self):
        code = '''
from shared import capture_layer
QUERY = {'returnIdsOnly': True}
def validate(rows):
    for row in rows:
        assert row.get('id')
def capture_dataset(*args):
    return capture_layer(*args)
'''
        self.assertEqual(capture_loops(code), [])

    def test_shared_iterator_consumption_is_not_a_local_acquisition_loop(self):
        code = '''
def validate_pages(shared_capture):
    for page in shared_capture():
        assert page.get('features')
'''
        self.assertEqual(capture_loops(code), [])


class CaptureCallbackRegressionTests(unittest.TestCase):
    def test_boolop_callable_keeps_capture_fallback_visible(self):
        source = """
from common import capture_get
def acquire(urls, fetch=None):
    for url in urls:
        (fetch or capture_get)(url)
"""
        self.assertEqual(capture_loops(source), [('acquire', 4)])

    def test_formal_callbacks_cannot_hide_request_loops(self):
        for callback in ('capture', 'fetch', 'request', 'operation'):
            with self.subTest(callback=callback):
                source = f"def acquire(urls, {callback}):\n    for url in urls:\n        {callback}(url)\n"
                self.assertEqual(capture_loops(source), [('acquire', 2)])

    def test_direct_http_function_import_aliases_are_found(self):
        for module, method, alias in (('httpx', 'get', 'send'), ('requests', 'post', None)):
            with self.subTest(module=module, method=method):
                suffix = f' as {alias}' if alias else ''
                source = f"from {module} import {method}{suffix}\ndef acquire(urls):\n    for url in urls:\n        {alias or method}(url)\n"
                self.assertEqual(capture_loops(source), [('acquire', 3)])

    def test_typed_and_named_http_clients_are_found_without_treating_dict_get_as_http(self):
        source = """
import httpx as web
def acquire(api: web.Client, urls):
    for url in urls:
        api.get(url)
def acquire_other(producer_client, urls):
    for url in urls:
        producer_client.get(url)
def validate(data: dict, rows):
    for row in rows:
        data.get(row)
"""
        self.assertEqual(capture_loops(source), [('acquire', 4), ('acquire_other', 7)])

    def test_unrelated_get_function_import_remains_validation(self):
        source = "from lookup import get as find\ndef validate(rows):\n    for row in rows:\n        find(row)\n"
        self.assertEqual(capture_loops(source), [])

    def test_function_local_import_alias_is_found(self):
        source = """
def acquire(urls):
    from common import capture_get as download
    for url in urls:
        download(url)
"""
        self.assertEqual(capture_loops(source), [('acquire', 4)])

    def test_function_local_http_module_alias_is_found_without_leaking_scope(self):
        source = """
def acquire(urls):
    import requests as web
    for url in urls:
        web.get(url)
def validate(web, rows):
    for row in rows:
        assert web.get(row)
"""
        self.assertEqual(capture_loops(source), [('acquire', 4)])

    def test_callback_bound_to_local_partial_is_found(self):
        source = """
from functools import partial
def acquire(urls, operation):
    download = partial(operation, max_bytes=10)
    for url in urls:
        download(url)
"""
        self.assertEqual(capture_loops(source), [('acquire', 5)])

    def test_local_validation_function_named_request_is_not_network(self):
        source = """
def request(row):
    return row.get('id')
def validate(rows):
    for row in rows:
        request(row)
"""
        self.assertEqual(capture_loops(source), [])

    def test_retained_response_decoding_is_not_an_http_request(self):
        source = """
from dagster_hifld.http_capture import capture_http_response
def validate_retained(wires):
    for wire in wires:
        capture_http_response(wire)
"""
        self.assertEqual(capture_loops(source), [])

    def test_shared_iterator_comprehension_is_only_consumption(self):
        source = """
def validate_pages(shared_capture):
    return [page.get('features') for page in shared_capture()]
"""
        self.assertEqual(capture_loops(source), [])


class CorpusSelectionRegressionTests(unittest.TestCase):
    def test_module_level_acquisition_is_not_an_escape(self):
        self.assertEqual(capture_loops("from common import capture_get\nfor url in urls:\n    capture_get(url)\n"),
                         [('<module>', 2)])

    def test_conditional_nested_helper_is_found(self):
        source = """
def acquire(client, urls):
    if True:
        def get_one(url):
            return client.get(url)
    for url in urls:
        get_one(url)
"""
        self.assertEqual(capture_loops(source), [('acquire', 6)])

    def test_callable_conditional_and_local_boolop_alias_are_found(self):
        source = """
from common import capture_get
def acquire(urls, fetch=None):
    chosen = fetch or capture_get
    for url in urls:
        (chosen if url else capture_get)(url)
"""
        self.assertEqual(capture_loops(source), [('acquire', 5)])

    def test_shared_import_alias_keeps_adapter_loop_visible(self):
        corpus = {
            'arcgis_capture.py': 'def capture_layer(client, url):\n    return client.get(url)\n',
            'adapter.py': 'from dagster_hifld.sources.arcgis_capture import capture_layer as layer\ndef acquire(client, urls):\n    for url in urls:\n        layer(client, url)\n',
        }
        self.assertEqual(corpus_capture_loops(corpus), {'adapter.py': [('acquire', 3)]})

    def test_relative_package_reexports_do_not_hide_requests(self):
        corpus = {
            'pkg/__init__.py': 'from .driver import download\n',
            'pkg/driver.py': 'def download(client, url):\n    return client.get(url)\n',
            'adapter.py': 'from dagster_hifld.sources.pkg import download\ndef acquire(client, urls):\n    for url in urls:\n        download(client, url)\n',
        }
        self.assertEqual(corpus_capture_loops(corpus), {'adapter.py': [('acquire', 3)]})

    def test_paired_specs_and_arcgis_imports_select_adapters(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            texts = {
                'paired.py': 'def validate(rows):\n    return len(rows)\n',
                'paired_spec.py': "URL = 'https://agency.test/FeatureServer/0'\n",
                'imported.py': 'from dagster_hifld.sources.arcgis_capture import capture_layer\nfrom dagster_hifld.sources.neutral import download\n',
                'neutral.py': 'def download(client, url):\n    return client.get(url)\n',
                'native.py': 'from dagster_hifld.sources.capture_common import capture_get\n',
                'unrelated.py': "URL = 'https://agency.test/file.csv'\n",
            }
            for name, text in texts.items():
                (root / name).write_text(text)
            self.assertEqual(set(arcgis_source_corpus(root)), {'paired.py', 'paired_spec.py', 'imported.py', 'neutral.py'})


if __name__ == '__main__':
    unittest.main()
