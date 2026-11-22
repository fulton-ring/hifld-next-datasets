"""Disk-backed acquisition of witnessed ArcGIS layer rosters.

Adapters provide source declarations and validation; this module owns discovery,
ordered paging and the closing population witnesses. Exceptional adaptive paging
remains in the corresponding family capture owners.
"""
from __future__ import annotations

import json
from collections.abc import MutableMapping
from pathlib import Path
from urllib.parse import urlencode

import httpx

from dagster_hifld.sources.capture_common import capture_get


class RetainedCaptureView(MutableMapping):
    """A capture mapping retaining paths and receipts, never a population of bodies.

    Fetchers write the immutable artifacts before assigning a value. Reads load
    one decoded body at a time. Prefix entries survive a failed acquisition so
    adapters can quarantine their existing evidence and write failure manifests.
    """
    def __init__(self, root: Path, captures=()):
        self.root = Path(root)
        self._receipts = {}
        self.update(captures)

    @property
    def body_paths(self):
        return {name: self.root / f'{name}.body' for name in self}

    def __getitem__(self, name):
        receipt = self._receipts[name]
        path = self.root / f'{name}.body'
        if path.is_symlink():
            raise ValueError('retained_capture_symlink_rejected')
        return path.read_bytes(), receipt

    def __setitem__(self, name, value):
        if not isinstance(name, str) or Path(name).name != name or '\\' in name:
            raise ValueError('capture_name_invalid')
        self._receipts[name] = value[1]

    def __delitem__(self, name):
        del self._receipts[name]

    def __iter__(self):
        return iter(self._receipts)

    def __len__(self):
        return len(self._receipts)


def capture_layer(url, capture_dir, byte_budget, page_size, *, client=None,
                  fetch=None, roster=None, seed_name='ids_before', seed_url=None,
                  validate_seed=None, captures=None, before_fetch=None,
                  after_fetch=None, oid_field='OBJECTID', validate_pages=True,
                  discovery_documents=(), on_validation_error=None):
    """Discover and capture a native ArcGIS population to retained disk evidence.

    ``roster`` maps the validated seed IDs to ordered (name, URL) requests. It
    preserves family URL and receipt identities, including offset-based plans.
    ``fetch`` accepts (client, URL, root, name), enforcing the family's existing
    retry and total-budget policy. Without it, bounded capture_get is used.
    A supplied view exposes the successful prefix even when a request fails.
    """
    # Imported here because arcgis_layer re-exports this entry point.
    from dagster_hifld.sources.arcgis_layer import capture_witness, layer_capture_roster

    root = Path(capture_dir)
    if byte_budget < 1 or page_size < 1:
        raise ValueError('source_capture_budget_or_page_size_invalid')
    root.mkdir(parents=True, exist_ok=True)
    view = captures if captures is not None else RetainedCaptureView(root)
    if not isinstance(view, RetainedCaptureView) or view.root != root:
        raise ValueError('capture_view_root_invalid')
    if client is None:
        with httpx.Client(follow_redirects=False, timeout=120) as active:
            return capture_layer(url, root, byte_budget, page_size, client=active,
                fetch=fetch, roster=roster, seed_name=seed_name, seed_url=seed_url,
                validate_seed=validate_seed, captures=view, before_fetch=before_fetch,
                after_fetch=after_fetch, oid_field=oid_field, validate_pages=validate_pages,
                discovery_documents=discovery_documents, on_validation_error=on_validation_error)

    def acquire(name, request_url):
        if name in view:
            return
        if before_fetch and name != seed_name:
            before_fetch(name)
        if fetch is None:
            retained = sum(p.stat().st_size for suffix in ('*.body', '*.wire')
                           for p in root.glob(suffix) if p.is_file())
            remaining = byte_budget - retained
            if remaining < 2:
                raise ValueError('source_capture_budget_exceeded_before_request')
            value = capture_get(client, request_url, root, name,
                                max_bytes=min(8_000_000, remaining // 2))
        else:
            value = fetch(client, request_url, root, name)
        view[name] = value
        if after_fetch and name != seed_name:
            after_fetch(name)

    seed_url = seed_url or url + '/query?' + urlencode(
        {'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
    for name, request_url in discovery_documents:
        acquire(name, request_url)
    acquire(seed_name, seed_url)
    seed = view[seed_name][0]
    if validate_seed:
        ids = validate_seed(seed)
    else:
        ids = json.loads(seed).get('objectIds')
        if (not isinstance(ids, list) or any(type(i) is not int for i in ids)
                or len(set(ids)) != len(ids)):
            raise ValueError('source_seed_id_population_drift')
        ids = sorted(ids)
    requests = list(roster(ids) if roster else layer_capture_roster(
        ids, layer_url=url, service_url=url.rsplit('/', 1)[0], page_size=page_size,
        oid_field=oid_field))
    names = [name for name, _ in requests]
    if len(names) != len(set(names)):
        raise ValueError('capture_roster_name_duplicate')
    for name, request_url in requests:
        acquire(name, request_url)
    try:
        captured_ids = None
        if validate_pages:
            captured_ids = [feature['attributes'][oid_field]
                        for name in view if name.startswith('page_')
                        for feature in json.loads(view[name][0]).get('features', [])]
        capture_witness(view, captured_ids=captured_ids)
    except (ValueError, KeyError, TypeError):
        if on_validation_error is not None:
            on_validation_error(view)
        raise
    return view
