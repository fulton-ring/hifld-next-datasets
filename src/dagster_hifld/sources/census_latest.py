"""Bounded producer annual-directory evidence for current Census TIGER roads.

This is discovery, not approval. A newer incomplete release fails closed rather
than silently falling back to an older edition. Dataset interpretation remains
with the source adapter.
"""
from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import httpx

from dagster_hifld.sources.capture_common import capture_get
from dagster_hifld.sources.esri_common import canonical, sha

ROOT = 'https://www2.census.gov/geo/tiger/'
MAX_LISTING_BYTES = 2_000_000


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.hrefs.extend(value for key, value in attrs if key == 'href' and value is not None)


def _links(body: bytes) -> list[str]:
    parser = _Links()
    parser.feed(body.decode('utf-8', errors='strict'))
    return parser.hrefs


def _listing(client: httpx.Client, url: str, root: Path, name: str):
    body, receipt = capture_get(client, url, root, name, max_bytes=MAX_LISTING_BYTES)
    if (receipt['status'] != 200 or receipt['final_url'] != url
            or receipt['complete_body'] is not True or not body):
        raise ValueError('annual_listing_capture_incomplete')
    return body, receipt


def _discover_annual_zip(
    client: httpx.Client, capture_root: Path, *, product: str,
    filename_template: str, annual_name: str, product_name: str,
) -> dict:
    root_body, root_receipt = _listing(client, ROOT, capture_root, annual_name)
    years = [match.group(1) for link in _links(root_body)
             if (match := re.fullmatch(r'TIGER(20\d{2})/', link))]
    if not years:
        raise ValueError('annual_editions_missing')
    if len(years) != len(set(years)):
        raise ValueError('annual_editions_ambiguous')
    edition = max(years)
    directory = ROOT + f'TIGER{edition}/{product}/'
    body, receipt = _listing(client, directory, capture_root, product_name)
    filename = filename_template.format(edition=edition)
    if _links(body).count(filename) != 1:
        raise ValueError('published_archive_missing_or_ambiguous')
    return {'edition': edition, 'source_url': directory + filename,
            'source_file': filename, 'latest_checked_at': receipt['observed_at'],
            'listing_sha256': sha(body), 'annual_listing_sha256': sha(root_body),
            'listing_url': directory, 'annual_listing_url': ROOT,
            'selection_rule': f'Largest exact TIGER20YY directory in producer listing; require one exact national {product} ZIP; no older-edition fallback.',
            'evidence_receipts': [root_receipt, receipt]}


def discover_primary_roads(client: httpx.Client, capture_root: Path) -> dict:
    """Select the largest exact published annual directory and its single ZIP."""
    return _discover_annual_zip(
        client, capture_root, product='PRIMARYROADS',
        filename_template='tl_{edition}_us_primaryroads.zip',
        annual_name='annual-listing', product_name='product-listing',
    )


def discover_county_boundaries(client: httpx.Client, capture_root: Path) -> dict:
    """Select the largest exact published TIGER county directory and its single ZIP."""
    return _discover_annual_zip(
        client, capture_root, product='COUNTY',
        filename_template='tl_{edition}_us_county.zip',
        annual_name='county-annual-listing', product_name='county-product-listing',
    )


def release_version(edition: str, archive_sha256: str, code_sha256: str) -> str:
    """Bind immutable versions to edition, bytes and code, excluding check time."""
    return 'v' + sha(canonical({'edition': edition, 'archive_sha256': archive_sha256,
                               'code_sha256': code_sha256}))[:32]
