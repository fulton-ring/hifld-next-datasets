"""Bounded producer listing evidence for current Census TIGER releases.

This is discovery, not approval. A newer incomplete release fails closed rather
than silently falling back to an older edition. Dataset interpretation remains
with the source adapter.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

import httpx

from dagster_hifld.sources.capture_common import capture_get
from dagster_hifld.sources.esri_common import canonical, sha

ROOT = 'https://www2.census.gov/geo/tiger/'
MAX_LISTING_BYTES = 2_000_000
# A new TIGER vintage can appear in the index before its product directory is
# published, so probe the newest index entry and the one before it. A directory
# that exists but omits the requested product fails closed; no older fallback.
MAX_VINTAGE_PROBES = 2
MIN_TIGER_VINTAGE = 2020


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


def _directory_listing(client: httpx.Client, url: str, root: Path, name: str):
    """Read one bounded directory listing, or return None when it is absent."""
    try:
        body, receipt = capture_get(client, url, root, name, max_bytes=MAX_LISTING_BYTES)
    except ValueError:
        receipt_path = root / f'{name}.receipt.json'
        try:
            receipt = json.loads(receipt_path.read_text('utf-8'))
        except (OSError, ValueError):
            raise
        if receipt.get('status') == 404:
            return None
        raise
    if (receipt['status'] != 200 or receipt['final_url'] != url
            or receipt['complete_body'] is not True or not body):
        raise ValueError('annual_listing_capture_incomplete')
    return body, receipt


def _candidate_vintages(body: bytes, pattern: re.Pattern[str], *, two_digit: bool = False) -> list[int]:
    vintages = {int(year) for year in pattern.findall(body.decode('utf-8', errors='strict'))}
    if two_digit:
        vintages = {2000 + year for year in vintages}
    ordered = sorted(vintages, reverse=True)
    return [vintage for vintage in ordered if vintage >= MIN_TIGER_VINTAGE][:MAX_VINTAGE_PROBES]


@dataclass(frozen=True)
class TigerRelease:
    """One exact national or state TIGER/Line publication."""

    vintage: int
    source_file: str
    source_url: str
    directory_url: str
    listing_sha256: str
    listing_bytes: int
    latest_checked_at: str
    annual_listing_sha256: str

    @property
    def edition(self) -> str:
        return str(self.vintage)

    def as_metadata(self) -> dict:
        return {
            'tiger_vintage': str(self.vintage),
            'source_file': self.source_file,
            'source_url': self.source_url,
            'source_directory': self.directory_url,
            'source_listing_sha256': self.listing_sha256,
            'source_listing_bytes': self.listing_bytes,
            'annual_listing_sha256': self.annual_listing_sha256,
        }


@dataclass(frozen=True)
class GeodatabaseRelease(TigerRelease):
    """One exact national TIGER/Line geodatabase publication."""

    product: str

    def as_receipt_evidence(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ShapefileRelease(TigerRelease):
    """One exact national or state TIGER/Line shapefile publication."""

    layer: str


_GEODATABASE_DIRECTORY = re.compile(r'TGRGDB(\d{2})/')
_SHAPEFILE_DIRECTORY = re.compile(r'TIGER(20\d{2})/')


def discover_geodatabase(client: httpx.Client, capture_root: Path, product: str) -> GeodatabaseRelease:
    """Find the newest published national TGRGDB edition that carries ``product``.

    The producer's annual index names the editions; each edition's directory
    listing must name exactly one national archive for the product. A newer
    edition whose directory exists without the archive fails closed instead of
    silently serving an older vintage.
    """
    if not re.fullmatch(r'[a-z0-9_]+', product):
        raise ValueError('geodatabase_product_invalid')
    root_body, _ = _listing(client, ROOT, capture_root, 'annual-listing')
    vintages = _candidate_vintages(root_body, _GEODATABASE_DIRECTORY, two_digit=True)
    if not vintages:
        raise ValueError('geodatabase_editions_missing')
    for vintage in vintages:
        directory = f'{ROOT}TGRGDB{vintage % 100:02d}/'
        listing = _directory_listing(client, directory, capture_root, f'geodatabase-{vintage}-listing')
        if listing is None:
            continue
        body, receipt = listing
        matches = [name for name in _links(body)
                   if re.fullmatch(rf'tlgdb_{vintage}_(?:a_)?us_{re.escape(product)}\.gdb\.zip', name)]
        if len(matches) != 1:
            raise ValueError('published_geodatabase_missing_or_ambiguous')
        return GeodatabaseRelease(
            vintage=vintage, product=product, source_file=matches[0],
            source_url=directory + matches[0], directory_url=directory,
            listing_sha256=sha(body), listing_bytes=len(body),
            latest_checked_at=receipt['observed_at'],
            annual_listing_sha256=sha(root_body),
        )
    raise ValueError('geodatabase_editions_missing')


_GEODATABASE_URL = re.compile(
    r'https://www2\.census\.gov/geo/tiger/TGRGDB(\d{2})/(tlgdb_(\d{4})_(?:a_)?us_([a-z0-9_]+)\.gdb\.zip)'
)


def geodatabase_release_from_url(url: str, product: str) -> GeodatabaseRelease:
    """Rebuild one retained national geodatabase release identity from its URL."""
    match = _GEODATABASE_URL.fullmatch(url)
    if match is None or match.group(4) != product or int(match.group(3)) % 100 != int(match.group(1)):
        raise ValueError('source_url_not_current_tgrgdb')
    return GeodatabaseRelease(
        vintage=int(match.group(3)), product=product, source_file=match.group(2),
        source_url=url, directory_url=url.rsplit('/', 1)[0] + '/',
        listing_sha256='', listing_bytes=0, latest_checked_at='', annual_listing_sha256='',
    )


def geodatabase_release_from_receipt(receipt: dict, product: str) -> GeodatabaseRelease:
    """Restore discovery evidence, or URL-only identity for legacy receipts."""
    url = receipt.get('url')
    release = geodatabase_release_from_url(url if isinstance(url, str) else '', product)
    if 'source_release' not in receipt:
        return release
    evidence = receipt['source_release']
    if not isinstance(evidence, dict) or set(evidence) != set(asdict(release)):
        raise ValueError('geodatabase_release_evidence_invalid')
    for name in ('vintage', 'product', 'source_file', 'source_url', 'directory_url'):
        if type(evidence[name]) is not type(getattr(release, name)) or evidence[name] != getattr(release, name):
            raise ValueError('geodatabase_release_evidence_identity_mismatch')
    for name in ('listing_sha256', 'annual_listing_sha256'):
        if (not isinstance(evidence[name], str) or not re.fullmatch(r'[0-9a-f]{64}', evidence[name])
                or evidence[name] == '0' * 64):
            raise ValueError('geodatabase_release_evidence_invalid')
    if type(evidence['listing_bytes']) is not int or not 0 < evidence['listing_bytes'] <= MAX_LISTING_BYTES:
        raise ValueError('geodatabase_release_evidence_invalid')
    try:
        checked_at = datetime.fromisoformat(evidence['latest_checked_at'])
        if checked_at.utcoffset() is None:
            raise ValueError('missing timezone')
    except (TypeError, ValueError) as exc:
        raise ValueError('geodatabase_release_evidence_invalid') from exc
    return GeodatabaseRelease(**evidence)


def discover_shapefile(
    client: httpx.Client, capture_root: Path, layer: str, product: str, *,
    geoid: str | None = None,
) -> ShapefileRelease:
    """Find the newest published national or state TIGER/Line shapefile.

    ``layer`` is the producer directory name (for example ``COUNTY``) and
    ``product`` the file suffix (for example ``county``). A newer edition whose
    directory exists without the requested archive fails closed.
    """
    if not re.fullmatch(r'[A-Z0-9_]+', layer) or not re.fullmatch(r'[a-z0-9_]+', product):
        raise ValueError('shapefile_product_invalid')
    if geoid is not None and not re.fullmatch(r'\d{2}', geoid):
        raise ValueError('shapefile_geoid_invalid')
    root_body, _ = _listing(client, ROOT, capture_root, 'annual-listing')
    vintages = _candidate_vintages(root_body, _SHAPEFILE_DIRECTORY)
    if not vintages:
        raise ValueError('annual_editions_missing')
    area = geoid or 'us'
    for vintage in vintages:
        directory = f'{ROOT}TIGER{vintage}/{layer}/'
        listing = _directory_listing(client, directory, capture_root, f'{layer.lower()}-{vintage}-listing')
        if listing is None:
            continue
        body, receipt = listing
        filename = f'tl_{vintage}_{area}_{product}.zip'
        if _links(body).count(filename) != 1:
            raise ValueError('published_archive_missing_or_ambiguous')
        return ShapefileRelease(
            vintage=vintage, layer=layer, source_file=filename,
            source_url=directory + filename, directory_url=directory,
            listing_sha256=sha(body), listing_bytes=len(body),
            latest_checked_at=receipt['observed_at'],
            annual_listing_sha256=sha(root_body),
        )
    raise ValueError('annual_editions_missing')


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
