"""Discovery of the newest published NCES EDGE school-location release.

The SchoolLocations page lists every school-year archive as a link. NCES also
leaves future-year links in HTML comments while a release is not published, so
discovery reads real anchors only and never guesses a URL from a year label.
"""
from __future__ import annotations

import hashlib
import json
import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

LANDING = "https://nces.ed.gov/programs/edge/Geographic/SchoolLocations"
PRODUCTS = frozenset({"PUBLICSCH", "PRIVATESCH"})


class NcesEdgeReleaseError(ValueError):
    """The landing page does not name exactly one newest published release."""


class _PublicationLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.stack: list[tuple[str, bool, bool]] = []
        self.tokens: list[tuple[bool, str, str]] = []
        self.main_seen = False

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        muted = (self.stack[-1][1] if self.stack else False) or tag in {
            "head", "script", "style", "noscript", "header", "footer", "nav", "aside",
        }
        main = ((self.stack[-1][2] if self.stack else False) or tag == "main"
                or "MainContent" in attributes.get("class", "").split())
        self.main_seen = self.main_seen or main
        if tag == "a" and not muted:
            for key, value in attrs:
                if key == "href" and value is not None:
                    self.hrefs.append(value)
                    self.tokens.append((main, "href", urljoin(LANDING, value)))
        if tag not in {
            "area", "base", "br", "col", "embed", "hr", "img", "input", "link",
            "meta", "param", "source", "track", "wbr",
        }:
            self.stack.append((tag, muted, main))

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        if self.stack and self.stack[-1][1]:
            return
        text = " ".join(data.split())
        if text:
            self.tokens.append((self.stack[-1][2] if self.stack else False, "text", text))

    def substantive_evidence(self):
        """Preserve producer notes, tables and links; ignore scripts and page chrome."""
        return [(kind, value) for main, kind, value in self.tokens if main or not self.main_seen]


def dbf_record_count(body: bytes, reason: str) -> int:
    """Return the publisher-written DBF record count after checking its header."""
    if len(body) < 33:
        raise ValueError(f"{reason}_dbf_header_drift")
    rows = int.from_bytes(body[4:8], "little")
    header = int.from_bytes(body[8:10], "little")
    record = int.from_bytes(body[10:12], "little")
    if (header < 33 or (header - 33) % 32 or body[header - 1] != 0x0D or record <= 0
            or len(body) not in (header + rows * record, header + rows * record + 1)):
        raise ValueError(f"{reason}_dbf_header_or_length_drift")
    return rows


def shapefile_index_count(body: bytes, reason: str) -> int:
    """Return the SHX record count after checking the point-shapefile header."""
    if (len(body) < 100 or int.from_bytes(body[:4], "big") != 9994
            or int.from_bytes(body[24:28], "big") * 2 != len(body)
            or int.from_bytes(body[28:32], "little") != 1000
            or int.from_bytes(body[32:36], "little") != 1
            or (len(body) - 100) % 8):
        raise ValueError(f"{reason}_shx_header_or_length_drift")
    return (len(body) - 100) // 8


def shapefile_record_count(body: bytes, reason: str) -> int:
    """Return the SHP record count after walking every point record."""
    if (len(body) < 100 or int.from_bytes(body[:4], "big") != 9994
            or int.from_bytes(body[24:28], "big") * 2 != len(body)
            or int.from_bytes(body[28:32], "little") != 1000
            or int.from_bytes(body[32:36], "little") != 1):
        raise ValueError(f"{reason}_shp_header_or_length_drift")
    offset, count = 100, 0
    while offset < len(body):
        if offset + 8 > len(body):
            raise ValueError(f"{reason}_shp_record_incomplete")
        length = int.from_bytes(body[offset + 4:offset + 8], "big") * 2
        if length < 20 or offset + 8 + length > len(body):
            raise ValueError(f"{reason}_shp_record_incomplete")
        count += 1
        offset += 8 + length
    if offset != len(body):
        raise ValueError(f"{reason}_shp_record_incomplete")
    return count


def canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _release(year: int, url: str, stem: str, *, evidence_sha256: str | None,
             selection_rule: str | None) -> dict[str, object]:
    return {
        "schoolyear": f"{2000 + year}-{2001 + year}",
        "source_url": url,
        "stem": stem,
        "landing_evidence_sha256": evidence_sha256,
        "selection_rule": selection_rule,
    }


def discover_release(body: bytes, product: str) -> dict[str, object]:
    """Select the newest school-year ZIP named by an active official anchor."""
    if product not in PRODUCTS:
        raise NcesEdgeReleaseError("unknown_nces_edge_product")
    links = _PublicationLinks()
    try:
        links.feed(body.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise NcesEdgeReleaseError("nces_landing_page_encoding_drift") from exc
    pattern = re.compile(
        rf"/programs/edge/data/(EDGE_GEOCODE_{product}_(\d{{2}})(\d{{2}}))\.zip"
    )
    candidates: list[tuple[int, str, str]] = []
    for href in links.hrefs:
        url = urljoin(LANDING, href)
        parts = urlsplit(url)
        if (parts.scheme != "https" or parts.netloc != "nces.ed.gov"
                or parts.query or parts.fragment):
            continue
        match = pattern.fullmatch(parts.path)
        if match and int(match[3]) == (int(match[2]) + 1) % 100:
            candidates.append((int(match[2]), url, match[1]))
    if not candidates:
        raise NcesEdgeReleaseError(f"published_{product.lower()}_release_missing")
    newest = max(year for year, _, _ in candidates)
    selected = [(url, stem) for year, url, stem in candidates if year == newest]
    if len(selected) != 1:
        raise NcesEdgeReleaseError(f"published_{product.lower()}_release_ambiguous")
    source, stem = selected[0]
    return _release(
        newest, source, stem,
        evidence_sha256=sha(canonical(links.substantive_evidence())),
        selection_rule=(
            f"Newest adjacent school-year {product} ZIP named by a real official "
            "SchoolLocations anchor; comments, scripts and guessed URLs are ignored; "
            "no older fallback."
        ),
    )


def release_from_url(url: str, product: str) -> dict[str, object]:
    """Derive the release identity of a retained official capture for replay."""
    if product not in PRODUCTS:
        raise NcesEdgeReleaseError("unknown_nces_edge_product")
    parts = urlsplit(url)
    pattern = re.compile(
        rf"/programs/edge/data/(EDGE_GEOCODE_{product}_(\d{{2}})(\d{{2}}))\.zip"
    )
    match = pattern.fullmatch(parts.path)
    if (parts.scheme != "https" or parts.netloc != "nces.ed.gov" or not match
            or int(match[3]) != (int(match[2]) + 1) % 100):
        raise NcesEdgeReleaseError(f"retained_{product.lower()}_url_not_official_release")
    return _release(int(match[2]), url, match[1], evidence_sha256=None, selection_rule=None)
