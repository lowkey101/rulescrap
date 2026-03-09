#!/usr/bin/env python3
"""Crawl Rajasthan government sites and collect employee-rule PDF links.

Features:
- Config-driven sources and keyword filters.
- Optional in-domain crawling from seed pages (depth-limited).
- PDF link extraction and de-duplication.
- Dry-run listing or direct PDF download.
- JSON report generation for downstream indexing/search.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import deque
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


@dataclass(slots=True)
class Source:
    name: str
    seed_urls: list[str]
    include_keywords: list[str]
    allowed_domains: list[str]
    crawl_depth: int


@dataclass(slots=True)
class PdfRecord:
    source: str
    page_url: str
    pdf_url: str
    text: str


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._inside_a = False
        self._href = ""
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = ""
        for key, value in attrs:
            if key.lower() == "href" and value:
                href = value
                break
        self._inside_a = True
        self._href = href
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._inside_a:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._inside_a:
            return
        text = " ".join("".join(self._text_parts).split())
        self.links.append((self._href, text))
        self._inside_a = False
        self._href = ""
        self._text_parts = []


def normalized_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def load_sources(path: Path) -> list[Source]:
    data = json.loads(path.read_text(encoding="utf-8"))
    sources: list[Source] = []
    for item in data.get("sources", []):
        seed_urls = item.get("seed_urls") or item.get("urls") or []
        if not seed_urls:
            raise ValueError(f"Source '{item.get('name', 'unknown')}' requires seed_urls/urls")
        allowed_domains = item.get("allowed_domains") or sorted(
            {normalized_domain(url) for url in seed_urls}
        )
        sources.append(
            Source(
                name=item["name"],
                seed_urls=seed_urls,
                include_keywords=item.get("include_keywords", []),
                allowed_domains=allowed_domains,
                crawl_depth=int(item.get("crawl_depth", 0)),
            )
        )
    return sources


def http_get_text(url: str, timeout: int) -> str:
    req = Request(url, headers={"User-Agent": DEFAULT_UA})
    with urlopen(req, timeout=timeout) as response:
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return ""
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def normalize_url(base_url: str, link: str) -> str:
    merged = urljoin(base_url, link.strip())
    clean, _ = urldefrag(merged)
    return clean


def is_http_url(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://")


def is_pdf_url(url: str) -> bool:
    return url.lower().split("?")[0].endswith(".pdf")


def keyword_match(text: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    t = text.lower()
    return any(kw.lower() in t for kw in keywords)


def in_allowed_domains(url: str, allowed_domains: list[str]) -> bool:
    domain = normalized_domain(url)
    for allowed in allowed_domains:
        if domain == allowed or domain.endswith(f".{allowed}"):
            return True
    return False


def parse_links(html: str) -> list[tuple[str, str]]:
    parser = LinkParser()
    parser.feed(html)
    return parser.links


def extract_from_page(source: Source, page_url: str, html: str) -> tuple[list[PdfRecord], list[str]]:
    records: list[PdfRecord] = []
    next_pages: list[str] = []
    for href, text in parse_links(html):
        if not href:
            continue
        url = normalize_url(page_url, href)
        if not is_http_url(url):
            continue
        if is_pdf_url(url):
            if keyword_match(f"{text} {url}", source.include_keywords):
                records.append(PdfRecord(source.name, page_url, url, text))
            continue
        if in_allowed_domains(url, source.allowed_domains):
            next_pages.append(url)
    return records, next_pages


def unique_records(records: Iterable[PdfRecord]) -> list[PdfRecord]:
    seen: set[str] = set()
    out: list[PdfRecord] = []
    for rec in records:
        if rec.pdf_url in seen:
            continue
        seen.add(rec.pdf_url)
        out.append(rec)
    return out


def crawl_source(source: Source, timeout: int, max_pages: int) -> list[PdfRecord]:
    queue: deque[tuple[str, int]] = deque((url, 0) for url in source.seed_urls)
    visited: set[str] = set()
    found: list[PdfRecord] = []

    while queue and len(visited) < max_pages:
        page_url, depth = queue.popleft()
        if page_url in visited:
            continue
        visited.add(page_url)
        try:
            html = http_get_text(page_url, timeout)
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            print(f"[WARN] [{source.name}] fetch failed: {page_url} ({exc})", file=sys.stderr)
            continue

        if not html:
            continue

        records, next_pages = extract_from_page(source, page_url, html)
        found.extend(records)

        if depth >= source.crawl_depth:
            continue
        for nxt in next_pages:
            if nxt not in visited:
                queue.append((nxt, depth + 1))

    return unique_records(found)


def filename_from_url(url: str) -> str:
    path_name = Path(urlparse(url).path).name or "download.pdf"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", path_name)
    return safe if safe.lower().endswith(".pdf") else f"{safe}.pdf"


def download_pdf(url: str, output_dir: Path, timeout: int) -> Path:
    req = Request(url, headers={"User-Agent": DEFAULT_UA})
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / filename_from_url(url)
    i = 1
    while target.exists():
        target = output_dir / f"{target.stem}_{i}{target.suffix}"
        i += 1

    with urlopen(req, timeout=timeout) as resp, target.open("wb") as f:
        while True:
            chunk = resp.read(64 * 1024)
            if not chunk:
                break
            f.write(chunk)
    return target


def write_report(records: list[PdfRecord], path: Path) -> None:
    payload = [asdict(r) for r in records]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run(config: Path, output_dir: Path, timeout: int, dry_run: bool, report: Path | None, max_pages: int) -> int:
    sources = load_sources(config)
    all_records: list[PdfRecord] = []

    for source in sources:
        print(f"[INFO] Crawling source: {source.name}")
        records = crawl_source(source, timeout=timeout, max_pages=max_pages)
        print(f"[INFO] {source.name}: found {len(records)} PDFs")
        all_records.extend(records)

    records = unique_records(all_records)
    print(f"\nTotal unique PDF links: {len(records)}")
    for idx, rec in enumerate(records, start=1):
        print(f"{idx:03d}. [{rec.source}] {rec.text or '(untitled)'} -> {rec.pdf_url}")

    if report:
        write_report(records, report)
        print(f"[INFO] Report written: {report}")

    if dry_run:
        return 0

    for rec in records:
        try:
            path = download_pdf(rec.pdf_url, output_dir=output_dir, timeout=timeout)
            print(f"[OK] Downloaded: {path}")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            print(f"[WARN] download failed: {rec.pdf_url} ({exc})", file=sys.stderr)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Rajasthan government rule PDFs")
    p.add_argument("--config", type=Path, default=Path("sources.json"), help="Path to source JSON")
    p.add_argument("--output-dir", type=Path, default=Path("downloads"), help="PDF download directory")
    p.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    p.add_argument("--max-pages", type=int, default=120, help="Max pages crawled per source")
    p.add_argument("--dry-run", action="store_true", help="Only list links, do not download")
    p.add_argument("--report", type=Path, default=Path("reports/pdfs.json"), help="Output JSON report path")
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    raise SystemExit(
        run(
            config=a.config,
            output_dir=a.output_dir,
            timeout=a.timeout,
            dry_run=a.dry_run,
            report=a.report,
            max_pages=a.max_pages,
        )
    )
