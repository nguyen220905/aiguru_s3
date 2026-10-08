"""Map zysjonline.com/books/<slug>/<id>/ to archived chapter pages of the old zysj.com.cn site.

The old book index page lists every section in reading order as <li id="si<N>">, with consecutive N
including header-only entries (author line, volume headers). zysjonline renumbered the same sequence with
a constant shift, and the corpus leaves out the header-only ids. So for each book we look for the shift k
with new_id = si + k such that the corpus ids land exactly on content entries; the gap pattern makes a
wrong k fail (checked on bencaofenjing: 17/17 gaps line up with the 17 headers).

Step 1 (fetch): download each book's index page from the Wayback Machine (cached in crawl/wayback/zysj_index/).
Step 2 (map):   align and write crawl/wayback/alt_map_books.tsv (corpus_url, archived chapter url, method).

Usage: python zysj_books.py [fetch|map|all]
"""
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict

import numpy as np
import pandas as pd
import requests

from crawl import CORPUS, OUT

WB = OUT / "wayback"
CACHE = WB / "zysj_index"
CUTOFF = "20260301"
UA = "r2ai-research-crawler/0.1 (archive lookup for a non-commercial IR competition)"
SLEEP = 4.0
MIN_EXACT = 0.995   # share of a book's corpus ids that must land on content entries

log = logging.getLogger("zysj_books")


def archived_paths():
    paths = {}
    with open(WB / "zysj.com.cn.cdx.tsv", encoding="utf-8") as fh:
        for ts, orig in csv.reader(fh, delimiter="\t"):
            if ts >= CUTOFF:
                continue
            p = re.sub(r"^https?://(www\.)?zysj\.com\.cn(:80)?", "", orig).split("?")[0]
            if ts > paths.get(p, ("",))[0]:
                paths[p] = (ts, orig)
    return paths


# zysjonline section -> old zysj.com.cn section holding the same catalog
SECTION = {"books": "lilunshuji", "columns": "bingzheng"}


def corpus_books():
    """(kind, slug) -> {new id: corpus url} for zysjonline books and columns."""
    df = pd.read_parquet(CORPUS)
    b = df[df.url.str.contains(r"://zysjonline\.com/(?:books|columns)/", regex=True)]
    books = defaultdict(dict)
    for url in b.url:
        kind, slug, num = url.rstrip("/").split("/")[-3:]
        books[(kind, slug)][int(num)] = url
    return books


def old_slug(kind, slug, paths):
    sec = SECTION[kind]
    for s in (slug, re.sub(r"\d+$", "", slug)):
        if f"/{sec}/{s}/index.html" in paths or f"/{sec}/{s}/" in paths:
            return s
    return None


def cache_file(kind, o):
    return CACHE / (f"{o}.html" if kind == "books" else f"{SECTION[kind]}_{o}.html")


def report_name(kind, slug):
    return slug if kind == "books" else f"{kind}/{slug}"


def step_fetch():
    CACHE.mkdir(parents=True, exist_ok=True)
    paths = archived_paths()
    books = corpus_books()
    s = requests.Session()
    s.headers["User-Agent"] = UA
    todo = []
    for kind, slug in sorted(books, key=lambda k: -len(books[k])):
        o = old_slug(kind, slug, paths)
        if o is None or cache_file(kind, o).exists():
            continue
        sec = SECTION[kind]
        cap = paths.get(f"/{sec}/{o}/index.html") or paths.get(f"/{sec}/{o}/")
        todo.append((kind, o, cap))
    log.info("%d index pages to fetch", len(todo))
    for n, (kind, o, (ts, orig)) in enumerate(todo, 1):
        url = f"https://web.archive.org/web/{ts}id_/{orig}"
        for attempt in range(8):
            try:
                r = s.get(url, timeout=120)
                if r.status_code in (429, 503):
                    log.warning("throttled (%s), waiting 10 min", r.status_code)
                    time.sleep(600)
                    continue
                if r.status_code == 200:
                    cache_file(kind, o).write_bytes(r.content)
                else:
                    log.warning("%s -> HTTP %s", o, r.status_code)
                break
            except requests.ConnectionError as e:
                log.warning("connection refused/reset (%s), waiting 10 min", str(e)[:80])
                time.sleep(600)
            except requests.RequestException as e:
                log.warning("%s -> %s", o, e)
                time.sleep(30)
        if n % 25 == 0:
            log.info("index pages %d / %d", n, len(todo))
        time.sleep(SLEEP)


def parse_index(html):
    """Ordered [(si, href or None)] of the catalog; href is the entry's own chapter link."""
    cat = html[html.find('id="catalog-content"'):]
    out = []
    for sid, body in re.findall(r'<li id="si(\d+)">(.*?)(?=<li id="si|</ul>|$)', cat, re.S):
        m = re.search(r'<a href="([^"#]+\.html)"(?![^>]*catalog_group)', body)
        href = m.group(1) if m and "_group" not in m.group(1) and "quanben" not in m.group(1) else None
        out.append((int(sid), href))
    return out


def align(items, ids):
    """Best shift k (new = si + k), the share of corpus ids landing on content entries, the number of
    corpus ids outside the catalog, and how many shifts reach MIN_EXACT. Every shift that puts the first
    corpus id on some content entry is tried; a catalog with too few header gaps can be matched by several
    shifts, and such an alignment is ambiguous."""
    content = np.array(sorted(si for si, href in items if href), dtype=np.int64)
    if not len(content):
        return None, 0.0, len(ids), 0
    lo = int(min(si for si, _ in items))
    span = int(max(si for si, _ in items)) - lo + 1
    is_content = np.zeros(span, dtype=bool)
    is_content[content - lo] = True
    in_catalog = np.zeros(span, dtype=bool)
    in_catalog[np.array([si for si, _ in items]) - lo] = True
    new = np.array(sorted(ids), dtype=np.int64)
    shares = {}
    for k in set((new[0] - content).tolist()):
        pos = new - k - lo
        ok = (pos >= 0) & (pos < span)
        shares[k] = int(is_content[pos[ok]].sum()) / len(new)
    k = max(shares, key=shares.get)
    n_good = sum(1 for s in shares.values() if s >= MIN_EXACT)
    pos = new - k - lo
    inside = (pos >= 0) & (pos < span)
    stray = int((~inside).sum() + (~in_catalog[pos[inside]]).sum())
    return k, shares[k], stray, n_good


def step_map():
    paths = archived_paths()
    books = corpus_books()
    rows, report, full = [], [], []   # full: every aligned chapter, archived in Wayback or not (for cc.py)
    for (kind, slug), ids in sorted(books.items(), key=lambda kv: -len(kv[1])):
        o = old_slug(kind, slug, paths)
        name = report_name(kind, slug)
        f = cache_file(kind, o) if o else None
        if not f or not f.exists():
            report.append(dict(book=name, n=len(ids), status="no index"))
            continue
        items = parse_index(f.read_text(encoding="utf-8", errors="ignore"))
        if not items:
            report.append(dict(book=name, n=len(ids), status="unparsed index"))
            continue
        k, share, stray, n_good = align(items, ids)
        ok = k is not None and share >= MIN_EXACT and n_good == 1
        href = dict(items)
        archived = 0
        if ok:
            for new_id, url in ids.items():
                h = href.get(new_id - k)
                if not h:
                    continue
                h = re.sub(r"^https?://(www\.)?zysj\.com\.cn", "", h)
                full.append((url, "http://www.zysj.com.cn" + h, f"book-si k={k}"))
                # chapter pages appear both as <si>.html and as <book>-<vol>-<ch>.html
                for cand in (h, f"/{SECTION[kind]}/{o}/{new_id - k}.html"):
                    if cand in paths:
                        rows.append((url, paths[cand][1], f"book-si k={k}"))
                        archived += 1
                        break
        status = "aligned" if ok else ("ambiguous" if n_good > 1 else "rejected")
        report.append(dict(book=name, n=len(ids), status=status, shift=k, exact_share=round(share, 4),
                           stray=stray, shifts_matching=n_good, archived=archived))
    with open(WB / "alt_map_books.tsv", "w", encoding="utf-8", newline="") as fh:
        csv.writer(fh, delimiter="\t").writerows(rows)
    with open(WB / "zysj_books_full.tsv", "w", encoding="utf-8", newline="") as fh:
        csv.writer(fh, delimiter="\t").writerows(full)
    rep = pd.DataFrame(report)
    rep.to_csv(WB / "zysj_books_report.csv", index=False)
    print(rep.groupby("status").agg(books=("book", "size"), urls=("n", "sum"),
                                    archived=("archived", "sum")).to_string())
    print("alt_map_books rows:", len(rows))


if __name__ == "__main__":
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / "zysj_books.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd in ("fetch", "all"):
        step_fetch()
    if cmd in ("map", "all"):
        step_map()
