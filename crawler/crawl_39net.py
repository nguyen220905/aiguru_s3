"""Specialized high-performance async crawler for *.39.net domains.

Features:
- Uses curl_cffi with Chrome 124 TLS impersonation to bypass 39.net bot detection & slider captchas
- Fallback to HTTP entry (which cleanly redirects to HTTPS) to avoid TLS connection resets
- Concurrent worker pool across all 22 subdomains of 39.net
- Direct compatibility with existing crawl/db/<host>.sqlite databases (zstd level 6 compression)
- Resumable: queries existing sqlite databases so previously crawled URLs are skipped
- Real-time throughput (req/s) and host completion reporting

Usage:
    python crawler/crawl_39net.py                    # crawl all pending 39.net subdomains
    python crawler/crawl_39net.py --concurrency 16   # 16 concurrent requests
    python crawler/crawl_39net.py --hosts ask.39.net # only crawl ask.39.net
    python crawler/crawl_39net.py --hosts smaller    # crawl all 21 smaller subdomains first
"""

import argparse
import asyncio
import logging
import os
import random
import re
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse

import pandas as pd
import zstandard
from curl_cffi.requests import AsyncSession

ROOT = Path("D:/project_r2ai")
CORPUS = ROOT / "data" / "links_corpus.parquet"
DB_DIR = ROOT / "crawl" / "db"

DONE_KINDS = ("ok", "notfound", "home", "robots", "http4xx", "nonhtml")
MAX_ATTEMPTS = 5
MAX_BODY = 10 * 1024 * 1024

STRIP_RE = re.compile(rb"<style\b.*?</style>|<svg\b.*?</svg>|<!--.*?-->", re.S | re.I)
NON_HTML_EXT = re.compile(r"\.(jpe?g|png|gif|webp|bmp|svg|pdf|docx?|xlsx?|zip|mp4|mp3)$", re.I)

log = logging.getLogger("crawl_39net")


def is_home(orig: str, final: str) -> bool:
    o, f = urlparse(orig), urlparse(final)
    return f.path in ("", "/") and not f.query and o.path not in ("", "/")


def is_non_html(final_url: str, ctype: str, body: bytes) -> bool:
    ctype = ctype.lower()
    if ctype and not any(t in ctype for t in ("html", "xml", "text/plain")):
        return True
    if NON_HTML_EXT.search(urlparse(final_url).path):
        return True
    head = body[:16].lstrip()
    return head.startswith((b"\xff\xd8\xff", b"\x89PNG", b"GIF8", b"%PDF", b"RIFF", b"PK\x03\x04"))


def classify(status: int, final_url: str, body: bytes, headers: dict) -> str:
    low = body[:4000].lower()
    if "verify.html" in final_url or "captcha" in final_url.lower() or "滑动拼图验证".encode() in body[:6000]:
        return "captcha"
    if b"just a moment..." in low or b"attention required! | cloudflare" in low:
        return "blocked"
    if len(body) < 2000 and b"document.cookie" in low and b"location.reload" in low:
        return "blocked"
    if status in (403, 429):
        return "blocked"
    if status in (404, 410):
        return "notfound"
    if status >= 500:
        return "error"
    if status >= 400:
        return "http4xx"
    if is_non_html(final_url, headers.get("content-type", headers.get("Content-Type", "")), body):
        return "nonhtml"
    return "ok"


class HostStore:
    """Manages thread-safe SQLite storage for a single host."""

    SCHEMA = """CREATE TABLE IF NOT EXISTS pages(
        id INTEGER PRIMARY KEY, url TEXT, final_url TEXT, status INTEGER, kind TEXT,
        ctype TEXT, nbytes INTEGER, fetched_at REAL, attempts INTEGER, err TEXT, html BLOB)"""

    def __init__(self, host: str):
        self.host = host
        DB_DIR.mkdir(parents=True, exist_ok=True)
        self.path = DB_DIR / f"{host}.sqlite"
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=60)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(self.SCHEMA)
        self.conn.commit()
        self.buf: List[tuple] = []
        self.last_flush = time.time()
        self.cctx = zstandard.ZstdCompressor(level=6)
        self.lock = asyncio.Lock()

    def get_done_ids(self) -> Set[int]:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT id FROM pages WHERE kind IN ({','.join('?' * len(DONE_KINDS))})"
            f" OR attempts >= {MAX_ATTEMPTS}",
            DONE_KINDS,
        )
        return {r[0] for r in cur.fetchall()}

    def get_attempts(self) -> Dict[int, int]:
        cur = self.conn.cursor()
        cur.execute("SELECT id, attempts FROM pages")
        return dict(cur.fetchall())

    def add(self, row: tuple):
        self.buf.append(row)

    def _write(self, rows: list):
        self.conn.executemany(
            "INSERT OR REPLACE INTO pages(id,url,final_url,status,kind,ctype,nbytes,fetched_at,attempts,err,html)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

    async def maybe_flush(self, force: bool = False):
        if not self.buf or (not force and len(self.buf) < 100 and time.time() - self.last_flush < 10):
            return
        async with self.lock:
            rows, self.buf = self.buf, []
            self.last_flush = time.time()
            if rows:
                await asyncio.to_thread(self._write, rows)

    def close(self):
        if self.buf:
            self._write(self.buf)
            self.buf.clear()
        try:
            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
            self.conn.close()
        except Exception:
            pass


class Crawler39Net:
    def __init__(self, concurrency: int = 12, host_filter: str = "all", limit: int = 0):
        self.concurrency = concurrency
        self.host_filter = host_filter
        self.limit = limit
        self.stores: Dict[str, HostStore] = {}
        self.queue: asyncio.Queue = asyncio.Queue()
        self.total_queued = 0
        self.done_count = 0
        self.ok_count = 0
        self.err_count = 0
        self.captcha_count = 0
        self.start_time = time.time()
        self.session_lock = asyncio.Lock()
        self.active_sessions: List[AsyncSession] = []

    def load_corpus(self):
        log.info("Reading corpus from %s...", CORPUS)
        corpus = pd.read_parquet(CORPUS)
        corpus["host"] = corpus["url"].apply(lambda u: urlparse(u).netloc.lower())
        df_39 = corpus[corpus["host"].str.endswith("39.net")].copy()

        hosts = sorted(df_39["host"].unique())
        if self.host_filter == "smaller":
            # All 39.net subdomains except ask.39.net
            hosts = [h for h in hosts if h != "ask.39.net"]
        elif self.host_filter != "all":
            selected = set(self.host_filter.split(","))
            hosts = [h for h in hosts if h in selected]

        log.info("Targeting hosts: %s", ", ".join(hosts))

        tasks_by_host = defaultdict(list)
        total_pending = 0

        for h in hosts:
            store = self.stores[h] = HostStore(h)
            done_ids = store.get_done_ids()
            attempts_map = store.get_attempts()

            grp = df_39[df_39["host"] == h]
            for _, row in grp.iterrows():
                doc_id = int(row["id"])
                if doc_id not in done_ids:
                    attempts = attempts_map.get(doc_id, 0)
                    tasks_by_host[h].append((h, doc_id, str(row["url"]), attempts))

            cnt = len(tasks_by_host[h])
            total_pending += cnt
            log.info("  Host %-25s: %7d pending (already done: %7d)", h, cnt, len(done_ids))

        # Interleave items across hosts to prioritize diversity and finish small hosts quickly
        all_items = []
        rng = random.Random(42)

        # Separate ask.39.net from smaller hosts
        smaller_items = []
        ask_items = []
        for h, items in tasks_by_host.items():
            rng.shuffle(items)
            if h == "ask.39.net":
                ask_items.extend(items)
            else:
                smaller_items.extend(items)

        rng.shuffle(smaller_items)
        rng.shuffle(ask_items)

        # Small hosts first, then ask.39.net
        all_items = smaller_items + ask_items

        if self.limit > 0:
            all_items = all_items[: self.limit]

        for item in all_items:
            self.queue.put_nowait(item)

        self.total_queued = len(all_items)
        log.info("Total pending URLs queued for crawl: %d", self.total_queued)

    async def fetch_url(self, session: AsyncSession, item: Tuple[str, int, str, int]):
        host, doc_id, url, attempts = item
        store = self.stores[host]
        attempts += 1

        # Fallback to http:// entry URL to bypass port 443 TLS timeouts on 39.net
        request_url = url
        if request_url.startswith("https://"):
            request_url = "http://" + request_url[8:]

        status, final_url, ctype, body, err = None, None, None, b"", None
        kind = "error"

        try:
            r = await session.get(request_url, timeout=12, allow_redirects=True)
            status = r.status_code
            final_url = str(r.url)
            ctype = r.headers.get("content-type", r.headers.get("Content-Type", ""))
            body = r.content[:MAX_BODY]
            kind = classify(status, final_url, body, dict(r.headers))
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"[:300]
            kind = "error"

        if kind == "ok" and is_home(url, final_url or ""):
            kind = "home"

        if kind == "captcha":
            self.captcha_count += 1
            log.warning("Received captcha on %s, backing off 15s...", host)
            await asyncio.sleep(15)
            if attempts < MAX_ATTEMPTS:
                await self.queue.put((host, doc_id, url, attempts))
                return
        elif kind == "error":
            self.err_count += 1
            if attempts < MAX_ATTEMPTS:
                await self.queue.put((host, doc_id, url, attempts))
                return
        elif kind == "ok":
            self.ok_count += 1

        self.done_count += 1

        html = None
        if kind == "ok" and body:
            cleaned = STRIP_RE.sub(b"", body)
            html = store.cctx.compress(cleaned)

        store.add((doc_id, url, final_url, status, kind, ctype, len(body), time.time(), attempts, err, html))
        await store.maybe_flush()

    async def worker(self, worker_id: int):
        async with AsyncSession(impersonate="chrome124") as session:
            while not self.queue.empty():
                try:
                    item = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    await self.fetch_url(session, item)
                except Exception as e:
                    log.error("Worker %d exception: %s", worker_id, e)
                finally:
                    self.queue.task_done()

                # Polite spacing per worker (100-300ms)
                await asyncio.sleep(random.uniform(0.1, 0.3))

    async def progress_reporter(self):
        prev_done = 0
        prev_time = time.time()
        while self.done_count < self.total_queued and not self.queue.empty():
            await asyncio.sleep(10)
            now = time.time()
            dt = now - prev_time
            d_done = self.done_count - prev_done
            rate = d_done / dt if dt > 0 else 0
            pct = (self.done_count / self.total_queued * 100) if self.total_queued > 0 else 0
            log.info(
                "[PROGRESS] Done: %d/%d (%.1f%%) | OK: %d | Err: %d | Captcha: %d | Rate: %.2f req/s",
                self.done_count,
                self.total_queued,
                pct,
                self.ok_count,
                self.err_count,
                self.captcha_count,
                rate,
            )
            prev_done = self.done_count
            prev_time = now

            # Periodic flush across all stores
            for store in self.stores.values():
                await store.maybe_flush(force=True)

    async def run(self):
        self.load_corpus()
        if self.total_queued == 0:
            log.info("No pending URLs to crawl. All done!")
            return

        reporter_task = asyncio.create_task(self.progress_reporter())
        workers = [asyncio.create_task(self.worker(i)) for i in range(self.concurrency)]

        await asyncio.gather(*workers)
        reporter_task.cancel()

        # Final flush & close
        for store in self.stores.values():
            store.close()

        total_dur = time.time() - self.start_time
        log.info("=" * 60)
        log.info("CRAWL 39.NET COMPLETED in %.1fs", total_dur)
        log.info("Total crawled: %d | OK: %d | Errors: %d", self.done_count, self.ok_count, self.err_count)
        log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Specialized crawler for 39.net")
    parser.add_argument("--concurrency", type=int, default=12, help="Max concurrent workers (default 12)")
    parser.add_argument("--hosts", type=str, default="all", help="'all', 'smaller', or comma-separated hosts")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit on total URLs to crawl")
    args = parser.parse_args()

    crawler = Crawler39Net(concurrency=args.concurrency, host_filter=args.hosts, limit=args.limit)
    asyncio.run(crawler.run())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
