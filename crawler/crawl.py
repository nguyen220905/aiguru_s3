"""Resumable, polite async crawler for links_corpus.parquet.

Raw HTML is stored zstd-compressed in one SQLite file per host (crawl/db/<host>.sqlite) so text
extraction can be redone later without re-downloading. Rate limits are applied per site group
(registered domain), and groups that start returning captchas / WAF blocks are paused with
exponential backoff and eventually disabled for the run (no attempt is made to solve them).

Usage:
    python crawl.py                       # crawl everything still pending
    python crawl.py --limit-per-host 30   # pilot run
    python crawl.py --hosts www.cnkang.com,ask.39.net
"""
import argparse
import asyncio
import json
import logging
import random
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
import pandas as pd
import zstandard

ROOT = Path("D:/project_r2ai")
CORPUS = ROOT / "data" / "links_corpus.parquet"
OUT = ROOT / "crawl"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,zh-CN;q=0.8,zh;q=0.7,en;q=0.6",
}

# Hosts that answered the probe with a Cloudflare challenge / JS cookie challenge. Skipped unless
# --include-blocked is passed (the circuit breaker would disable them anyway).
BLOCKED_HOSTS = {
    "zysjonline.com", "nhathuoclongchau.com.vn", "tamanhhospital.vn", "vov.vn",
    "pmc-ecm-healthblog.beta.pharmacity.io", "laodong.vn",
}

# (requests per second, max concurrent connections) per site group.
DEFAULT_LIMIT = (2.0, 4)
GROUP_LIMITS = {
    "cnkang.com": (16.0, 40),
    "120ask.com": (16.0, 40),
    "familydoctor.com.cn": (12.0, 28),
    "39.net": (3.0, 12),          # all subdomains share one limiter; slider captcha when pushed harder; slow (3-4s) responses
    "a-hospital.com": (10.0, 20),
    "zhongyibaodian.net": (8.0, 20),
    "zydcd.com": (8.0, 16),
    "wujue.com": (6.0, 16),
    "youlai.cn": (8.0, 16),
    "qihuangzhishu.com": (6.0, 12),
    "suckhoecongdongonline.vn": (10.0, 20),
    "suckhoedoisong.vn": (4.0, 8),
    "thanhnien.vn": (4.0, 8),
    "giadinhonline.vn": (3.0, 6),
    "vinmec.com": (3.0, 6),
    "medlatec.vn": (3.0, 6),
    "vov2.vov.vn": (2.0, 4),      # separate from Cloudflare-blocked vov.vn
    "iiyi.com": (2.0, 4),
    "pmphai.com": (2.0, 4),
}

DONE_KINDS = ("ok", "notfound", "home", "robots", "http4xx")
MAX_ATTEMPTS = 5          # across runs, for transient errors
MAX_BODY = 10 * 1024 * 1024
BLOCK_PAUSES = [60, 120, 300, 600, 1200, 1800, 1800, 1800]  # seconds; group disabled for the run after the last
MIN_RATE = 0.2            # req/s floor for the adaptive limiter
SPEEDUP_EVERY = 500       # successes needed before the limiter creeps back toward its configured rate
ERROR_STREAK_LIMIT = 60   # consecutive network/5xx errors on a host -> disable host for the run

STRIP_RE = re.compile(rb"<style\b.*?</style>|<svg\b.*?</svg>|<!--.*?-->", re.S | re.I)

MULTI_PART_SUFFIXES = {"com.cn", "com.vn", "org.vn", "gov.vn", "edu.vn", "net.vn", "org.cn", "gov.cn"}

log = logging.getLogger("crawl")


def group_of(host: str) -> str:
    if host in GROUP_LIMITS:
        return host
    parts = host.split(".")
    n = 3 if ".".join(parts[-2:]) in MULTI_PART_SUFFIXES else 2
    return ".".join(parts[-n:])


def is_home(orig: str, final: str) -> bool:
    o, f = urlparse(orig), urlparse(final)
    return f.path in ("", "/") and not f.query and o.path not in ("", "/")


def classify(status: int, final_url: str, body: bytes, headers) -> str:
    low = body[:4000].lower()
    if "verify.html" in final_url or "captcha" in final_url.lower() or "滑动拼图验证".encode() in body[:6000]:
        return "captcha"
    if headers.get("cf-mitigated") or b"just a moment..." in low or b"attention required! | cloudflare" in low:
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
    return "ok"


class HostStore:
    """One SQLite file per host; writes are buffered and flushed from a worker thread."""

    SCHEMA = """CREATE TABLE IF NOT EXISTS pages(
        id INTEGER PRIMARY KEY, url TEXT, final_url TEXT, status INTEGER, kind TEXT,
        ctype TEXT, nbytes INTEGER, fetched_at REAL, attempts INTEGER, err TEXT, html BLOB)"""

    def __init__(self, host: str, subdir: str = "db"):
        self.host = host
        (OUT / subdir).mkdir(parents=True, exist_ok=True)
        self.path = OUT / subdir / f"{host}.sqlite"
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=60)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(self.SCHEMA)
        self.conn.commit()
        self.buf = []
        self.lock = asyncio.Lock()
        self.last_flush = time.time()
        self.cctx = zstandard.ZstdCompressor(level=6)

    def state(self):
        done = {r[0] for r in self.conn.execute(
            f"SELECT id FROM pages WHERE kind IN ({','.join('?' * len(DONE_KINDS))})", DONE_KINDS)}
        exhausted = {r[0] for r in self.conn.execute(
            "SELECT id FROM pages WHERE attempts >= ?", (MAX_ATTEMPTS,))}
        attempts = dict(self.conn.execute("SELECT id, attempts FROM pages"))
        return done | exhausted, attempts

    def add(self, row):
        self.buf.append(row)

    def _write(self, rows):
        self.conn.executemany(
            "INSERT OR REPLACE INTO pages(id,url,final_url,status,kind,ctype,nbytes,fetched_at,attempts,err,html)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        self.conn.commit()

    async def maybe_flush(self, force=False):
        if not self.buf or (not force and len(self.buf) < 200 and time.time() - self.last_flush < 15):
            return
        async with self.lock:
            rows, self.buf = self.buf, []
            self.last_flush = time.time()
            if rows:
                await asyncio.to_thread(self._write, rows)


class Group:
    def __init__(self, name, rate, conc):
        self.name = name
        self.base_interval = self.interval = 1.0 / rate
        self.conc = conc
        self.ok_streak = 0
        self.next_slot = 0.0
        self.paused_until = 0.0
        self.strikes = 0
        self.disabled = False
        self.queue = []          # (host, id, url, attempts)
        self.err_streak = defaultdict(int)
        self.dead_hosts = set()

    async def wait_turn(self):
        while True:
            now = time.monotonic()
            if now < self.paused_until:
                await asyncio.sleep(min(self.paused_until - now, 5))
                continue
            if self.next_slot <= now:
                self.next_slot = now + self.interval
                return
            await asyncio.sleep(self.next_slot - now)

    def on_block(self, kind, host):
        now = time.monotonic()
        if now < self.paused_until:
            return  # already paused because of another in-flight request
        if self.strikes >= len(BLOCK_PAUSES):
            self.disabled = True
            log.warning("group %s disabled for this run after repeated %s (last host %s)", self.name, kind, host)
            return
        pause = BLOCK_PAUSES[self.strikes]
        self.strikes += 1
        self.paused_until = now + pause
        self.ok_streak = 0
        self.interval = min(self.interval * 1.5, 1.0 / MIN_RATE)
        log.warning("group %s got %s on %s -> pause %ds (strike %d), rate now %.2f req/s",
                    self.name, kind, host, pause, self.strikes, 1.0 / self.interval)

    def on_success(self):
        self.strikes = 0
        self.ok_streak += 1
        if self.ok_streak >= SPEEDUP_EVERY and self.interval > self.base_interval:
            self.ok_streak = 0
            self.interval = max(self.base_interval, self.interval / 1.1)


class Crawler:
    def __init__(self, args):
        self.args = args
        self.stores = {}
        self.groups = {}
        self.robots = {}
        self.stats = defaultdict(lambda: defaultdict(int))
        self.pending_total = defaultdict(int)
        self.started = time.time()

    def load(self):
        df = pd.read_parquet(CORPUS)
        df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
        if self.args.hosts:
            df = df[df.host.isin(self.args.hosts.split(","))]
        if self.args.exclude_hosts:
            df = df[~df.host.isin(self.args.exclude_hosts.split(","))]
        if not self.args.include_blocked:
            df = df[~df.host.isin(BLOCKED_HOSTS)]
        rng = random.Random(42)
        for host, g in df.groupby("host"):
            store = self.stores[host] = HostStore(host)
            done, attempts = store.state()
            items = [(host, i, u, attempts.get(i, 0)) for i, u in zip(g.id.tolist(), g.url.tolist()) if i not in done]
            rng.shuffle(items)
            if self.args.limit_per_host:
                items = items[: self.args.limit_per_host]
            gname = group_of(host)
            if gname not in self.groups:
                rate, conc = GROUP_LIMITS.get(gname, DEFAULT_LIMIT)
                self.groups[gname] = Group(gname, rate * self.args.rate_scale, conc)
            self.groups[gname].queue.extend(items)
            self.pending_total[host] = len(items)
            self.stats[host]["already_done"] = len(done)
        for grp in self.groups.values():
            # interleave hosts of a group so small hosts are not starved behind a big one
            rng.shuffle(grp.queue)
            grp.queue.reverse()  # pop() from the end
        log.info("loaded %d pending urls in %d groups / %d hosts",
                 sum(self.pending_total.values()), len(self.groups), len(self.stores))

    async def load_robots(self, session, host):
        rp = RobotFileParser()
        try:
            async with session.get(f"https://{host}/robots.txt", allow_redirects=True) as r:
                if r.status == 200:
                    rp.parse((await r.read()).decode("utf-8", "ignore").splitlines())
                else:
                    rp.allow_all = True
        except Exception:
            rp.allow_all = True
        self.robots[host] = rp
        cd = rp.crawl_delay("*") if not rp.allow_all else None
        if cd:
            grp = self.groups[group_of(host)]
            grp.base_interval = grp.interval = max(grp.interval, float(cd))
            log.info("%s crawl-delay %s", host, cd)

    async def fetch_one(self, session, grp, host, doc_id, url, attempts):
        store = self.stores[host]
        rp = self.robots.get(host)
        if rp is not None and not rp.allow_all and not rp.can_fetch("*", url):
            store.add((doc_id, url, None, None, "robots", None, 0, time.time(), attempts, None, None))
            self.stats[host]["robots"] += 1
            return
        await grp.wait_turn()
        if grp.disabled or host in grp.dead_hosts:
            return
        attempts += 1
        status, final, ctype, body, err = None, None, None, b"", None
        try:
            async with session.get(url, allow_redirects=True, max_redirects=10) as r:
                status, final, ctype = r.status, str(r.url), r.headers.get("Content-Type", "")
                chunks, size = [], 0
                async for chunk in r.content.iter_chunked(1 << 16):
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > MAX_BODY:
                        break
                body = b"".join(chunks)
                kind = classify(status, final, body, r.headers)
        except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeError, ValueError) as e:
            kind, err = "error", f"{type(e).__name__}: {e}"[:300]

        if kind == "ok" and is_home(url, final):
            kind = "home"
        if kind in ("captcha", "blocked"):
            grp.on_block(kind, host)
            if attempts < MAX_ATTEMPTS and not grp.disabled:
                grp.queue.insert(0, (host, doc_id, url, attempts))  # retry after the pause
            else:
                store.add((doc_id, url, final, status, kind, ctype, len(body), time.time(), attempts, err, None))
            self.stats[host][kind] += 1
            return
        if kind == "error":
            grp.err_streak[host] += 1
            if grp.err_streak[host] >= ERROR_STREAK_LIMIT and host not in grp.dead_hosts:
                grp.dead_hosts.add(host)
                log.warning("host %s disabled for this run after %d consecutive errors (last: %s %s)",
                            host, ERROR_STREAK_LIMIT, status, err)
        else:
            grp.err_streak[host] = 0
            grp.on_success()
        html = store.cctx.compress(STRIP_RE.sub(b"", body)) if kind == "ok" else None
        store.add((doc_id, url, final, status, kind, ctype, len(body), time.time(), attempts, err, html))
        self.stats[host][kind] += 1
        await store.maybe_flush()

    async def run_group(self, grp):
        jar = aiohttp.CookieJar(unsafe=True, quote_cookie=False)
        conn = aiohttp.TCPConnector(limit=grp.conc * 2, ssl=False, ttl_dns_cache=3600)
        timeout = aiohttp.ClientTimeout(total=60, sock_connect=20)
        async with aiohttp.ClientSession(headers=HEADERS, cookie_jar=jar, connector=conn,
                                         timeout=timeout, auto_decompress=True) as session:
            hosts = {h for h, *_ in grp.queue}
            await asyncio.gather(*[self.load_robots(session, h) for h in hosts])

            async def worker():
                while grp.queue and not grp.disabled:
                    host, doc_id, url, attempts = grp.queue.pop()
                    if host in grp.dead_hosts:
                        continue
                    try:
                        await self.fetch_one(session, grp, host, doc_id, url, attempts)
                    except Exception:
                        log.exception("unexpected error on %s", url)

            await asyncio.gather(*[worker() for _ in range(grp.conc)])
        for h in hosts:
            await self.stores[h].maybe_flush(force=True)
        log.info("group %s finished (disabled=%s, dead hosts=%s)", grp.name, grp.disabled, sorted(grp.dead_hosts))

    async def reporter(self):
        prev, prev_t = 0, time.time()
        while True:
            await asyncio.sleep(60)
            for s in self.stores.values():
                await s.maybe_flush(force=True)
            self.write_status()
            total = sum(sum(v for k, v in st.items() if k != "already_done") for st in self.stats.values())
            now = time.time()
            log.info("progress: %d fetched this run, %.1f req/s last minute", total, (total - prev) / (now - prev_t))
            prev, prev_t = total, now

    def write_status(self):
        rows = {}
        for host, st in sorted(self.stats.items()):
            rows[host] = dict(st, pending_at_start=self.pending_total[host])
        groups = {g.name: dict(queue=len(g.queue), rate=round(1.0 / g.interval, 2), disabled=g.disabled, strikes=g.strikes,
                               paused_for=max(0, round(g.paused_until - time.monotonic())),
                               dead_hosts=sorted(g.dead_hosts)) for g in self.groups.values()}
        (OUT / f"status{self.args.tag}.json").write_text(json.dumps(
            dict(updated=time.strftime("%Y-%m-%d %H:%M:%S"), elapsed_min=round((time.time() - self.started) / 60, 1),
                 groups=groups, hosts=rows), ensure_ascii=False, indent=1), encoding="utf-8")

    async def main(self):
        self.load()
        rep = asyncio.create_task(self.reporter())
        await asyncio.gather(*[self.run_group(g) for g in self.groups.values() if g.queue])
        rep.cancel()
        for s in self.stores.values():
            await s.maybe_flush(force=True)
        self.write_status()
        log.info("all groups finished")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", default="")
    ap.add_argument("--exclude-hosts", default="")
    ap.add_argument("--limit-per-host", type=int, default=0)
    ap.add_argument("--rate-scale", type=float, default=1.0, help="multiply every group's rate")
    ap.add_argument("--include-blocked", action="store_true")
    ap.add_argument("--tag", default="", help="suffix for log/status files when running several crawlers")
    args = ap.parse_args()
    (OUT / "db").mkdir(parents=True, exist_ok=True)
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / f"crawl{args.tag}.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    asyncio.run(Crawler(args).main())


if __name__ == "__main__":
    main()
