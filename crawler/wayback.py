"""Recover pages of hosts we cannot fetch directly (Cloudflare-blocked or origin down) from the
Internet Archive's Wayback Machine.

Step 1 (cdx):   list archived 200/text-html captures per URL prefix -> crawl/wayback/<host>.cdx.tsv
Step 2 (fetch): download the raw archived HTML (id_ endpoint) for corpus URLs that have a capture,
                newest capture first, falling back to older ones if the capture is a challenge page.
                Stored like the main crawl, in crawl/db_wayback/<host>.sqlite (final_url = archive URL).

Usage:
    python wayback.py cdx
    python wayback.py fetch
"""
import asyncio
import csv
import logging
import sqlite3
import ssl
import sys
import time
from collections import defaultdict
from urllib.parse import urlparse

import aiohttp
import certifi
import pandas as pd
import requests

from crawl import CORPUS, OUT, STRIP_RE, HostStore, classify, stored_ok

TARGET_HOSTS = [
    "nhathuoclongchau.com.vn", "laodong.vn", "vov.vn", "tamanhhospital.vn", "bingli.iiyi.com",
    "zysjonline.com", "pmc-ecm-healthblog.beta.pharmacity.io", "article.iiyi.com",
]
FETCH_ORDER = ["zysjonline.com", "bingli.iiyi.com", "article.iiyi.com", "tamanhhospital.vn", "vov.vn",
               "laodong.vn", "nhathuoclongchau.com.vn"]
MIN_PREFIX_URLS = 50      # path prefixes with fewer corpus urls are skipped when the domain listing is too big
MAX_DOMAIN_PAGES = 60     # list the whole domain when its CDX listing is at most this many pages
RATE = 0.25               # archive fetches per second to start with (~15/min); creeps up while unthrottled
MAX_RATE = 0.5
CONC = 2
THROTTLE_PAUSES = [600, 1200, 2400, 3600]   # global pause after 429 / refused connection, seconds
# Extra listings that are not corpus hosts themselves (old domain of zysjonline.com).
EXTRA_CDX = {"zysj.com.cn": [("zysj.com.cn", "domain")]}
# zysj.com.cn moved to zysjonline.com and the old domain served an unrelated site in 2026.
CAPTURE_CUTOFF = {"zysj.com.cn": "20260301"}
# crawl/wayback/alt_map*.tsv hold corpus_url \t archived_url \t method rows for corpus urls that live
# under a different archived url (built by zysj_map.py and zysj_books.py).
CDX_SLEEP = 6.0           # seconds between CDX page requests (a faster burst got the IP refused)
UA = "r2ai-research-crawler/0.1 (archive lookup for a non-commercial IR competition)"
WB = OUT / "wayback"

log = logging.getLogger("wayback")


def norm(url: str) -> str:
    p = urlparse(url.strip())
    host = p.hostname or ""
    host = host[4:] if host.startswith("www.") else host
    path = p.path.rstrip("/")
    return f"{host}{path}?{p.query}" if p.query else f"{host}{path}"


def corpus_for(hosts):
    df = pd.read_parquet(CORPUS)
    df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
    return df[df.host.isin(hosts)]


def cdx_queries(session, host, urls):
    """Whole domain if its listing is small, else the path prefixes holding most corpus urls."""
    base = dict(filter=["statuscode:200", "mimetype:text/html"], showNumPages="true")
    if int(cdx_get(session, dict(base, url=host, matchType="domain")).strip() or 0) <= MAX_DOMAIN_PAGES:
        return [(host, "domain")]
    seg = urls.map(lambda u: urlparse(u).path.split("/")[1] if urlparse(u).path.count("/") > 1 else "")
    counts = seg.value_counts()
    big = [s for s, n in counts.items() if s and n >= MIN_PREFIX_URLS]
    log.info("%s: domain listing too big, using %d prefixes covering %d/%d urls",
             host, len(big), int(counts[big].sum()), len(urls))
    return [(f"{host}/{s}/", "prefix") for s in big]


def cdx_get(session, params, tries=6):
    for i in range(tries):
        try:
            r = session.get("https://web.archive.org/cdx/search/cdx", params=params, timeout=180)
            if r.status_code == 200:
                return r.text
            log.warning("cdx %s -> HTTP %s", params.get("url"), r.status_code)
        except requests.ConnectionError as e:
            log.warning("cdx %s -> %s; archive.org refusing, waiting 10 min", params.get("url"), str(e)[:120])
            time.sleep(600)
            continue
        except requests.RequestException as e:
            log.warning("cdx %s -> %s", params.get("url"), e)
        time.sleep(min(300, 15 * 2 ** i))
    raise RuntimeError(f"CDX failed for {params}")


def step_cdx():
    df = corpus_for(TARGET_HOSTS)
    WB.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    s.headers["User-Agent"] = UA
    for host in TARGET_HOSTS + list(EXTRA_CDX):
        out = WB / f"{host}.cdx.tsv"
        if out.exists():
            log.info("%s: cdx already listed", host)
            continue
        queries = EXTRA_CDX.get(host) or cdx_queries(s, host, df[df.host == host].url)
        tmp, n = out.with_suffix(".tmp"), 0
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            for q, mt in queries:
                base = dict(url=q, matchType=mt, filter=["statuscode:200", "mimetype:text/html"])
                pages = int(cdx_get(s, dict(base, showNumPages="true")).strip() or 0)  # fl breaks the page count
                log.info("%s: %s (%s) -> %d cdx pages", host, q, mt, pages)
                for p in range(pages):
                    txt = cdx_get(s, dict(base, fl="timestamp,original", page=p))
                    rows = [line.split(" ", 1) for line in txt.splitlines() if " " in line]
                    w.writerows(rows)
                    f.flush()  # readable while the listing is still running
                    n += len(rows)
                    if p % 20 == 0:
                        log.info("%s: page %d/%d, %d captures so far", host, p, pages, n)
                    time.sleep(CDX_SLEEP)
        tmp.replace(out)
        log.info("%s: %d captures listed", host, n)


def load_matches():
    """id -> (host, url, [(timestamp, original), ...] newest first, at most 3)."""
    df = corpus_for(TARGET_HOSTS)
    want = {norm(u): (i, h, u) for i, h, u in zip(df.id, df.host, df.url)}
    by_url = {u: (i, h, u) for i, h, u in zip(df.id, df.host, df.url)}
    for alt_map in sorted(WB.glob("alt_map*.tsv")):
        with open(alt_map, encoding="utf-8") as fh:
            for corpus_url, archived_url, _method in csv.reader(fh, delimiter="\t"):
                if corpus_url in by_url:
                    want.setdefault(norm(archived_url), by_url[corpus_url])
    caps = defaultdict(list)
    for host in TARGET_HOSTS + list(EXTRA_CDX):
        f = WB / f"{host}.cdx.tsv"
        if not f.exists():
            continue
        cutoff = CAPTURE_CUTOFF.get(host)
        with open(f, encoding="utf-8") as fh:
            for ts, orig in csv.reader(fh, delimiter="\t"):
                if cutoff and ts >= cutoff:
                    continue
                k = norm(orig)
                if k in want:
                    caps[k].append((ts, orig))
    out = {}
    for k, lst in caps.items():
        i, h, u = want[k]
        uniq = sorted(set(lst), reverse=True)
        out[i] = (h, u, uniq[:3])
    per_host = defaultdict(int)
    for h, *_ in out.values():
        per_host[h] += 1
    for h in TARGET_HOSTS:
        log.info("%-40s corpus=%7d archived=%7d", h, int((df.host == h).sum()), per_host[h])
    return out


START_DELAY = 0


async def step_fetch():
    matches = load_matches()
    stores = {h: HostStore(h, subdir="db_wayback") for h in TARGET_HOSTS}
    done = set()
    for st in stores.values():
        d, _ = st.state()
        done |= d
    todo = [(i, *v) for i, v in matches.items() if i not in done]
    log.info("%d archived pages to fetch (%d already stored)", len(todo), len(matches) - len(todo))
    # Hosts Common Crawl barely covers go first; the Vietnamese sites are mostly served faster by cc.py.
    todo.sort(key=lambda t: FETCH_ORDER.index(t[1]) if t[1] in FETCH_ORDER else len(FETCH_ORDER))
    todo.reverse()

    state = dict(next_slot=0.0, paused_until=time.monotonic() + START_DELAY, rate=RATE, level=0,
                 ok_streak=0, n=0, t0=time.time())

    class Throttled(Exception):
        pass

    async def turn():
        while True:
            now = time.monotonic()
            if now < state["paused_until"]:
                await asyncio.sleep(state["paused_until"] - now)
                continue
            if state["next_slot"] <= now:
                state["next_slot"] = now + 1.0 / state["rate"]
                return
            await asyncio.sleep(state["next_slot"] - now)

    def throttled(reason):
        now = time.monotonic()
        if now < state["paused_until"]:
            return  # another in-flight request already triggered the pause
        pause = THROTTLE_PAUSES[min(state["level"], len(THROTTLE_PAUSES) - 1)]
        state["level"] += 1
        state["ok_streak"] = 0
        state["rate"] = max(0.1, state["rate"] / 2)
        state["paused_until"] = now + pause
        log.warning("archive.org throttling (%s): pausing %ds, rate now %.2f/s", reason, pause, state["rate"])

    async def get(session, url):
        """Returns (status, final_url, ctype, body, headers); retries forever through throttling."""
        while True:
            await turn()
            try:
                async with session.get(url, allow_redirects=True) as r:
                    if r.status in (429, 503, 509):
                        raise Throttled(f"HTTP {r.status}")
                    body = await r.read()
                state["level"] = 0
                state["ok_streak"] += 1
                if state["ok_streak"] >= 300 and state["rate"] < MAX_RATE:
                    state["ok_streak"] = 0
                    state["rate"] = min(MAX_RATE, state["rate"] * 1.1)
                return r.status, str(r.url), r.headers.get("Content-Type", ""), body, r.headers
            except Throttled as e:
                throttled(str(e))
            except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError, ConnectionResetError) as e:
                throttled(type(e).__name__)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.info("archive fetch error %s on %s, will retry", type(e).__name__, url)
                await asyncio.sleep(30)

    async def worker(session):
        while todo:
            doc_id, host, url, caps = todo.pop()
            if stored_ok(host, doc_id, ("db", "db_alt", "db_cc")):   # saved meanwhile by another source
                continue
            st, row = stores[host], None
            for ts, orig in caps:
                status, final, ctype, body, headers = await get(session, f"https://web.archive.org/web/{ts}id_/{orig}")
                if status == 200 and classify(200, final, body, headers) == "ok" and len(body) > 2000:
                    row = (doc_id, url, final, 200, "ok", ctype, len(body), time.time(), 1, f"wayback:{ts}",
                           st.cctx.compress(STRIP_RE.sub(b"", body)))
                    break
            if row is None:
                row = (doc_id, url, None, None, "error", None, 0, time.time(), 1, "wayback: no usable capture", None)
            st.add(row)
            await st.maybe_flush()
            state["n"] += 1
            if state["n"] % 100 == 0:
                log.info("fetched %d / %d (%.3f/s overall, rate %.2f/s)", state["n"], len(todo) + state["n"],
                         state["n"] / (time.time() - state["t0"]), state["rate"])

    timeout = aiohttp.ClientTimeout(total=120, sock_connect=30)
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())  # system store lacks archive.org's root
    async with aiohttp.ClientSession(headers={"User-Agent": UA}, timeout=timeout,
                                     connector=aiohttp.TCPConnector(ssl=ssl_ctx)) as session:
        await asyncio.gather(*[worker(session) for _ in range(CONC)])
    for st in stores.values():
        await st.maybe_flush(force=True)
    log.info("wayback fetch finished")


if __name__ == "__main__":
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / "wayback.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    START_DELAY = int(sys.argv[2]) if len(sys.argv) > 2 else 0   # seconds to wait before the first fetch
    if cmd in ("cdx", "all"):
        step_cdx()
    if cmd in ("fetch", "all"):
        asyncio.run(step_fetch())
