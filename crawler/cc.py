"""Second archive source: Common Crawl, for corpus pages that are neither fetchable (Cloudflare / origin
down) nor stored yet from the Wayback Machine.

The public index server (index.commoncrawl.org) times out on most queries, so the index is read directly
from data.commoncrawl.org: binary-search each crawl's cluster.idx with HTTP range requests for a host's SURT
range, keep the cdx blocks that overlap the wanted path prefixes, and read only those gzip blocks.
data.commoncrawl.org answers 403 when a client goes too fast, so every request goes through one global
limiter and any 403/429/503 pauses all threads.

Step 1 (index): crawl/cc/index/<collection>/<host>.jsonl    (url, ts, filename, offset, length)
Step 2 (fetch): WARC records for corpus urls still missing -> crawl/db_cc/<corpus host>.sqlite
Usage: python cc.py [index|fetch|all]
"""
import csv
import gzip
import json
import logging
import re
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import pandas as pd
import requests

from crawl import CORPUS, OUT, STRIP_RE, HostStore, classify, is_home
from wayback import norm

DATA = "https://data.commoncrawl.org"
UA = "r2ai-research-crawler/0.1 (non-commercial IR competition)"
CCDIR = OUT / "cc"
WB = OUT / "wayback"
WORKERS = 3
RATE = 2.0                      # requests per second, all threads together
BLOCK_PAUSES = [900, 1800, 3600]
MIN_PREFIX_URLS = 50

log = logging.getLogger("cc")
_local = threading.local()


class Limiter:
    def __init__(self, rate):
        self.interval = 1.0 / rate
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.paused_until = 0.0
        self.level = 0

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                if now >= self.paused_until and now >= self.next_slot:
                    self.next_slot = now + self.interval
                    return
                delay = max(self.paused_until, self.next_slot) - now
            time.sleep(min(delay, 5))

    def blocked(self, why):
        with self.lock:
            now = time.monotonic()
            if now < self.paused_until:
                return
            pause = BLOCK_PAUSES[min(self.level, len(BLOCK_PAUSES) - 1)]
            self.level += 1
            self.paused_until = now + pause
            log.warning("data.commoncrawl.org pushed back (%s): pausing all requests %ds", why, pause)

    def ok(self):
        self.level = 0


LIMIT = Limiter(RATE)


def session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers["User-Agent"] = UA
    return _local.s


def get_range(url, a, b):
    """Bytes a..b (inclusive) of a data.commoncrawl.org object; retries through throttling."""
    for attempt in range(30):
        LIMIT.wait()
        try:
            r = session().get(url, headers={"Range": f"bytes={a}-{b}"}, timeout=120)
            if r.status_code in (200, 206):
                LIMIT.ok()
                return r.content
            if r.status_code == 416:
                return b""
            if r.status_code in (403, 429, 503):
                LIMIT.blocked(f"HTTP {r.status_code}")
                continue
            log.warning("%s %s-%s -> HTTP %s", url.rsplit("/", 1)[-1], a, b, r.status_code)
        except requests.RequestException as e:
            log.warning("%s -> %s", url.rsplit("/", 1)[-1], str(e)[:100])
        time.sleep(min(120, 5 * 2 ** min(attempt, 5)))
    raise RuntimeError(f"range request failed: {url} {a}-{b}")


_sizes = {}


def idx_size(coll):
    if coll not in _sizes:
        LIMIT.wait()
        url = f"{DATA}/cc-index/collections/{coll}/indexes/cluster.idx"
        r = session().head(url, timeout=60)
        if r.status_code in (403, 429, 503):
            LIMIT.blocked(f"HTTP {r.status_code}")
            return idx_size(coll)
        _sizes[coll] = int(r.headers["Content-Length"])
    return _sizes[coll]


def surt(host, path=""):
    h = host[4:] if host.startswith("www.") else host
    return ",".join(reversed(h.split("."))) + ")/" + path


def targets():
    """host -> (SURT host root, [SURT path prefixes])."""
    df = pd.read_parquet(CORPUS, columns=["url"])
    df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
    paths = {
        "zysj.com.cn": ["zaji/", "lilunshuji/"],
        "zysjonline.com": [""],
        "bingli.iiyi.com": ["show/"],
        "article.iiyi.com": ["detail/"],
        "pmc-ecm-healthblog.beta.pharmacity.io": [""],
    }
    for host in ("nhathuoclongchau.com.vn", "laodong.vn", "vov.vn", "tamanhhospital.vn"):
        seg = df[df.host == host].url.map(lambda u: urlparse(u).path.split("/")[1])
        paths[host] = [s + "/" for s, n in seg.value_counts().items() if s and n >= MIN_PREFIX_URLS]
    return {h: (surt(h), sorted(surt(h, p) for p in ps)) for h, ps in paths.items()}


def collections():
    cols = session().get("https://index.commoncrawl.org/collinfo.json", timeout=120).json()
    return [c["id"] for c in cols if re.fullmatch(r"CC-MAIN-\d{4}-\d{2}", c["id"])]


def cluster_lines(coll, start, end):
    """[(key, (cdx file, offset, length))] for cluster.idx entries whose blocks can hold keys in
    [start, end): the entry before the first key >= start, then every entry with key < end."""
    idx = f"{DATA}/cc-index/collections/{coll}/indexes/cluster.idx"
    size = idx_size(coll)
    lo, hi = 0, size
    while hi - lo > 1 << 16:
        mid = (lo + hi) // 2
        chunk = get_range(idx, mid, mid + 16383)
        nl = chunk.find(b"\n")
        line = chunk[nl + 1:].split(b"\n", 1)[0] if nl >= 0 else b""
        if not line:
            hi = mid
        elif line.split(b" ", 1)[0] < start:
            lo = mid + nl + 1
        else:
            hi = mid
    out, prev, pos = [], None, lo
    while pos < size:
        chunk = get_range(idx, pos, min(size, pos + (1 << 17)) - 1)
        if not chunk:
            break
        body = chunk if pos + len(chunk) >= size else chunk[: chunk.rfind(b"\n") + 1]
        for line in body.split(b"\n"):
            if not line:
                continue
            key = line.split(b" ", 1)[0]
            f = line.split(b"\t")
            entry = (key, (f[1].decode(), int(f[2]), int(f[3])))
            if key < start:
                prev = entry
            elif key < end:
                if prev and not out:
                    out.append(prev)
                out.append(entry)
            else:
                return (out or ([prev] if prev else [])), key
        pos += len(body)
    return (out or ([prev] if prev else [])), b"\xff"


def blocks_for(coll, root, prefixes):
    """cdx blocks of this host that overlap any wanted path prefix."""
    lines, after = cluster_lines(coll, root.encode(), root.encode() + b"\xff")
    bounds = [(p.encode(), p.encode() + b"\xff") for p in prefixes]
    keep = []
    for i, (key, block) in enumerate(lines):
        nxt = lines[i + 1][0] if i + 1 < len(lines) else after
        if any(key < pe and nxt > ps for ps, pe in bounds):
            keep.append(block)
    return keep


def lookup(coll, host, root, prefixes):
    recs = []
    for cdx_file, off, length in blocks_for(coll, root, prefixes):
        raw = get_range(f"{DATA}/cc-index/collections/{coll}/indexes/{cdx_file}", off, off + length - 1)
        for line in gzip.decompress(raw).decode("utf-8", "replace").splitlines():
            key, ts, js = line.split(" ", 2)
            if not any(key.startswith(p) for p in prefixes):
                continue
            rec = json.loads(js)
            if rec.get("status") != "200" or "html" not in (rec.get("mime-detected") or rec.get("mime") or ""):
                continue
            recs.append(dict(url=rec["url"], ts=ts, filename=rec["filename"], offset=int(rec["offset"]),
                             length=int(rec["length"]), host=host))
    return recs


def step_index():
    tg = targets()
    cols = collections()
    tasks = [(c, h) for c in cols for h in tg]
    log.info("%d collections x %d hosts = %d lookups", len(cols), len(tg), len(tasks))
    found, done = 0, 0

    def run(c, h):
        d = CCDIR / "index" / c
        d.mkdir(parents=True, exist_ok=True)
        f = d / f"{h}.jsonl"
        if f.exists():
            return sum(1 for _ in open(f, encoding="utf-8"))
        recs = lookup(c, h, *tg[h])
        tmp = f.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        tmp.replace(f)
        return len(recs)

    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(run, *t): t for t in tasks}
        for fut in as_completed(futs):
            done += 1
            try:
                found += fut.result()
            except Exception as e:
                log.warning("lookup failed %s: %s", futs[fut], e)
            if done % 50 == 0:
                log.info("lookups %d / %d, %d html captures so far", done, len(tasks), found)
    log.info("index step finished: %d captures", found)


# ---------------------------------------------------------------- fetch

def stored_ok(subdir, host):
    f = OUT / subdir / f"{host}.sqlite"
    if not f.exists():
        return set()
    with sqlite3.connect(f"file:{f}?mode=ro", uri=True, timeout=60) as c:
        return {r[0] for r in c.execute("SELECT id FROM pages WHERE kind IN ('ok','notfound','home')")}


def wanted():
    """norm(captured url) -> (corpus id, corpus host, corpus url) for corpus pages not stored anywhere yet."""
    df = pd.read_parquet(CORPUS)
    df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
    hosts = [h for h in targets() if h != "zysj.com.cn"]
    df = df[df.host.isin(hosts)]
    have = set()
    for h in hosts:
        for sub in ("db", "db_wayback", "db_alt", "db_cc"):
            have |= stored_ok(sub, h)
    df = df[~df.id.isin(have)]
    want = {norm(u): (i, h, u) for i, h, u in zip(df.id, df.host, df.url)}
    # zysjonline lives on as zysj.com.cn: herbs/formulas by slug, articles by id, books via the index alignment
    z = {u: (i, h, u) for i, h, u in zip(df.id, df.host, df.url) if h == "zysjonline.com"}
    for f in ("alt_map.tsv", "zysj_books_full.tsv"):
        p = WB / f
        if p.exists():
            with open(p, encoding="utf-8") as fh:
                for corpus_url, old_url, _ in csv.reader(fh, delimiter="\t"):
                    if corpus_url in z:
                        want.setdefault(norm(old_url), z[corpus_url])
    for u, v in z.items():
        m = re.search(r"/articles/misc/(\d+)/", u)
        if m:
            want.setdefault(f"zaji-id:{m.group(1)}", v)
    log.info("%d corpus pages still missing on the target hosts", len(set(want.values())))
    return want


def capture_key(url):
    m = re.search(r"zysj\.com\.cn(?::80)?/zaji/\d+/(\d+)\.html", url)
    return f"zaji-id:{m.group(1)}" if m else norm(url)


def parse_warc(raw):
    data = gzip.decompress(raw)
    _, _, rest = data.partition(b"\r\n\r\n")          # WARC headers
    http_head, _, body = rest.partition(b"\r\n\r\n")  # HTTP headers (payload is stored decoded)
    ctype = ""
    for h in http_head.split(b"\r\n")[1:]:
        k, _, v = h.partition(b":")
        if k.strip().lower() == b"content-type":
            ctype = v.strip().decode("latin-1")
    return ctype, body


def step_fetch():
    want = wanted()
    caps = defaultdict(list)
    for f in (CCDIR / "index").glob("*/*.jsonl"):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                r = json.loads(line)
                k = capture_key(r["url"])
                if k in want:
                    caps[want[k]].append(r)
    todo = sorted(caps.items(), key=lambda kv: kv[0][0])
    log.info("%d missing corpus pages have Common Crawl captures", len(todo))
    stores = {}
    lock = threading.Lock()
    counter = dict(n=0, ok=0)

    def run(item):
        (doc_id, host, url), recs = item
        recs = sorted(recs, key=lambda r: r["ts"], reverse=True)[:3]
        row = None
        for r in recs:
            raw = get_range(f"{DATA}/{r['filename']}", r["offset"], r["offset"] + r["length"] - 1)
            try:
                ctype, body = parse_warc(raw)
            except Exception:
                continue
            kind = classify(200, r["url"], body, {})
            if kind == "ok" and len(body) > 2000 and not is_home(url, r["url"]):
                row = (doc_id, url, r["url"], 200, "ok", ctype, len(body), time.time(), 1, f"cc:{r['ts']}",
                       None)
                break
        with lock:
            st = stores.setdefault(host, HostStore(host, subdir="db_cc"))
            if row:
                row = row[:-1] + (st.cctx.compress(STRIP_RE.sub(b"", body)),)
                counter["ok"] += 1
            else:
                row = (doc_id, url, None, None, "error", None, 0, time.time(), 1, "cc: no usable capture", None)
            st.buf.append(row)
            counter["n"] += 1
            if len(st.buf) >= 100:
                st._write(st.buf)
                st.buf = []
            if counter["n"] % 500 == 0:
                log.info("cc fetched %d / %d (%d ok)", counter["n"], len(todo), counter["ok"])

    with ThreadPoolExecutor(WORKERS) as ex:
        for fut in as_completed([ex.submit(run, it) for it in todo]):
            try:
                fut.result()
            except Exception as e:
                log.warning("fetch failed: %s", e)
    for st in stores.values():
        if st.buf:
            st._write(st.buf)
            st.buf = []
    log.info("cc fetch finished: %d / %d ok", counter["ok"], counter["n"])


if __name__ == "__main__":
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / "cc.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd in ("index", "all"):
        step_index()
    if cmd in ("fetch", "all"):
        step_fetch()
