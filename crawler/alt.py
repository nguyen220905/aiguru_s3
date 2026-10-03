"""Fetch pages of unreachable hosts from an alternate public location that serves the same article.

pmc-ecm-healthblog.beta.pharmacity.io is a staging copy of the Pharmacity health blog (Cloudflare-blocked);
the same slugs are published on www.pharmacity.vn/<slug>.htm, whose robots.txt allows crawling with a
20 s crawl-delay. Results go to crawl/db_alt/<original host>.sqlite (final_url = the alternate URL).

Usage: python alt.py
"""
import logging
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests

from crawl import CORPUS, HEADERS, OUT, STRIP_RE, HostStore, classify

SOURCES = {
    "pmc-ecm-healthblog.beta.pharmacity.io": dict(
        alt_host="www.pharmacity.vn",
        map=lambda u: "https://www.pharmacity.vn/" + urlparse(u).path.strip("/") + ".htm",
    ),
}

log = logging.getLogger("alt")


def crawl_delay(host, session):
    rp = RobotFileParser()
    r = session.get(f"https://{host}/robots.txt", timeout=30)
    rp.parse(r.text.splitlines())
    return float(rp.crawl_delay("*") or 1.0), rp


def main():
    df = pd.read_parquet(CORPUS)
    s = requests.Session()
    s.headers.update(HEADERS)
    for host, cfg in SOURCES.items():
        urls = df[df.url.map(lambda u: urlparse(u).netloc.lower()) == host]
        store = HostStore(host, subdir="db_alt")
        done, _ = store.state()
        todo = [(i, u) for i, u in zip(urls.id, urls.url) if i not in done]
        delay, rp = crawl_delay(cfg["alt_host"], s)
        log.info("%s: %d to fetch from %s (crawl-delay %.0fs)", host, len(todo), cfg["alt_host"], delay)
        for n, (doc_id, url) in enumerate(todo, 1):
            alt = cfg["map"](url)
            if not rp.can_fetch("*", alt):
                store.add((doc_id, url, alt, None, "robots", None, 0, time.time(), 1, "alt", None))
                continue
            t0 = time.time()
            try:
                r = s.get(alt, timeout=60)
                kind = classify(r.status_code, r.url, r.content, r.headers)
                if kind == "ok" and urlparse(r.url).path in ("", "/"):
                    kind = "home"
                html = store.cctx.compress(STRIP_RE.sub(b"", r.content)) if kind == "ok" else None
                store.add((doc_id, url, r.url, r.status_code, kind, r.headers.get("Content-Type"), len(r.content),
                           time.time(), 1, f"alt:{cfg['alt_host']}", html))
                if kind in ("blocked", "captcha"):
                    log.warning("%s answered %s, backing off 10 min", cfg["alt_host"], kind)
                    time.sleep(600)
            except requests.RequestException as e:
                store.add((doc_id, url, alt, None, "error", None, 0, time.time(), 1, f"alt: {e}"[:300], None))
            if n % 20 == 0 or n == len(todo):
                store._write(store.buf)
                store.buf = []
                log.info("%s: %d / %d", host, n, len(todo))
            time.sleep(max(0.0, delay - (time.time() - t0)))
        if store.buf:
            store._write(store.buf)
            store.buf = []


if __name__ == "__main__":
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / "alt.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    main()
