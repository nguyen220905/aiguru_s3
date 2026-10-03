"""Crawler specifically targeting recoverable URLs from missing_pages.csv.

Sites that were previously skipped due to robots.txt or Cloudflare / JS challenges
are fetched directly using curl_cffi with browser impersonation and cookie bypass.
Results are saved to crawl/db/<host>.sqlite adhering to the project's standard schema.

Usage:
    python crawl_missing.py
    python crawl_missing.py --hosts nhathuoclongchau.com.vn,www.baidu.com
    python crawl_missing.py --limit-per-host 20
    python crawl_missing.py --passes 5 --gap-minutes 10
"""
import argparse
import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from curl_cffi.requests import AsyncSession

# Import project utilities
from crawl import OUT, ROOT, STRIP_RE, HostStore, classify, is_home

MISSING_CSV = ROOT / "missing_pages.csv"
LOG_DIR = OUT / "logs"

log = logging.getLogger("crawl_missing")

HOST_CONFIGS = {
    "tamanhhospital.vn": {
        "concurrency": 6,
        "delay": 0.0,
        "impersonate": "chrome124",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        },
    },
    "nhathuoclongchau.com.vn": {
        "concurrency": 2,
        "delay": 1.5,
        "impersonate": "chrome124",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
        },
    },
    "vov.vn": {
        "concurrency": 8,
        "delay": 0.0,
        "impersonate": "chrome124",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        },
    },
    "baolangson.vn": {
        "concurrency": 8,
        "delay": 0.0,
        "impersonate": "chrome124",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        },
    },
    "www.baidu.com": {
        "concurrency": 1,
        "delay": 2.0,
        "impersonate": "chrome124",
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.baidu.com/",
        },
    },
    "laodong.vn": {
        "concurrency": 6,
        "delay": 0.0,
        "impersonate": "chrome124",
        "init_cookie": True,
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        },
    },
}


async def init_session(cfg: dict, host: str) -> AsyncSession:
    session = AsyncSession(impersonate=cfg.get("impersonate", "chrome124"), headers=cfg.get("headers", {}))
    if cfg.get("init_cookie") and host == "laodong.vn":
        try:
            r0 = await session.get("https://laodong.vn", timeout=15)
            m = re.search(r'document\.cookie="([^=]+)=([^";]+)', r0.text)
            if m:
                name, val = m.group(1), m.group(2)
                session.cookies.set(name, val, domain="laodong.vn")
                log.info("[%s] Injected bypass cookie %s=%s", host, name, val)
        except Exception as e:
            log.warning("[%s] Failed to init cookie: %s", host, e)
    return session


async def fetch_one(session: AsyncSession, sem: asyncio.Semaphore, doc_id: int, url: str, store: HostStore, delay: float = 0.0):
    async with sem:
        if delay > 0:
            await asyncio.sleep(delay)
        for attempt in range(1, 4):
            try:
                r = await session.get(url, timeout=20, allow_redirects=True)
                final_url = str(r.url)
                body = r.content
                ctype = r.headers.get("Content-Type") or ""
                kind = classify(r.status_code, final_url, body, r.headers)
                if kind == "ok" and is_home(url, final_url):
                    kind = "home"

                html = None
                if kind == "ok":
                    cleaned = STRIP_RE.sub(b"", body)
                    html = store.cctx.compress(cleaned)

                store.add((doc_id, url, final_url, r.status_code, kind, ctype, len(body), time.time(), attempt, None, html))
                return kind
            except Exception as e:
                if attempt == 3:
                    store.add((doc_id, url, url, -1, "error", None, 0, time.time(), attempt, str(e)[:300], None))
                    return "error"
                await asyncio.sleep(1.0 * attempt)


async def crawl_host(host: str, urls_df: pd.DataFrame, limit: int = 0):
    cfg = HOST_CONFIGS[host]
    store = HostStore(host, subdir="db")
    done_ids = {r[0] for r in store.conn.execute("SELECT id FROM pages WHERE kind = 'ok' AND html IS NOT NULL")}

    todo = [(row.id, row.url) for _, row in urls_df.iterrows() if row.id not in done_ids]
    if limit > 0:
        todo = todo[:limit]

    log.info("[%s] Total corpus: %d | Already stored: %d | Remaining to crawl: %d", host, len(urls_df), len(done_ids), len(todo))
    if not todo:
        log.info("[%s] Nothing to crawl!", host)
        return

    sem = asyncio.Semaphore(cfg["concurrency"])
    delay = cfg.get("delay", 0.0)
    session = await init_session(cfg, host)

    t_start = time.time()
    n_done = 0
    consecutive_blocks = 0
    stats = {}

    batch_size = 30 if delay > 0 else 50
    for i in range(0, len(todo), batch_size):
        chunk = todo[i : i + batch_size]
        tasks = [fetch_one(session, sem, doc_id, url, store, delay=delay) for doc_id, url in chunk]
        results = await asyncio.gather(*tasks)

        for res in results:
            stats[res] = stats.get(res, 0) + 1
            if res in ("blocked", "captcha"):
                consecutive_blocks += 1
            else:
                consecutive_blocks = 0

        n_done += len(results)
        await store.maybe_flush()

        elapsed = time.time() - t_start
        speed = n_done / elapsed if elapsed > 0 else 0
        log.info("[%s] Progress: %d / %d (%.1f%%) | Speed: %.1f req/s | Stats: %s",
                 host, n_done, len(todo), (n_done / len(todo)) * 100, speed, stats)

        # If origin started blocking / throttling heavily, pause and skip to avoid wasting requests
        if consecutive_blocks >= 10:
            log.warning("[%s] Hit 10 consecutive blocks/captchas. Pausing host for this pass.", host)
            break

    await store.maybe_flush(force=True)
    await session.close()
    log.info("[%s] FINISHED PASS: %d pages processed in %.1f seconds", host, n_done, time.time() - t_start)


async def run_all_hosts(target_hosts, df, limit: int = 0):
    for host in target_hosts:
        sub_df = df[df.host == host]
        await crawl_host(host, sub_df, limit=limit)


async def main():
    parser = argparse.ArgumentParser(description="Crawl recoverable missing pages")
    parser.add_argument("--hosts", type=str, default="", help="Comma-separated hosts to crawl")
    parser.add_argument("--limit-per-host", type=int, default=0, help="Max pages per host (for testing)")
    parser.add_argument("--passes", type=int, default=1, help="Number of retry passes")
    parser.add_argument("--gap-minutes", type=int, default=10, help="Minutes between passes")
    args = parser.parse_args()

    if not MISSING_CSV.exists():
        log.error("Missing file %s", MISSING_CSV)
        return

    df = pd.read_csv(MISSING_CSV)
    available_hosts = [h for h in HOST_CONFIGS if h in set(df.host) or h == "www.baidu.com"]

    if args.hosts:
        chosen = [h.strip() for h in args.hosts.split(",") if h.strip()]
        target_hosts = [h for h in chosen if h in HOST_CONFIGS]
    else:
        target_hosts = available_hosts

    if "www.baidu.com" in target_hosts and "www.baidu.com" not in set(df.host):
        from crawl import CORPUS
        c_df = pd.read_parquet(CORPUS)
        b_df = c_df[c_df.url.str.contains("://www.baidu.com/", regex=False)].copy()
        b_df["host"] = "www.baidu.com"
        df = pd.concat([df, b_df], ignore_index=True)

    log.info("Target hosts to crawl: %s across %d passes", target_hosts, args.passes)

    for p in range(1, args.passes + 1):
        log.info("=== STARTING PASS %d / %d ===", p, args.passes)
        await run_all_hosts(target_hosts, df, limit=args.limit_per_host)
        if p < args.passes:
            log.info("Pass %d complete. Sleeping %d minutes before next pass...", p, args.gap_minutes)
            await asyncio.sleep(args.gap_minutes * 60)


if __name__ == "__main__":
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "crawl_missing.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
