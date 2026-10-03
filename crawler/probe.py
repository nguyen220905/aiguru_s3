"""Fetch a few sample URLs per host to see which sites respond, block, or need special handling."""
import asyncio
import re
import sys
import time
from urllib.parse import urlparse

import aiohttp
import pandas as pd

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,zh-CN;q=0.8,zh;q=0.7,en;q=0.6",
}
N_PER_HOST = int(sys.argv[1]) if len(sys.argv) > 1 else 3


async def fetch(session, sem, host, url):
    async with sem:
        t = time.time()
        try:
            async with session.get(url, allow_redirects=True) as r:
                body = await r.read()
                head = body[:200000].decode(r.charset or "utf-8", errors="ignore")
                m = re.search(r"<title[^>]*>(.*?)</title>", head, re.S | re.I)
                title = m.group(1).strip()[:60] if m else ""
                return dict(host=host, url=url, status=r.status, final=str(r.url)[:100],
                            ctype=r.headers.get("Content-Type", ""), size=len(body),
                            secs=round(time.time() - t, 2), title=title, err="")
        except Exception as e:
            return dict(host=host, url=url, status=-1, final="", ctype="", size=0,
                        secs=round(time.time() - t, 2), title="", err=f"{type(e).__name__}: {e}"[:120])


async def main():
    df = pd.read_parquet("D:/project_r2ai/data/links_corpus.parquet")
    df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
    sample = pd.concat(g.sample(min(len(g), N_PER_HOST), random_state=1) for _, g in df.groupby("host"))
    sem = asyncio.Semaphore(64)
    timeout = aiohttp.ClientTimeout(total=40)
    conn = aiohttp.TCPConnector(limit=64, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=conn) as s:
        res = await asyncio.gather(*[fetch(s, sem, h, u) for h, u in zip(sample.host, sample.url)])
    out = pd.DataFrame(res).sort_values(["host", "url"])
    out.to_csv("D:/project_r2ai/crawler/probe_results.csv", index=False, encoding="utf-8")
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 60)
    print(out[["host", "status", "size", "secs", "title", "err"]].to_string())


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
