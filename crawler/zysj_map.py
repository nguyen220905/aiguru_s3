"""Map zysjonline.com corpus URLs (Cloudflare-blocked, never archived) to archived pages of the site's
previous domain zysj.com.cn, writing crawl/wayback/alt_map.tsv for wayback.py.

- herbs/<slug>/     -> zhongyaocai/yaocai_<first letter>/<slug>.html   (same slug, direct)
- formulas/<slug>/  -> zhongyaofang/yaofang_<first letter>/<slug>.html (same slug, direct)
- articles/misc/<id>/ -> zaji/<category>/<id>.html (ids were kept: 98.7% of archived zaji ids are corpus
                         article ids, and both id ranges end at 81153)
Books are NOT mapped: their old numeric ids are shifted relative to the new ones and the old
book-volume-chapter paths would need a guessed chapter alignment.
"""
import csv
import re
from collections import defaultdict
from urllib.parse import urlparse

import pandas as pd

from crawl import CORPUS, OUT

WB = OUT / "wayback"
CUTOFF = "20260301"


def old_paths():
    """Archived zysj.com.cn paths (no host, no query) captured before the domain changed hands."""
    paths = defaultdict(str)
    with open(WB / "zysj.com.cn.cdx.tsv", encoding="utf-8") as fh:
        for ts, orig in csv.reader(fh, delimiter="\t"):
            if ts >= CUTOFF:
                continue
            p = re.sub(r"^https?://(www\.)?zysj\.com\.cn(:80)?", "", orig).split("?")[0]
            paths[p] = max(paths[p], ts)
    return paths


def direct_map(parts, zaji):
    kind, slug = parts[0], parts[1] if len(parts) > 1 else ""
    if kind == "articles" and len(parts) > 2 and parts[2].isdigit():
        return zaji.get(int(parts[2]))
    if kind == "herbs" and slug:
        return f"/zhongyaocai/yaocai_{slug[0]}/{slug}.html"
    if kind == "formulas" and slug:
        return f"/zhongyaofang/yaofang_{slug[0]}/{slug}.html"
    return None


def main():
    df = pd.read_parquet(CORPUS)
    z = df[df.url.str.contains("://zysjonline.com/", regex=False)]
    archived = old_paths()
    zaji = {int(m.group(1)): p for p in archived for m in [re.match(r"^/zaji/\d+/(\d+)\.html$", p)] if m}
    rows, stats = [], defaultdict(lambda: [0, 0])
    for url in z.url:
        parts = urlparse(url).path.strip("/").split("/")
        stats[parts[0]][0] += 1
        old = direct_map(parts, zaji)
        if old and old in archived:
            rows.append((url, "http://www.zysj.com.cn" + old, "id" if parts[0] == "articles" else "slug"))
            stats[parts[0]][1] += 1
    with open(WB / "alt_map.tsv", "w", encoding="utf-8", newline="") as f:
        csv.writer(f, delimiter="\t").writerows(rows)
    for k, (n, m) in sorted(stats.items(), key=lambda kv: -kv[1][0]):
        print(f"{k:10} corpus={n:7d} mapped={m:7d}")
    print("alt_map rows:", len(rows))


if __name__ == "__main__":
    main()
