"""Print crawl progress: per-host counts from the SQLite stores plus live group state."""
import glob
import json
import sqlite3
from pathlib import Path

import pandas as pd

OUT = Path("D:/project_r2ai/crawl")
TOTALS = pd.read_parquet("D:/project_r2ai/data/links_corpus.parquet", columns=["url"]).url \
    .str.extract(r"^https?://([^/]+)")[0].str.lower().value_counts()

rows = []
for f in glob.glob(str(OUT / "db" / "*.sqlite")):
    host = Path(f).stem
    c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
    counts = dict(c.execute("SELECT kind, COUNT(*) FROM pages GROUP BY kind"))
    size = sum(Path(p).stat().st_size for p in glob.glob(f + "*"))
    rows.append(dict(host=host, total=int(TOTALS.get(host, 0)), **counts, mb=round(size / 2**20)))
df = pd.DataFrame(rows).fillna(0).set_index("host")
for k in ("ok", "notfound", "home", "robots", "http4xx", "error", "blocked", "captcha"):
    if k not in df:
        df[k] = 0
df["done%"] = (100 * (df.ok + df.notfound + df.home + df.robots + df.http4xx) / df.total).round(1)
cols = ["total", "ok", "notfound", "home", "robots", "http4xx", "error", "blocked", "captcha", "done%", "mb"]
df = df[cols].astype({c: int for c in cols if c != "done%"}).sort_values("total", ascending=False)
pd.set_option("display.width", 200)
print(df.to_string())
ok, total = int(df.ok.sum()), int(TOTALS.sum())
print(f"\nOK pages: {ok:,} / {total:,} ({100 * ok / total:.1f}%), stored {df.mb.sum() / 1024:.1f} GB")

for status in sorted(OUT.glob("status*.json")):
    st = json.loads(status.read_text(encoding="utf-8"))
    print(f"\nstatus.json updated {st['updated']} (run elapsed {st['elapsed_min']} min)")
    for name, g in sorted(st["groups"].items(), key=lambda kv: -kv[1]["queue"]):
        if g["queue"] or g["disabled"] or g["paused_for"]:
            print(f"  {name:28} queue={g['queue']:>8,} rate={g.get('rate', '?'):>5} req/s "
                  f"paused={g['paused_for']}s strikes={g['strikes']} disabled={g['disabled']} dead={g['dead_hosts']}")
