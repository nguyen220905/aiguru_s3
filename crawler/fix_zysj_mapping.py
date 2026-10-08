"""Invalidate archived zysjonline book/column pages whose corpus URL is no longer mapped to the same
old zysj.com.cn page (e.g. a book later found ambiguous). Rows become kind='unmapped', body dropped.

Usage: python fix_zysj_mapping.py [--dry-run]
"""
import csv
import re
import sqlite3
import sys

from crawl import OUT
from wayback import norm

WB = OUT / "wayback"
mapping = {}
with open(WB / "zysj_books_full.tsv", encoding="utf-8") as fh:
    for corpus_url, old_url, _ in csv.reader(fh, delimiter="\t"):
        mapping[corpus_url] = norm(old_url)

dry = "--dry-run" in sys.argv
for sub in ("db_wayback", "db_cc"):
    f = OUT / sub / "zysjonline.com.sqlite"
    if not f.exists():
        continue
    with sqlite3.connect(f, timeout=120) as c:
        bad = []
        for i, url, final in c.execute("SELECT id, url, final_url FROM pages WHERE kind='ok'"):
            if not re.search(r"/(books|columns)/", url):
                continue
            old = final.split("id_/", 1)[1] if final and "id_/" in final else final
            if mapping.get(url) != norm(old or ""):
                bad.append(i)
        print(f"{sub}: {len(bad)} pages no longer backed by the alignment")
        if bad and not dry:
            c.executemany("UPDATE pages SET kind='unmapped', html=NULL WHERE id=?", [(i,) for i in bad])
