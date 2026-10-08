"""One-off cleanup: pages stored as 'ok' whose content is not HTML (images/PDFs a deleted article
redirected to) are re-marked kind='nonhtml' and their body dropped. Safe to re-run.

Usage: python fix_nonhtml.py [--dry-run]
"""
import io
import sqlite3
import sys

import zstandard

from crawl import OUT, is_non_html

dz = zstandard.ZstdDecompressor()
dry = "--dry-run" in sys.argv
total = 0
for sub in ("db", "db_wayback", "db_alt", "db_cc"):
    for f in sorted((OUT / sub).glob("*.sqlite")):
        with sqlite3.connect(f, timeout=120) as c:
            bad = []
            for i, final, ctype, h in c.execute("SELECT id, final_url, ctype, html FROM pages WHERE kind='ok'"):
                url = final or ""
                if sub == "db_wayback" and "id_/" in url:
                    url = url.split("id_/", 1)[1]
                # archive responses carry the archive's own content-type, so only trust the body/extension there
                ct = (ctype or "") if sub in ("db", "db_alt") else ""
                head = dz.stream_reader(io.BytesIO(h)).read(16) if h else b""   # only the first bytes
                if is_non_html(url, ct, head):
                    bad.append(i)
            if bad:
                print(f"{sub}/{f.stem}: {len(bad)} non-HTML pages")
                total += len(bad)
                if not dry:
                    c.executemany("UPDATE pages SET kind='nonhtml', html=NULL WHERE id=?", [(i,) for i in bad])
print(("would fix" if dry else "fixed"), total, "pages")
