"""Per-host coverage: what is stored, what has a known source still queued, and what has no source.

stored          page saved from any source (direct crawl, Wayback, alternate site, Common Crawl)
dead            the original site answered 404 / 4xx or redirected to its home page (article removed)
robots          skipped because robots.txt disallows it
queued_direct   still in the direct crawl queue (site reachable)
queued_archive  not stored yet, but a Wayback / Common Crawl capture or an alternate URL is known
no_source       blocked/down host and no capture found anywhere (Common Crawl lookup may still add some)
"""
import json
import re
import sqlite3
import sys
import time
from urllib.parse import unquote, urlparse

import pandas as pd

import cc
import wayback
from crawl import CORPUS, NON_HTML_EXT, OUT, ROOT

SUBDIRS = ("db", "db_wayback", "db_alt", "db_cc")
ROBOTS_DISALLOWED = {"www.baidu.com", "baolangson.vn"}   # robots.txt disallows the corpus paths


def _url_key(u):
    p = urlparse(u)
    host = (p.hostname or "").removeprefix("www.")
    return host, unquote(p.path).replace("\xa0", " ").strip().rstrip("/").lower()


def _slug_tokens(path):
    last = re.sub(r"\.\w{2,5}$", "", path.rsplit("/", 1)[-1])
    return {t for t in re.split(r"[-_\s.]+", last) if t and not t.isdigit()}


def redirect_kind(url, final):
    """None if final is the same URL; else same_article / different_article / non_html."""
    (hu, pu), (hf, pf) = _url_key(url), _url_key(final)
    if (hu, pu) == (hf, pf):
        return None
    if NON_HTML_EXT.search(pf):
        return "non_html"
    last_u, last_f = pu.rsplit("/", 1)[-1], pf.rsplit("/", 1)[-1]
    if any(i in last_f for i in re.findall(r"\d{5,}", last_u)):   # article id kept (e.g. moved to tuoitre.vn)
        return "same_article"
    tu, tf = _slug_tokens(pu), _slug_tokens(pf)
    if tu and len(tu & tf) / len(tu | tf) >= 0.6:
        return "same_article"
    return "different_article"


REDIRECT_VI = {
    "same_article": "cùng bài, chỉ đổi URL/domain (an toàn)",
    "different_article": "chuyển sang bài khác (bài bị gộp/thay) — nội dung có thể không khớp đáp án",
    "non_html": "chuyển về file ảnh/PDF — không có nội dung bài",
}


def redirected_pages():
    """Stored pages whose content came from a different URL than the corpus one, classified by
    redirect_kind(). different_article / non_html pages may not hold the article the organisers indexed."""
    rows = []
    for sub in SUBDIRS:
        for f in (OUT / sub).glob("*.sqlite"):
            with sqlite3.connect(f"file:{f}?mode=ro", uri=True, timeout=60) as c:
                for i, url, final in c.execute("SELECT id, url, final_url FROM pages WHERE kind='ok'"):
                    if not final:
                        continue
                    if sub == "db_wayback":   # https://web.archive.org/web/<ts>id_/<captured url>
                        m = re.search(r"/web/\d+id_/(.+)$", final)
                        final = m.group(1) if m else final
                    if sub == "db_alt" or "zysj.com.cn" in final:   # deliberate alternate source, not a redirect
                        continue
                    kind = redirect_kind(url, final)
                    if kind:
                        rows.append((i, url, final, f.stem, sub, kind))
    return pd.DataFrame(rows, columns=["id", "url", "final_url", "host", "source", "redirect"])


def ids_of(host, kinds, subdirs=SUBDIRS):
    out = set()
    for sub in subdirs:
        f = OUT / sub / f"{host}.sqlite"
        if f.exists():
            with sqlite3.connect(f"file:{f}?mode=ro", uri=True, timeout=60) as c:
                q = f"SELECT id FROM pages WHERE kind IN ({','.join('?' * len(kinds))})"
                out |= {r[0] for r in c.execute(q, kinds)}
    return out


def main():
    df = pd.read_parquet(CORPUS)
    df["host"] = df.url.map(lambda u: urlparse(u).netloc.lower())
    problem = set(wayback.TARGET_HOSTS)

    archive_src = set(wayback.load_matches())                      # Wayback captures (incl. zysj mapping)
    want = cc.wanted()
    for f in (OUT / "cc" / "index").glob("*/*.jsonl"):             # Common Crawl captures found so far
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                k = cc.capture_key(json.loads(line)["url"])
                if k in want:
                    archive_src.add(want[k][0])
    archive_src |= set(df[df.host == "pmc-ecm-healthblog.beta.pharmacity.io"].id)   # same slugs on pharmacity.vn

    rows, missing = [], []   # missing: (id, url, host, reason) for pages we cannot get
    for host, g in df.groupby("host"):
        ids = set(g.id)
        stored = ids_of(host, ("ok",)) & ids
        dead = (ids_of(host, ("notfound", "home", "http4xx"), ("db",)) & ids) - stored
        robots = (ids_of(host, ("robots",), ("db",)) & ids) - stored
        if host in ROBOTS_DISALLOWED:   # crawl.py will not fetch these, whatever an earlier attempt recorded
            robots |= ids - stored - dead
        rest = ids - stored - dead - robots
        r = dict(host=host, total=len(ids), stored=len(stored), dead=len(dead), robots=len(robots))
        no_src = rest - archive_src if host in problem else set()
        if host in problem:
            r["queued_archive"] = len(rest & archive_src)
            r["no_source"] = len(no_src)
            r["queued_direct"] = 0
        else:
            r["queued_direct"], r["queued_archive"], r["no_source"] = len(rest), 0, 0
        rows.append(r)
        url_of = dict(zip(g.id, g.url))
        for reason, sel in (("no_source", no_src), ("dead", dead), ("robots", robots)):
            missing += [(i, url_of[i], host, reason) for i in sorted(sel)]
        if host == "zysjonline.com":
            kind = g.url.str.extract(r"zysjonline\.com/([a-z]+)/")[0]
            for k, sub in g.groupby(kind):
                s = set(sub.id)
                rest_k = s - stored
                rows.append(dict(host=f"  zysjonline/{k}", total=len(s), stored=len(s & stored), dead=0, robots=0,
                                 queued_direct=0, queued_archive=len(rest_k & archive_src),
                                 no_source=len(rest_k - archive_src)))
    rep = pd.DataFrame(rows).set_index("host")
    rep.to_csv(OUT / "coverage.csv")
    pd.set_option("display.width", 220)
    main_rows = rep[~rep.index.str.startswith("  ")]
    print(rep.loc[[h for h in rep.index if h in problem or h.startswith("  ")]].to_string())
    print()
    print(main_rows.sort_values("total", ascending=False).head(25).to_string())
    print("\nTOTAL", main_rows.sum(numeric_only=True).to_dict())
    if "--export" in sys.argv:
        export_missing(missing, main_rows)


REASON_VI = {
    "no_source": "Site chặn bot / server sập và không có bản lưu nào (Wayback Machine, Common Crawl, site thay thế)",
    "no_source_book_rejected": "Sách zysjonline bị sửa nội dung giữa bản cũ và bản mới nên không ánh xạ chắc chắn được sang zysj.com.cn",
    "no_source_not_archived": "zysjonline: ánh xạ được sang zysj.com.cn nhưng trang cũ không có bản lưu nào",
    "dead": "Trang gốc đã bị xóa (404 / lỗi 4xx / chuyển hướng về trang chủ)",
    "robots": "robots.txt của site cấm thu thập",
}


def refine_zysj(missing):
    """Split zysjonline no_source pages by why they could not be mapped/recovered."""
    rep = OUT / "wayback" / "zysj_books_report.csv"
    bad_books = set()
    if rep.exists():
        r = pd.read_csv(rep)
        bad_books = set(r[r.status != "aligned"].book)
    out = []
    for i, url, host, reason in missing:
        if host == "zysjonline.com" and reason == "no_source" and re.search(r"/(books|articles)/", url):
            m = re.search(r"/books/([^/]+)/", url)
            reason = "no_source_book_rejected" if m and m.group(1) in bad_books else "no_source_not_archived"
        out.append((i, url, host, reason))
    return out


def export_missing(missing, per_host):
    missing = refine_zysj(missing)
    m = pd.DataFrame(missing, columns=["id", "url", "host", "reason"])
    m.to_csv(ROOT / "missing_pages.csv", index=False, encoding="utf-8")
    tot = per_host.sum(numeric_only=True)
    lines = [
        "# Danh sách trang chưa lấy được",
        "",
        f"Cập nhật: {time.strftime('%Y-%m-%d %H:%M')}. Nguồn: `links_corpus.parquet` ({int(tot.total):,} URL).",
        "Bản dạng bảng cho máy đọc: `missing_pages.csv` (cột id, url, host, reason).",
        "",
        "Các trang **đang trong hàng đợi** (site truy cập được, hoặc đã tìm thấy bản lưu và đang tải) không nằm trong"
        " danh sách này vì sẽ lấy được; chỉ ghi số lượng ở bảng dưới.",
        "",
        "## Tổng quan",
        "",
        "| Tình trạng | Số URL |",
        "|---|---:|",
        f"| Đã lấy về | {int(tot.stored):,} |",
        f"| Đang trong hàng đợi crawl trực tiếp | {int(tot.queued_direct):,} |",
        f"| Đã tìm được bản lưu / nguồn thay thế, đang tải | {int(tot.queued_archive):,} |",
        f"| **Không lấy được** (liệt kê bên dưới) | **{len(m):,}** |",
        "",
        "## Lý do không lấy được",
        "",
        "| Lý do | Số URL |",
        "|---|---:|",
    ]
    for reason, n in m.reason.value_counts().items():
        lines.append(f"| {REASON_VI[reason]} | {n:,} |")
    lines += ["", "## Theo domain", "", "| Domain | Không lấy được | Lý do chính |", "|---|---:|---|"]
    by_host = m.groupby("host")
    for host, g in sorted(by_host, key=lambda kv: -len(kv[1])):
        main_reason = g.reason.value_counts().index[0]
        lines.append(f"| {host} | {len(g):,} | {REASON_VI[main_reason]} |")
    red = redirected_pages()
    red.to_csv(ROOT / "redirected_pages.csv", index=False, encoding="utf-8")
    lines += ["", "## Trang đã lấy nhưng bị chuyển hướng sang URL khác", "",
              "Các trang này **đã lưu**, nhưng site gốc trả về nội dung từ một URL khác. Phân loại:", ""]
    lines += [f"- **{k}**: {REDIRECT_VI[k]}" for k in REDIRECT_VI]
    lines += ["", "Danh sách đầy đủ: `redirected_pages.csv` (id, url, final_url, host, source, redirect).", "",
              "| Domain | Đã lưu | same_article | different_article | non_html |", "|---|---:|---:|---:|---:|"]
    stored_by_host = per_host.stored.to_dict()
    tab = red.pivot_table(index="host", columns="redirect", values="id", aggfunc="count", fill_value=0)
    for k in REDIRECT_VI:
        if k not in tab:
            tab[k] = 0
    tab = tab.sort_values(["different_article", "non_html", "same_article"], ascending=False)
    for host, r in tab.iterrows():
        lines.append(f"| {host} | {int(stored_by_host.get(host, 0)):,} | {r.same_article:,} | "
                     f"{r.different_article:,} | {r.non_html:,} |")
    lines += ["", "## Chi tiết", ""]
    for host, g in sorted(by_host, key=lambda kv: -len(kv[1])):
        lines.append(f"### {host} ({len(g):,} trang)")
        lines.append("")
        for reason, gr in g.groupby("reason"):
            lines.append(f"**{REASON_VI[reason]}** — {len(gr):,} trang")
            lines.append("")
            lines.append("```")
            lines += [f"{i}\t{u}" for i, u in zip(gr.id, gr.url)]
            lines.append("```")
            lines.append("")
    (ROOT / "missing_pages.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {ROOT / 'missing_pages.md'} ({len(m):,} pages)")


if __name__ == "__main__":
    main()
