"""Fetch the PubMed part of the corpus (pubmed.ncbi.nlm.nih.gov/<PMID>/) through NCBI E-utilities
(efetch, PubMed XML, 200 PMIDs per request) instead of scraping 240k HTML pages.

NCBI allows 3 requests/s without an API key (10/s with one: set NCBI_API_KEY). Each PubmedArticle XML
record is stored like any other page in crawl/db/pubmed.ncbi.nlm.nih.gov.sqlite (ctype application/xml);
PMIDs NCBI does not return are stored as kind='notfound'.

Usage: python pubmed.py
"""
import logging
import os
import re
import time

import pandas as pd
import requests

from crawl import CORPUS, OUT, HostStore

HOST = "pubmed.ncbi.nlm.nih.gov"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
BATCH = 200
API_KEY = os.environ.get("NCBI_API_KEY", "")
INTERVAL = 0.12 if API_KEY else 0.4

log = logging.getLogger("pubmed")


def main():
    df = pd.read_parquet(CORPUS)
    df = df[df.url.str.contains("://pubmed.ncbi.nlm.nih.gov/", regex=False)].copy()
    df["pmid"] = df.url.str.extract(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)")[0]
    store = HostStore(HOST)
    done, _ = store.state()
    todo = df[~df.id.isin(done) & df.pmid.notna()]
    log.info("%d PubMed records to fetch (%d already stored)", len(todo), len(df) - len(todo))
    by_pmid = dict(zip(todo.pmid, zip(todo.id, todo.url)))
    pmids = list(by_pmid)
    s = requests.Session()
    s.headers["User-Agent"] = "r2ai-research-crawler/0.1"
    params = dict(db="pubmed", retmode="xml", tool="r2ai-ir-crawler")
    if API_KEY:
        params["api_key"] = API_KEY
    for b in range(0, len(pmids), BATCH):
        batch = pmids[b:b + BATCH]
        for attempt in range(8):
            t0 = time.time()
            try:
                r = s.post(EFETCH, data=dict(params, id=",".join(batch)), timeout=120)
                if r.status_code == 200:
                    break
                log.warning("efetch HTTP %s, retrying", r.status_code)
            except requests.RequestException as e:
                log.warning("efetch %s, retrying", str(e)[:100])
            time.sleep(min(60, 3 * 2 ** attempt))
        else:
            log.error("giving up on batch starting %s", batch[0])
            continue
        xml = r.content
        found = set()
        for rec in re.findall(rb"<PubmedArticle>.*?</PubmedArticle>", xml, re.S):
            m = re.search(rb'<PMID[^>]*>(\d+)</PMID>', rec)
            if not m or m.group(1).decode() not in by_pmid:
                continue
            pmid = m.group(1).decode()
            found.add(pmid)
            doc_id, url = by_pmid[pmid]
            store.add((doc_id, url, f"{EFETCH}?db=pubmed&id={pmid}", 200, "ok", "application/xml", len(rec),
                       time.time(), 1, "efetch", store.cctx.compress(rec)))
        for pmid in set(batch) - found:
            doc_id, url = by_pmid[pmid]
            store.add((doc_id, url, None, 404, "notfound", None, 0, time.time(), 1, "efetch: not returned", None))
        store._write(store.buf)
        store.buf = []
        if (b // BATCH) % 25 == 0:
            log.info("%d / %d PMIDs", b + len(batch), len(pmids))
        time.sleep(max(0.0, INTERVAL - (time.time() - t0)))
    log.info("pubmed finished")


if __name__ == "__main__":
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(OUT / "logs" / "pubmed.log", encoding="utf-8"),
                                  logging.StreamHandler()])
    main()
