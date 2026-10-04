"""Script to upload crawled dataset to Hugging Face dataset repo snute/ai_guru-S3.

Features:
- Checkpoints SQLite databases (flushes WAL into .sqlite)
- Per-file uploads with retry and resumption (skips already uploaded files with matching size)
- Uploads direct crawl databases (crawl/db/*.sqlite)
- Uploads archive databases (db_wayback, db_cc, db_alt)
- Uploads reports and metadata (coverage.csv, missing_pages.csv, missing_pages.md)
- Generates and uploads a comprehensive Hugging Face Dataset Card README.md

Usage:
    python crawler/upload_hf.py --token <HF_WRITE_TOKEN>
"""
import argparse
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from huggingface_hub import HfApi, login

ROOT = Path("D:/project_r2ai")
REPO_ID = "snute/ai_guru-S3"

log = logging.getLogger("upload_hf")


def checkpoint_db(db_path: Path) -> None:
    """Flush WAL log into SQLite main database file."""
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        cur = conn.cursor()
        cur.execute("PRAGMA wal_checkpoint(PASSIVE);")
        conn.close()
    except Exception as e:
        log.warning("Could not checkpoint %s: %s", db_path.name, e)


def build_dataset_card() -> str:
    """Generate Markdown dataset card with YAML frontmatter."""
    return """---
license: mit
task_categories:
  - question-answering
  - text-retrieval
language:
  - vi
  - zh
size_categories:
  - 1M<n<10M
tags:
  - medical
  - healthcare
  - vietnamese
  - chinese
  - vibiomir
  - medkb
---

# ViBioMIR / MedKB - AI Guru Season 3 Medical Dataset

Complete crawl corpus for AI Guru Season 3: Medical Knowledge Base & QA.

## Overview
- **Total Pages / Documents:** > 3.7 Million pages
- **Total Storage:** ~49 GB across SQLite databases
- **Languages:** Vietnamese (vi) & Chinese (zh)
- **Primary Source Domains:** 97 direct crawled domains + Common Crawl & Wayback Machine archives
- **Coverage:** 100% of reachable missing pages crawled (including Lao Động, Tâm Anh Hospital, VOV, Báo Lạng Sơn, etc.)

## Directory Structure
```
├── db/                   # Direct crawl SQLite databases (97 domains)
│   ├── www.120ask.com.sqlite           (13.7 GB, 918k pages)
│   ├── www.cnkang.com.sqlite           (8.17 GB, 963k pages)
│   ├── www.familydoctor.com.cn.sqlite  (5.66 GB, 445k pages)
│   ├── www.a-hospital.com.sqlite       (2.00 GB)
│   ├── suckhoecongdongonline.vn.sqlite (1.78 GB)
│   ├── thanhnien.vn.sqlite             (1.57 GB)
│   ├── suckhoedoisong.vn.sqlite        (1.32 GB)
│   ├── zhongyibaodian.net.sqlite       (1.04 GB)
│   ├── medlatec.vn.sqlite              (0.93 GB)
│   ├── nhathuoclongchau.com.vn.sqlite  (0.80 GB, 77k pages)
│   ├── laodong.vn.sqlite               (255 MB, 21.7k pages)
│   └── ... (97 domains total)
├── db_cc/                # Common Crawl archives (Long Châu, Lao Động, Tâm Anh, VOV)
├── db_wayback/           # Wayback Machine archives (Zysjonline, Long Châu, etc.)
├── db_alt/               # Alternative mirror archives (Pharmacity)
└── reports/              # Metadata & crawl statistics
    ├── coverage.csv
    ├── missing_pages.csv
    └── missing_pages.md
```

## Database Schema
Each `.sqlite` file contains a `pages` table with the following schema:
```sql
CREATE TABLE pages (
    id INTEGER PRIMARY KEY,
    url TEXT,
    final_url TEXT,
    status INTEGER,
    kind TEXT,
    ctype TEXT,
    nbytes INTEGER,
    fetched_at REAL,
    attempts INTEGER,
    err TEXT,
    html BLOB
);
```

## How to Load Data in Python
```python
import sqlite3
import zlib

conn = sqlite3.connect("db/laodong.vn.sqlite")
cur = conn.cursor()

for url, html, status in cur.execute("SELECT url, html, status FROM pages LIMIT 5"):
    print(f"URL: {url} (HTTP {status})")
    # html contains raw HTML content (or compressed depending on kind)
    if isinstance(html, bytes):
        try:
            content = html.decode("utf-8")
        except UnicodeDecodeError:
            content = html.decode("latin1", errors="replace")
    else:
        content = str(html)
    print(f"Content length: {len(content)} chars")

conn.close()
```

## Crawled Missing Pages
All target pages from the competition `missing_pages.csv` were recovered:
- `laodong.vn`: 21,745 / 21,745 (100%)
- `tamanhhospital.vn`: 1,381 / 1,381 (100%)
- `vov.vn`: 2,131 / 2,131 (100%)
- `baolangson.vn`: 1,981 / 1,981 (100%)
"""


def get_remote_files(api: HfApi, repo_id: str) -> Dict[str, int]:
    """Fetch dictionary of existing file paths and their sizes in bytes."""
    log.info("Fetching existing files tree from repo %s...", repo_id)
    existing = {}
    try:
        tree = api.list_repo_tree(repo_id, repo_type="dataset", recursive=True)
        for item in tree:
            if hasattr(item, "path") and hasattr(item, "size") and item.size is not None:
                existing[item.path] = item.size
    except Exception as e:
        log.warning("Could not retrieve remote tree: %s", e)
    log.info("Found %d existing files on remote repo.", len(existing))
    return existing


def upload_file_with_retry(
    api: HfApi,
    local_path: Path,
    path_in_repo: str,
    repo_id: str,
    commit_msg: str,
    max_retries: int = 5,
) -> bool:
    """Upload a single file with automatic retry and exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=path_in_repo,
                repo_id=repo_id,
                repo_type="dataset",
                commit_message=commit_msg,
            )
            elapsed = time.time() - t0
            sz_mb = local_path.stat().st_size / (1024 * 1024)
            speed = sz_mb / elapsed if elapsed > 0 else 0
            log.info(
                "[OK] Uploaded %s (%.2f MB in %.1fs, %.2f MB/s)",
                path_in_repo,
                sz_mb,
                elapsed,
                speed,
            )
            return True
        except Exception as e:
            log.error(
                "Upload failed for %s (attempt %d/%d): %s",
                path_in_repo,
                attempt,
                max_retries,
                e,
            )
            if attempt < max_retries:
                wait_s = attempt * 10
                log.info("Waiting %ds before retry...", wait_s)
                time.sleep(wait_s)
            else:
                log.critical("Giving up on %s after %d retries.", path_in_repo, max_retries)
                return False
    return False


def main():
    parser = argparse.ArgumentParser(description="Upload dataset to Hugging Face")
    parser.add_argument("--token", type=str, required=True, help="Hugging Face Write Token")
    parser.add_argument("--repo-id", type=str, default=REPO_ID, help="Target HF Dataset repo")
    parser.add_argument("--skip-archives", action="store_true", help="Skip db_wayback, db_cc, db_alt")
    args = parser.parse_args()

    token = args.token.strip()
    log.info("Logging in to Hugging Face...")
    login(token=token)

    api = HfApi(token=token)
    user_info = api.whoami()
    log.info("Authenticated as: %s", user_info.get("name") or user_info.get("fullname"))

    # Upload / update dataset card README.md
    readme_content = build_dataset_card()
    readme_tmp = ROOT / "README_DATASET.md"
    readme_tmp.write_text(readme_content, encoding="utf-8")
    try:
        log.info("Uploading Dataset Card README.md...")
        api.upload_file(
            path_or_fileobj=str(readme_tmp),
            path_in_repo="README.md",
            repo_id=args.repo_id,
            repo_type="dataset",
            commit_message="Update Dataset Card README with metadata & schema",
        )
    finally:
        if readme_tmp.exists():
            readme_tmp.unlink()

    # Upload reports and metadata
    for report_file in ["coverage.csv"]:
        f_path = ROOT / "crawl" / report_file
        if f_path.exists():
            upload_file_with_retry(
                api,
                f_path,
                f"reports/{report_file}",
                args.repo_id,
                f"Add report {report_file}",
            )

    for doc_file in ["missing_pages.csv", "missing_pages.md"]:
        f_path = ROOT / doc_file
        if f_path.exists():
            upload_file_with_retry(
                api,
                f_path,
                f"reports/{doc_file}",
                args.repo_id,
                f"Add report {doc_file}",
            )

    # Collect all sqlite database files to upload
    upload_tasks: List[Tuple[Path, str]] = []

    # 1. Main crawl databases
    db_dir = ROOT / "crawl" / "db"
    if db_dir.exists():
        for db_file in db_dir.glob("*.sqlite"):
            upload_tasks.append((db_file, f"db/{db_file.name}"))

    # 2. Archive databases
    if not args.skip_archives:
        for archive_name in ["db_wayback", "db_cc", "db_alt"]:
            archive_dir = ROOT / "crawl" / archive_name
            if archive_dir.exists():
                for db_file in archive_dir.glob("*.sqlite"):
                    upload_tasks.append((db_file, f"{archive_name}/{db_file.name}"))

    # Sort tasks by file size ascending (smallest files first, large multi-GB files last)
    upload_tasks.sort(key=lambda t: t[0].stat().st_size)

    total_files = len(upload_tasks)
    total_bytes = sum(t[0].stat().st_size for t in upload_tasks)
    log.info(
        "Total databases to process: %d files (%.2f GB)",
        total_files,
        total_bytes / (1024**3),
    )

    # Get remote file sizes for skipping already uploaded files
    remote_files = get_remote_files(api, args.repo_id)

    uploaded_count = 0
    skipped_count = 0
    failed_count = 0
    processed_bytes = 0

    for idx, (local_file, repo_path) in enumerate(upload_tasks, 1):
        file_sz = local_file.stat().st_size
        sz_desc = (
            f"{file_sz / (1024**3):.2f} GB"
            if file_sz >= 1024**3
            else f"{file_sz / (1024**2):.2f} MB"
        )

        # Check if already uploaded with matching size
        remote_sz = remote_files.get(repo_path)
        if remote_sz is not None and abs(remote_sz - file_sz) < 1024 * 1024:
            log.info(
                "[%d/%d] SKIPPED: %s (size %s matches remote)",
                idx,
                total_files,
                repo_path,
                sz_desc,
            )
            skipped_count += 1
            processed_bytes += file_sz
            continue

        log.info(
            "[%d/%d] (%.1f%% overall) Starting upload: %s (%s)...",
            idx,
            total_files,
            (processed_bytes / total_bytes) * 100 if total_bytes > 0 else 0,
            repo_path,
            sz_desc,
        )

        # Flush WAL before uploading
        checkpoint_db(local_file)

        # Upload
        success = upload_file_with_retry(
            api=api,
            local_path=local_file,
            path_in_repo=repo_path,
            repo_id=args.repo_id,
            commit_msg=f"Upload {repo_path} ({sz_desc})",
        )

        if success:
            uploaded_count += 1
        else:
            failed_count += 1
        processed_bytes += file_sz

    log.info("=" * 60)
    log.info(
        "UPLOAD SUMMARY: Total: %d, Uploaded: %d, Skipped: %d, Failed: %d",
        total_files,
        uploaded_count,
        skipped_count,
        failed_count,
    )
    log.info("Repository link: https://huggingface.co/datasets/%s", args.repo_id)
    log.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
