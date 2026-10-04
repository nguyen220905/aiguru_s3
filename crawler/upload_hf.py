"""Script to upload crawled dataset to Hugging Face dataset repo snute/ai_guru-S3.

Usage:
    python crawler/upload_hf.py --token <HF_WRITE_TOKEN>
    python crawler/upload_hf.py --token <HF_WRITE_TOKEN> --include-archives
"""
import argparse
import logging
import sys
from pathlib import Path
from huggingface_hub import HfApi, login

ROOT = Path("D:/project_r2ai")
REPO_ID = "snute/ai_guru-S3"

log = logging.getLogger("upload_hf")

def main():
    parser = argparse.ArgumentParser(description="Upload dataset to Hugging Face")
    parser.add_argument("--token", type=str, required=True, help="Hugging Face Write Token (hf_...)")
    parser.add_argument("--repo-id", type=str, default=REPO_ID, help="Target Hugging Face Dataset repo")
    parser.add_argument("--include-archives", action="store_true", help="Include db_wayback, db_cc, db_alt")
    args = parser.parse_args()

    token = args.token.strip()
    log.info("Logging in to Hugging Face...")
    login(token=token)

    api = HfApi(token=token)
    user_info = api.whoami()
    log.info("Authenticated as: %s", user_info.get("name") or user_info.get("fullname"))

    # Upload main SQLite databases
    db_dir = ROOT / "crawl" / "db"
    log.info("Uploading direct crawl SQLite databases from %s to %s...", db_dir, args.repo_id)
    api.upload_folder(
        folder_path=str(db_dir),
        path_in_repo="db",
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message="Add direct crawl SQLite databases",
    )
    log.info("Successfully uploaded crawl/db/")

    # Upload metadata and reports
    for report_file in ["coverage.csv"]:
        f_path = ROOT / "crawl" / report_file
        if f_path.exists():
            log.info("Uploading report %s...", report_file)
            api.upload_file(
                path_or_fileobj=str(f_path),
                path_in_repo=f"reports/{report_file}",
                repo_id=args.repo_id,
                repo_type="dataset",
                commit_message=f"Add report {report_file}",
            )

    for doc_file in ["missing_pages.csv", "missing_pages.md"]:
        f_path = ROOT / doc_file
        if f_path.exists():
            log.info("Uploading %s...", doc_file)
            api.upload_file(
                path_or_fileobj=str(f_path),
                path_in_repo=f"reports/{doc_file}",
                repo_id=args.repo_id,
                repo_type="dataset",
                commit_message=f"Add report {doc_file}",
            )

    if args.include_archives:
        for sub in ["db_wayback", "db_cc", "db_alt"]:
            sub_dir = ROOT / "crawl" / sub
            if sub_dir.exists():
                log.info("Uploading archive folder %s...", sub)
                api.upload_folder(
                    folder_path=str(sub_dir),
                    path_in_repo=sub,
                    repo_id=args.repo_id,
                    repo_type="dataset",
                    commit_message=f"Add {sub} SQLite databases",
                )

    log.info("ALL DATASET UPLOADS COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
