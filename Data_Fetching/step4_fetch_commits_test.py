# step4_fetch_commits_test.py
import os
import sys
import time
import json
import logging
import requests

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("step4-commits")

def env():
    return os.getenv("GITHUB_TOKEN"), os.getenv("REPO")

def ensure_outdir(path="results"):
    os.makedirs(path, exist_ok=True)
    return path

def fetch_commits_paginated(token, owner, repo_name, max_pages=None):
    url = f"https://api.github.com/repos/{owner}/{repo_name}/commits"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    per_page = 100
    page = 1
    all_commits = []
    while True:
        params = {"per_page": per_page, "page": page}
        log.debug("Requesting commits page %d", page)
        r = requests.get(url, headers=headers, params=params, timeout=30)
        log.debug("HTTP %s  URL: %s", r.status_code, r.url)
        if "X-RateLimit-Remaining" in r.headers:
            log.info("RateLimit: remaining=%s reset=%s", r.headers.get("X-RateLimit-Remaining"), r.headers.get("X-RateLimit-Reset"))
        if r.status_code == 401:
            log.error("Unauthorized (401) — check token.")
            break
        if r.status_code == 403:
            log.error("Forbidden (403) — maybe rate-limited or no access. Body preview: %s", r.text[:400])
            break
        r.raise_for_status()
        items = r.json()
        if not isinstance(items, list):
            log.error("Unexpected response type for commits endpoint.")
            break
        # normalize minimal metadata
        for c in items:
            commit_meta = {
                "sha": c.get("sha"),
                "message_head": (c.get("commit", {}).get("message") or "").splitlines()[0] if c.get("commit") else None,
                "author_name": c.get("commit", {}).get("author", {}).get("name"),
                "author_email": c.get("commit", {}).get("author", {}).get("email"),
                "author_date": c.get("commit", {}).get("author", {}).get("date"),
                "parents": [p.get("sha") for p in c.get("parents", [])]
            }
            all_commits.append(commit_meta)
        log.info("Fetched page %d -> %d commits (total %d)", page, len(items), len(all_commits))
        # break if fewer than full page (last page)
        if len(items) < per_page:
            break
        page += 1
        if max_pages and page > max_pages:
            log.info("Reached max_pages (%s) — stopping early.", max_pages)
            break
        time.sleep(0.2)  # be polite
    return all_commits

def main():
    token, repo = env()
    log.info("Using REPO=%s  Token set=%s", repo or "<missing>", "YES" if token else "NO")
    if not repo:
        log.error("REPO not set. Aborting.")
        sys.exit(2)
    owner, repo_name = repo.split("/")
    outdir = ensure_outdir("results")

    # OPTIONAL: set max_pages to limit how many pages you fetch for this test.
    # Set to None to fetch all pages. For initial test, we can fetch up to 5 pages (500 commits).
    MAX_PAGES_FOR_TEST = 5

    commits = fetch_commits_paginated(token, owner, repo_name, max_pages=MAX_PAGES_FOR_TEST)
    log.info("Total commits fetched in this run: %d", len(commits))
    sample = {
        "repo": repo,
        "total_fetched": len(commits),
        "sample_first_n": commits[:20],
        "last_sha": commits[-1]["sha"] if commits else None
    }
    out_file = os.path.join(outdir, "commits_sample.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)
    log.info("Wrote sample commits to %s", out_file)
    # print short preview for your copy-paste convenience
    for i, c in enumerate(sample["sample_first_n"][:5], start=1):
        log.info("Sample %d: sha=%s head=%r author=%s date=%s", i, c["sha"][:7], c["message_head"], c["author_name"], c["author_date"])

if __name__ == "__main__":
    main()
