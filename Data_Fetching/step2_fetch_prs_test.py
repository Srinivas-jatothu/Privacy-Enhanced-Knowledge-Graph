# step2_fetch_prs_test.py
import os
import sys
import logging
import time
import requests
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fetch-prs")

def get_env():
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("REPO")
    return token, repo

def fetch_all_prs(token, repo):
    owner_repo = repo.split("/")
    if len(owner_repo) != 2:
        log.error("REPO value is not owner/repo: %s", repo)
        return []
    owner, repo_name = owner_repo
    url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    per_page = 100
    page = 1
    all_prs = []
    while True:
        params = {"state": "all", "per_page": per_page, "page": page}
        log.debug("Requesting page %d (per_page=%d)", page, per_page)
        r = requests.get(url, headers=headers, params=params, timeout=30)
        log.debug("HTTP %s  (URL: %s)", r.status_code, r.url)
        # rate-limit headers (if present)
        if "X-RateLimit-Limit" in r.headers:
            log.info("RateLimit: limit=%s remaining=%s reset=%s",
                     r.headers.get("X-RateLimit-Limit"),
                     r.headers.get("X-RateLimit-Remaining"),
                     r.headers.get("X-RateLimit-Reset"))
        # handle auth errors
        if r.status_code == 401:
            log.error("Unauthorized (401). Token likely invalid or missing.")
            return []
        if r.status_code == 403:
            log.error("Forbidden (403). Possibly rate-limited or no access. Body preview: %s", r.text[:400])
            return []
        r.raise_for_status()
        page_items = r.json()
        if not isinstance(page_items, list):
            log.error("Expected list response; got: %s", type(page_items))
            return all_prs
        all_prs.extend(page_items)
        log.info("Fetched %d PRs so far (page %d returned %d items).", len(all_prs), page, len(page_items))
        # stop when page has fewer than per_page items
        if len(page_items) < per_page:
            break
        page += 1
        # tiny sleep to be polite (and give you visible logs)
        time.sleep(0.2)
    return all_prs

def summarize_prs(prs, max_show=5):
    log.info("Total PRs fetched: %d", len(prs))
    for i, pr in enumerate(prs[:max_show], start=1):
        log.info("PR #%d -> number=%s title=%r user=%s state=%s created_at=%s merged_at=%s",
                 i,
                 pr.get("number"),
                 pr.get("title"),
                 pr.get("user", {}).get("login") if pr.get("user") else None,
                 pr.get("state"),
                 pr.get("created_at"),
                 pr.get("merged_at"))

def main():
    token, repo = get_env()
    log.info("Using REPO=%s   Token set=%s", repo or "<missing>", "YES" if token else "NO")
    if not repo:
        log.error("REPO is not set. Stop and set REPO in .env or env vars.")
        sys.exit(2)

    prs = fetch_all_prs(token, repo)
    summarize_prs(prs)
    # optional: write a tiny JSON sample file for inspection
    try:
        import json
        sample = {"repo": repo, "total_prs": len(prs), "sample_first": prs[:10]}
        with open("results/prs_sample.json", "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False)
        log.info("Wrote sample file: results/prs_sample.json")
    except Exception as e:
        log.warning("Could not write sample file: %s", e)

if __name__ == "__main__":
    main()



#here are we saving all the prs or with the limit? so the answer is we are saving all the prs