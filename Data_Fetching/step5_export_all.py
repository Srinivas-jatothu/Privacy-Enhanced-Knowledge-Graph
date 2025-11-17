# step5_export_all.py
import os
import sys
import time
import json
import logging
import requests
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("step5-export")

GITHUB_API = "https://api.github.com"
PER_PAGE = 100

def get_env():
    return os.getenv("GITHUB_TOKEN"), os.getenv("REPO")

def ensure_outdir(path="results"):
    os.makedirs(path, exist_ok=True)
    return path

def request_with_retry(session, url, headers=None, params=None, max_retries=5):
    """Simple request wrapper with exponential backoff for 403/429/5xx."""
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        r = session.get(url, headers=headers, params=params, timeout=30)
        log.debug("HTTP %s  URL: %s", r.status_code, getattr(r, "url", url))
        if "X-RateLimit-Remaining" in r.headers:
            log.info("RateLimit: remaining=%s reset=%s", r.headers.get("X-RateLimit-Remaining"), r.headers.get("X-RateLimit-Reset"))
        # success
        if r.status_code == 200:
            return r
        # unauthorized -> stop
        if r.status_code == 401:
            log.error("401 Unauthorized for URL: %s", url)
            r.raise_for_status()
        # rate limited or server error: backoff and retry
        if r.status_code in (403, 429) or 500 <= r.status_code < 600:
            log.warning("Transient HTTP %s on attempt %d. Backing off %.1fs", r.status_code, attempt, delay)
            time.sleep(delay)
            delay *= 2
            continue
        # other non-200: raise for inspection
        r.raise_for_status()
    # if we exit loop, raise last response status
    r.raise_for_status()

def paginate(session, url, headers, params_extra=None):
    """Generic paginator returning list of items."""
    items = []
    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if params_extra:
            params.update(params_extra)
        r = request_with_retry(session, url, headers=headers, params=params)
        page_items = r.json()
        if not isinstance(page_items, list):
            log.debug("Non-list paginated response; returning what we have.")
            return items
        items.extend(page_items)
        log.info("Paginated: fetched page %d -> %d items (total %d)", page, len(page_items), len(items))
        if len(page_items) < PER_PAGE:
            break
        page += 1
        time.sleep(0.12)
    return items

def fetch_all_commits(session, owner, repo_name, token):
    url = f"{GITHUB_API}/repos/{owner}/{repo_name}/commits"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    raw = paginate(session, url, headers)
    # normalize minimal metadata
    commits = []
    for c in raw:
        commits.append({
            "sha": c.get("sha"),
            "message_head": (c.get("commit", {}).get("message") or "").splitlines()[0],
            "author_name": c.get("commit", {}).get("author", {}).get("name"),
            "author_email": c.get("commit", {}).get("author", {}).get("email"),
            "author_date": c.get("commit", {}).get("author", {}).get("date"),
            "parents": [p.get("sha") for p in c.get("parents", [])],
            "html_url": c.get("html_url")
        })
    return commits

def fetch_all_prs_with_commits(session, owner, repo_name, token):
    url = f"{GITHUB_API}/repos/{owner}/{repo_name}/pulls"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    raw_prs = paginate(session, url, headers, params_extra={"state":"all"})
    prs = []
    for pr in raw_prs:
        pr_number = pr.get("number")
        # fetch commits for PR (paginated)
        commits_url = f"{GITHUB_API}/repos/{owner}/{repo_name}/pulls/{pr_number}/commits"
        pr_commits_raw = paginate(session, commits_url, headers)
        pr_commits = [{"sha": c.get("sha"), "message_head": (c.get("commit", {}).get("message") or "").splitlines()[0]} for c in pr_commits_raw]
        prs.append({
            "number": pr_number,
            "title": pr.get("title"),
            "user": pr.get("user", {}).get("login") if pr.get("user") else None,
            "state": pr.get("state"),
            "created_at": pr.get("created_at"),
            "closed_at": pr.get("closed_at"),
            "merged_at": pr.get("merged_at"),
            "merge_commit_sha": pr.get("merge_commit_sha"),
            "html_url": pr.get("html_url"),
            "commits": pr_commits
        })
        time.sleep(0.08)
    return prs

def build_mapping_from_prs(prs):
    mapping = defaultdict(list)
    for pr in prs:
        for c in pr.get("commits", []):
            sha = c.get("sha")
            if sha:
                mapping[sha].append(pr["number"])
    return dict(mapping)

def map_remaining_commits(session, owner, repo_name, token, commits, mapping_existing):
    headers = {"Accept": "application/vnd.github.groot-preview+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    to_check = [c["sha"] for c in commits if c["sha"] not in mapping_existing]
    log.info("Commits needing mapping via /commits/{sha}/pulls : %d", len(to_check))
    extra_mapping = {}
    for i, sha in enumerate(to_check, start=1):
        url = f"{GITHUB_API}/repos/{owner}/{repo_name}/commits/{sha}/pulls"
        try:
            r = request_with_retry(session, url, headers=headers, params=None, max_retries=3)
        except Exception as e:
            log.warning("Failed to map commit %s: %s", sha[:7], e)
            extra_mapping[sha] = []
            continue
        pulls = r.json()
        extra_mapping[sha] = [p.get("number") for p in pulls] if pulls else []
        if i % 50 == 0:
            log.info("Mapped %d/%d commits", i, len(to_check))
        time.sleep(0.12)
    return extra_mapping

def write_json(outdir, fname, data):
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Wrote %s (entries: %d)", path, len(data) if isinstance(data, (list,dict)) else 0)

def main():
    token, repo = get_env()
    if not repo:
        log.error("REPO is not set.")
        sys.exit(2)
    owner, repo_name = repo.split("/")
    session = requests.Session()
    outdir = ensure_outdir("results")

    log.info("Fetching all commits (this paginates)...")
    commits = fetch_all_commits(session, owner, repo_name, token)
    log.info("Total commits fetched: %d", len(commits))

    log.info("Fetching all pull requests and their commits...")
    prs = fetch_all_prs_with_commits(session, owner, repo_name, token)
    log.info("Total PRs fetched: %d", len(prs))

    mapping_from_prs = build_mapping_from_prs(prs)
    log.info("Mapping from PRs covers %d unique commits", len(mapping_from_prs))

    # Map remaining commits using commit->pulls endpoint
    extra_map = map_remaining_commits(session, owner, repo_name, token, commits, mapping_from_prs)
    # merge mappings
    final_map = dict(mapping_from_prs)
    for k, v in extra_map.items():
        if k in final_map:
            # merge unique PR numbers
            existing = set(final_map[k])
            existing.update(v)
            final_map[k] = sorted(existing)
        else:
            final_map[k] = v

    # write outputs
    write_json(outdir, "commits.json", commits)
    write_json(outdir, "pull_requests.json", prs)
    write_json(outdir, "commit_to_prs.json", final_map)

    # Print short summary
    log.info("EXPORT COMPLETE: commits=%d prs=%d mappings=%d", len(commits), len(prs), len(final_map))

if __name__ == "__main__":
    main()
