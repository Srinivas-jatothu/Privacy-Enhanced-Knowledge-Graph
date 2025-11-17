"""
github_metadata_exporter.py

Usage:
  python github_metadata_exporter.py --repo OWNER/REPO [--token TOKEN] [--clone-path /path/to/clone] [--outdir out]

Outputs:
  - out/commits.json
  - out/pull_requests.json
  - out/commit_to_prs.json

Notes:
  - If --clone-path is provided and points to a git clone, the script will read commits locally (using GitPython).
  - Otherwise, commits are fetched from GitHub REST API (paginated).
  - Requires a GitHub token for private repos or to increase rate limits; token can be provided with --token or as env var GITHUB_TOKEN.
"""

import os
import sys
import json
import argparse
import requests
from urllib.parse import urljoin
from time import sleep
from tqdm import tqdm

# Try to import GitPython (optional)
try:
    from git import Repo, GitCommandError
    HAVE_GITPY = True
except Exception:
    HAVE_GITPY = False

# Constants
GITHUB_API = "https://api.github.com/"
PER_PAGE = 100
HEADERS_BASE = {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "github-metadata-exporter/1.0"
}
# This Accept header was historically required for the "List pull requests associated with a commit" endpoint.
ASSOC_PULLS_ACCEPT = "application/vnd.github.groot-preview+json"

def get_auth_header(token):
    h = HEADERS_BASE.copy()
    if token:
        h["Authorization"] = f"token {token}"
    return h

def paginate_get(session, url, params=None, headers=None):
    """Generic paginator for GitHub REST endpoints returning arrays."""
    params = params.copy() if params else {}
    params.setdefault("per_page", PER_PAGE)
    page = 1
    items = []
    while True:
        params["page"] = page
        r = session.get(url, params=params, headers=headers)
        if r.status_code == 403:
            print("RATE LIMIT OR ACCESS DENIED:", r.status_code, r.text[:400], file=sys.stderr)
            # Show rate limit headers
            if "X-RateLimit-Remaining" in r.headers:
                print("Rate limit remaining:", r.headers.get("X-RateLimit-Remaining"), "reset:", r.headers.get("X-RateLimit-Reset"))
            r.raise_for_status()
        r.raise_for_status()
        page_items = r.json()
        if not isinstance(page_items, list):
            # Some endpoints return dicts for single objects
            return page_items
        items.extend(page_items)
        # check if we have another page
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        page += 1
    return items

def fetch_commits_api(session, owner, repo, token):
    """Fetch commits via REST API: GET /repos/{owner}/{repo}/commits (paginated)"""
    url = urljoin(GITHUB_API, f"repos/{owner}/{repo}/commits")
    headers = get_auth_header(token)
    commits = paginate_get(session, url, params={"sha": "HEAD"}, headers=headers)
    # normalize fields we want
    out = []
    for c in commits:
        commit_info = {
            "sha": c.get("sha"),
            "message": c.get("commit", {}).get("message"),
            "author_name": c.get("commit", {}).get("author", {}).get("name"),
            "author_email": c.get("commit", {}).get("author", {}).get("email"),
            "author_date": c.get("commit", {}).get("author", {}).get("date"),
            "committer_name": c.get("commit", {}).get("committer", {}).get("name"),
            "committer_email": c.get("commit", {}).get("committer", {}).get("email"),
            "committer_date": c.get("commit", {}).get("committer", {}).get("date"),
            "html_url": c.get("html_url"),
            "parents": [p.get("sha") for p in c.get("parents", [])],
            "github_author_login": c.get("author", {}).get("login") if c.get("author") else None
        }
        out.append(commit_info)
    return out

def fetch_commits_local(clone_path):
    """Read commits from a local git clone using GitPython."""
    if not HAVE_GITPY:
        raise RuntimeError("GitPython not installed; install with 'pip install GitPython' or omit --clone-path to use API.")
    repo = Repo(clone_path)
    commits = []
    # iterate all reachable commits
    try:
        for commit in tqdm(list(repo.iter_commits(all=True)), desc="Reading local commits"):
            c = {
                "sha": commit.hexsha,
                "message": commit.message,
                "author_name": commit.author.name,
                "author_email": commit.author.email,
                "author_date": commit.authored_datetime.isoformat() if commit.authored_datetime else None,
                "committer_name": commit.committer.name,
                "committer_email": commit.committer.email,
                "committer_date": commit.committed_datetime.isoformat() if commit.committed_datetime else None,
                "parents": [p.hexsha for p in commit.parents],
                "stats": {
                    "insertions": commit.stats.total.get("insertions") if hasattr(commit, "stats") else None,
                    "deletions": commit.stats.total.get("deletions") if hasattr(commit, "stats") else None,
                },
            }
            commits.append(c)
    except GitCommandError as e:
        raise RuntimeError("Error reading local repo: " + str(e))
    # Remove duplicates (iter_commits(all=True) may list reachable commits multiple times)
    uniq = {c["sha"]: c for c in commits}
    return list(uniq.values())

def fetch_pull_requests(session, owner, repo, token):
    """List all pull requests (all states) and for each PR fetch its commits."""
    url = urljoin(GITHUB_API, f"repos/{owner}/{repo}/pulls")
    headers = get_auth_header(token)
    all_prs = paginate_get(session, url, params={"state": "all", "per_page": PER_PAGE}, headers=headers)
    out = []
    for pr in tqdm(all_prs, desc="Processing PRs"):
        pr_number = pr.get("number")
        # fetch commits for this PR
        commits_url = urljoin(GITHUB_API, f"repos/{owner}/{repo}/pulls/{pr_number}/commits")
        pr_commits = paginate_get(session, commits_url, params={"per_page": PER_PAGE}, headers=headers)
        commits_list = []
        for c in pr_commits:
            commits_list.append({
                "sha": c.get("sha"),
                "message": c.get("commit", {}).get("message"),
                "author_name": c.get("commit", {}).get("author", {}).get("name"),
                "author_email": c.get("commit", {}).get("author", {}).get("email"),
                "author_date": c.get("commit", {}).get("author", {}).get("date")
            })
        pr_obj = {
            "number": pr_number,
            "title": pr.get("title"),
            "body": pr.get("body"),
            "user": pr.get("user", {}).get("login") if pr.get("user") else None,
            "state": pr.get("state"),
            "created_at": pr.get("created_at"),
            "closed_at": pr.get("closed_at"),
            "merged_at": pr.get("merged_at"),
            "merge_commit_sha": pr.get("merge_commit_sha"),
            "html_url": pr.get("html_url"),
            "commits": commits_list
        }
        out.append(pr_obj)
    return out

def fetch_commit_to_prs(session, owner, repo, commits, token):
    """For each commit SHA, request the associated PRs via REST endpoint:
       GET /repos/{owner}/{repo}/commits/{sha}/pulls
       Note: historically required the 'groot-preview' accept header; include that.
    """
    mapping = {}
    headers = get_auth_header(token)
    # add preview Accept for the endpoint
    headers["Accept"] = ASSOC_PULLS_ACCEPT
    for c in tqdm(commits, desc="Mapping commits -> PRs"):
        sha = c.get("sha")
        if not sha:
            continue
        url = urljoin(GITHUB_API, f"repos/{owner}/{repo}/commits/{sha}/pulls")
        r = session.get(url, headers=headers)
        if r.status_code == 404:
            # commit not found on remote (e.g., local-only commit). Continue
            mapping[sha] = []
            continue
        if r.status_code == 403:
            print("Access/Rate limit while fetching commit->PRs", r.status_code, r.text[:300], file=sys.stderr)
            r.raise_for_status()
        r.raise_for_status()
        pulls = r.json()
        # pulls is list of PR objects (summary)
        mapping[sha] = [p.get("number") for p in pulls] if pulls else []
    return mapping

def write_json(outdir, fname, data):
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("Wrote:", path)

def main():
    p = argparse.ArgumentParser(description="Export GitHub repo commits, PRs, and mappings.")
    p.add_argument("--repo", required=True, help="owner/repo (e.g., octocat/Hello-World)")
    p.add_argument("--token", help="GitHub token (or set GITHUB_TOKEN env var)")
    p.add_argument("--clone-path", help="Path to a local git clone (optional). If provided, commit data will be read locally.")
    p.add_argument("--outdir", default="out", help="Output directory")
    p.add_argument("--skip-commit-pr-mapping", action="store_true", help="Skip commit->PR mapping calls")
    args = p.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("Warning: No GITHUB_TOKEN provided. Unauthenticated requests are heavily rate-limited and cannot access private repos.", file=sys.stderr)

    owner_repo = args.repo.strip().split("/")
    if len(owner_repo) != 2:
        print("repo must be in owner/repo format", file=sys.stderr)
        sys.exit(2)
    owner, repo = owner_repo

    session = requests.Session()

    # 1) Collect commits
    if args.clone_path:
        if not HAVE_GITPY:
            print("GitPython not installed; cannot read local clone. Install GitPython or omit --clone-path.", file=sys.stderr)
            sys.exit(1)
        if not os.path.isdir(args.clone_path):
            print("Provided clone path does not exist:", args.clone_path, file=sys.stderr)
            sys.exit(1)
        print("Reading commits from local clone:", args.clone_path)
        commits = fetch_commits_local(args.clone_path)
    else:
        print("Fetching commits via GitHub API (this may be slow for large repos).")
        commits = fetch_commits_api(session, owner, repo, token)

    print(f"Collected {len(commits)} commits.")

    # 2) Collect pull requests and their commits
    print("Fetching pull requests and their commits via GitHub API...")
    prs = fetch_pull_requests(session, owner, repo, token)
    print(f"Collected {len(prs)} pull requests.")

    # 3) Map commits -> PRs (optional)
    commit_to_prs = {}
    if not args.skip_commit_pr_mapping:
        print("Mapping commits -> pull requests (uses /commits/{sha}/pulls endpoint).")
        commit_to_prs = fetch_commit_to_prs(session, owner, repo, commits, token)
    else:
        print("Skipping commit->PR mapping as requested.")
        # We can also derive mapping from PRs side:
        for pr in prs:
            for c in pr.get("commits", []):
                sha = c.get("sha")
                if sha:
                    commit_to_prs.setdefault(sha, []).append(pr["number"])

    # 4) Save outputs
    outdir = args.outdir
    write_json(outdir, "commits.json", commits)
    write_json(outdir, "pull_requests.json", prs)
    write_json(outdir, "commit_to_prs.json", commit_to_prs)

    print("Done.")

if __name__ == "__main__":
    main()
