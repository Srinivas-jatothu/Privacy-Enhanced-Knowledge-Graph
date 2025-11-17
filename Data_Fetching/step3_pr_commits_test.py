# step3_pr_commits_test.py
import os
import sys
import time
import json
import logging
import requests
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("step3-pr-commits")

def env():
    return os.getenv("GITHUB_TOKEN"), os.getenv("REPO")

def ensure_outdir(path="results"):
    os.makedirs(path, exist_ok=True)
    return path

def fetch_pr_commits(token, owner, repo_name, pr_number):
    url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls/{pr_number}/commits"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, params={"per_page": 100}, timeout=30)
    log.debug("PR commits HTTP %s  URL: %s", r.status_code, r.url)
    r.raise_for_status()
    return r.json()

def commits_to_prs_mapping(token, owner, repo_name, commit_shas):
    headers = {"Accept": "application/vnd.github.groot-preview+json"}  # preview for assoc pulls
    if token:
        headers["Authorization"] = f"token {token}"
    mapping = {}
    for sha in commit_shas:
        url = f"https://api.github.com/repos/{owner}/{repo_name}/commits/{sha}/pulls"
        r = requests.get(url, headers=headers, timeout=30)
        log.debug("Mapping [%s] HTTP %s", sha[:7], r.status_code)
        if r.status_code == 404:
            mapping[sha] = []
            continue
        r.raise_for_status()
        pulls = r.json()
        mapping[sha] = [p.get("number") for p in pulls] if pulls else []
        # tiny sleep to be polite
        time.sleep(0.1)
    return mapping

def main():
    token, repo = env()
    if not repo:
        log.error("REPO not set. Aborting.")
        sys.exit(2)
    owner, repo_name = repo.split("/")
    outdir = ensure_outdir("results")

    # Load PR sample from previous run if present
    sample_path = os.path.join(outdir, "prs_sample.json")
    if os.path.exists(sample_path):
        log.info("Loading PR sample from %s", sample_path)
        with open(sample_path, "r", encoding="utf-8") as f:
            sample = json.load(f)
        prs = sample.get("sample_first") or []
    else:
        log.info("No prs_sample.json found; will fetch first page of PRs directly.")
        # fetch first page of PRs
        url = f"https://api.github.com/repos/{owner}/{repo_name}/pulls"
        headers = {"Accept": "application/vnd.github.v3+json"}
        if token:
            headers["Authorization"] = f"token {token}"
        r = requests.get(url, headers=headers, params={"state": "all", "per_page": 10, "page": 1}, timeout=30)
        r.raise_for_status()
        prs = r.json()

    # We'll process only first N PRs to keep test quick
    N = 5
    prs = prs[:N]
    log.info("Processing %d PRs for commit listing test", len(prs))

    pr_commits_out = []
    commit_shas_to_map = []
    for pr in prs:
        pr_num = pr.get("number")
        log.info("Fetching commits for PR #%s  title=%r", pr_num, pr.get("title"))
        commits = fetch_pr_commits(token, owner, repo_name, pr_num)
        log.info("  PR #%s returned %d commits", pr_num, len(commits))
        # keep a lightweight record
        recorded = {"number": pr_num, "title": pr.get("title"), "commits_count": len(commits), "commits": []}
        for c in commits:
            sha = c.get("sha")
            recorded["commits"].append({"sha": sha, "message": c.get("commit", {}).get("message")})
            # collect a few shas to check mapping (limit to avoid too many calls)
            if len(commit_shas_to_map) < 10:
                commit_shas_to_map.append(sha)
        pr_commits_out.append(recorded)
        time.sleep(0.15)

    log.info("Collected commits for %d PRs; will map %d commits -> PRs", len(pr_commits_out), len(commit_shas_to_map))
    mapping = commits_to_prs_mapping(token, owner, repo_name, commit_shas_to_map)

    out = {"repo": repo, "pr_commits_sample": pr_commits_out, "commit_to_prs_sample": mapping}
    out_file = os.path.join(outdir, "prs_commits_sample.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    log.info("Wrote sample to %s", out_file)
    log.info("Done.")

if __name__ == "__main__":
    main()
