#!/usr/bin/env python3
"""
auto_repair_function_commits.py

Auto-repair missing entries in results/function_commits.json by:
 - using node file/lineno + git blame, or
 - searching repository for function definition candidates and running git blame for those ranges.

Usage examples:
  # dry-run, create repaired file but don't replace original
  python auto_repair_function_commits.py --nodes results/nodes.json --func-commits results/function_commits.json --repo-dir .

  # write and replace original (backup created)
  python auto_repair_function_commits.py --nodes results/nodes.json --func-commits results/function_commits.json --repo-dir . --replace

  # test on first 50 missing functions
  python auto_repair_function_commits.py --sample-only 50 --repo-dir .

Important: run this from a machine that has the repo cloned and git available.
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple
import datetime

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SHA_RE = re.compile(r"[0-9a-fA-F]{7,40}")
DEF_RE = re.compile(r"^\s*(async\s+def|def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\b")

def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def run_git_blame(repo_dir: str, file_rel: str, start: int, end: int) -> Tuple[List[str], Optional[str]]:
    """
    Run git blame --line-porcelain -Lstart,end -- file_rel
    Return (list_of_shas_in_order_of_appearance, error_message_or_None)
    """
    cmd = ["git", "-C", repo_dir, "blame", "--line-porcelain", f"-L{start},{end}", "--", file_rel]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        text = out.decode("utf-8", errors="replace")
    except subprocess.CalledProcessError as e:
        return [], f"git_blame_failed: {e.output[:1000].decode('utf-8', errors='replace')}"
    shas = []
    for line in text.splitlines():
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 1:
            sha = parts[0]
            if SHA_RE.match(sha):
                shas.append(sha)
    return shas, None

def file_exists_under(repo_dir: str, path: str) -> Optional[str]:
    """
    Try to resolve path to a repo-relative path that exists.
    Returns relative path (unix-style) or None.
    """
    if os.path.isabs(path):
        if os.path.exists(path):
            try:
                rel = os.path.relpath(path, repo_dir)
                if not rel.startswith(".."):
                    return rel.replace("\\", "/")
            except Exception:
                pass
        return None
    cand = os.path.join(repo_dir, path)
    if os.path.exists(cand):
        return os.path.relpath(cand, repo_dir).replace("\\", "/")
    # try searching by basename (only first matching file)
    base = os.path.basename(path)
    matches = []
    for root, dirs, files in os.walk(repo_dir):
        if base in files:
            matches.append(os.path.join(root, base))
            if len(matches) >= 5:
                break
    if len(matches) == 1:
        return os.path.relpath(matches[0], repo_dir).replace("\\", "/")
    return None

def find_def_candidates(repo_dir: str, shortname: str, max_candidates: int = 5) -> List[Tuple[str,int]]:
    """
    Search for definitions of shortname in repo. Returns list of (file_rel, lineno).
    Only supports simple text scan for `def <name>(` and `class <name>`.
    """
    candidates = []
    for root, dirs, files in os.walk(repo_dir):
        for fname in files:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(root, fname)
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh, start=1):
                        m = DEF_RE.match(line)
                        if m and m.group(2) == shortname:
                            rel = os.path.relpath(path, repo_dir).replace("\\","/")
                            candidates.append((rel, i))
                            if len(candidates) >= max_candidates:
                                return candidates
            except Exception:
                continue
    return candidates

def infer_end_lineno_for_candidate(repo_dir: str, file_rel: str, start_lineno: int) -> int:
    """
    Infer end line for a candidate function: naive approach -
    scan file from start_lineno+1 until next def/class or EOF.
    """
    path = os.path.join(repo_dir, file_rel)
    end = start_lineno
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
            n = len(lines)
            for j in range(start_lineno, n):
                if DEF_RE.match(lines[j]):
                    end = j  # next def starts at j+1, so end is j
                    return end
            return n
    except Exception:
        return start_lineno

def choose_best_sha_from_blame(shas: List[str], commit_meta_index: Dict[str, datetime.datetime]) -> Optional[str]:
    """
    Choose best commit SHA from list by frequency, then by latest date (if available).
    """
    if not shas:
        return None
    ctr = Counter(shas)
    candidates = list(ctr.items())  # (sha, count)
    # sort by count desc, date desc
    def keyfn(item):
        sha, cnt = item
        dt = commit_meta_index.get(sha) or commit_meta_index.get(sha[:7])
        ts = dt.timestamp() if dt else 0
        return (-cnt, -ts)
    candidates.sort(key=keyfn)
    return candidates[0][0] if candidates else None

# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default="results/nodes.json")
    p.add_argument("--func-commits", default="results/function_commits.json")
    p.add_argument("--repo-dir", default=".")
    p.add_argument("--commits", default=None, help="optional commits.json to attach dates")
    p.add_argument("--out", default="results/function_commits_repaired.json")
    p.add_argument("--debug-out", default="results/kg_check/function_commits_auto_repair_debug.json")
    p.add_argument("--max-candidates", type=int, default=5)
    p.add_argument("--sample-only", type=int, default=0, help="process only first N missing functions (0 = all)")
    p.add_argument("--replace", action="store_true", help="Replace original function_commits.json (backed up)")
    p.add_argument("--interactive", action="store_true", help="Prompt on ambiguous matches (not recommended for large runs)")
    args = p.parse_args()

    nodes = load_json(args.nodes)
    if nodes is None:
        print("nodes.json not found:", args.nodes); return 2
    func_commits = load_json(args.func_commits) or {}
    commits = load_json(args.commits) if args.commits else None

    # build commit meta index if commits.json provided
    commit_meta_idx = {}
    if commits and isinstance(commits, list):
        for c in commits:
            sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
            date = None
            if c.get("commit") and isinstance(c["commit"].get("author"), dict):
                date = c["commit"]["author"].get("date")
            if not date:
                date = c.get("date") or c.get("author_date")
            if sha and date:
                try:
                    dt = datetime.datetime.fromisoformat(date.replace("Z", "+00:00"))
                    commit_meta_idx[sha] = dt
                    commit_meta_idx[sha[:7]] = dt
                except Exception:
                    pass

    # identify missing functions
    functions = [n for n in nodes if n.get("type") == "Function"]
    missing = [n for n in functions if not (func_commits.get(n.get("qualified_name")) or func_commits.get(n.get("id")))]
    total_missing = len(missing)
    print(f"Total functions: {len(functions)}; missing mapping: {total_missing}")

    repaired_count = 0
    debug = {"processed": [], "summary": {}}
    limit = args.sample_only if args.sample_only > 0 else None
    processed = 0

    for node in missing:
        if limit is not None and processed >= limit:
            break
        processed += 1

        qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
        nodeid = node.get("id")
        attrs = node.get("attrs") or {}
        entry_debug = {"id": nodeid, "qualified_name": qname, "attempts": []}
        # 1) try node file + lineno if present
        file_attr = attrs.get("file") or attrs.get("path") or attrs.get("filename")
        lineno = attrs.get("lineno")
        end_lineno = attrs.get("end_lineno")
        used_candidate = None
        top_sha = None

        if file_attr and lineno:
            file_rel = file_exists_under(args.repo_dir, file_attr)
            if file_rel:
                start = int(lineno)
                end = int(end_lineno) if end_lineno else infer_end_lineno_for_candidate(args.repo_dir, file_rel, start)
                entry_debug["attempts"].append({"strategy": "blame_from_node", "file": file_rel, "start": start, "end": end})
                shas, err = run_git_blame(args.repo_dir, file_rel, start, end)
                if err:
                    entry_debug["attempts"][-1]["error"] = err
                else:
                    entry_debug["attempts"][-1]["shas_sample"] = shas[:10]
                    chosen = choose_best_sha_from_blame(shas, commit_meta_idx)
                    if chosen:
                        top_sha = chosen
                        used_candidate = {"file": file_rel, "start": start, "end": end, "method": "blame_from_node"}
        # 2) fallback: search for def <shortname>
        if not top_sha:
            short = qname.split(".")[-1] if qname else None
            if short:
                cands = find_def_candidates(args.repo_dir, short, max_candidates=args.max_candidates)
                entry_debug["attempts"].append({"strategy": "search_candidates", "candidates_found": len(cands), "candidates": cands[:10]})
                for (file_rel, startln) in cands:
                    endln = infer_end_lineno_for_candidate(args.repo_dir, file_rel, startln)
                    shas, err = run_git_blame(args.repo_dir, file_rel, startln, endln)
                    attempt = {"file":file_rel, "start":startln, "end":endln}
                    if err:
                        attempt["error"] = err
                        entry_debug["attempts"].append(attempt)
                        continue
                    attempt["shas_sample"] = shas[:10]
                    entry_debug["attempts"].append(attempt)
                    chosen = choose_best_sha_from_blame(shas, commit_meta_idx)
                    if chosen:
                        top_sha = chosen
                        used_candidate = {"file": file_rel, "start": startln, "end": endln, "method": "search_candidate"}
                        break
            else:
                entry_debug["attempts"].append({"strategy":"no_shortname_to_search"})
        # 3) if interactive and ambiguous, prompt (optional)
        if args.interactive and top_sha and len(entry_debug["attempts"])>1:
            print("Function:", qname)
            print("Candidates tried:", entry_debug["attempts"])
            ans = input("Accept top_sha %s? [Y/n] " % (top_sha,))
            if ans.strip().lower().startswith("n"):
                top_sha = None
        # 4) record result
        if top_sha:
            # attach a structured mapping entry similar to your existing shape
            mapped = {
                "file": used_candidate["file"],
                "lineno": used_candidate["start"],
                "end_lineno": used_candidate["end"],
                "blame_summary": { top_sha: { "count_est": None } },
                "top_commit": { "sha": top_sha }
            }
            func_commits[qname] = mapped
            repaired_count += 1
            entry_debug["result"] = {"status":"repaired", "sha": top_sha, "candidate": used_candidate}
        else:
            entry_debug["result"] = {"status":"failed", "reason":"no_candidate_or_no_blame"}
        debug["processed"].append(entry_debug)

    # write outputs
    write_json(args.out, func_commits)
    write_json(args.debug_out, {"summary": {"total_missing": total_missing, "repaired": repaired_count, "remaining": total_missing - repaired_count}, "processed": debug["processed"]})
    print(f"Repaired: {repaired_count}; remaining: {total_missing - repaired_count}")
    if args.replace:
        backup = args.func_commits + f".bak.{int(datetime.datetime.now().timestamp())}"
        write_json(backup, load_json(args.func_commits) or {})
        write_json(args.func_commits, func_commits)
        print(f"Replaced original {args.func_commits} (backup at {backup})")

if __name__ == "__main__":
    main()
