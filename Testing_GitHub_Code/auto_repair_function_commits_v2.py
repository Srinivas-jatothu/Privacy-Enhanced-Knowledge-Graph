#!/usr/bin/env python3
"""
auto_repair_function_commits_v2.py

Aggressive auto-repair of missing entries in results/function_commits.json.

Usage examples:
  # test aggressive mode on first 50 missing functions
  python auto_repair_function_commits_v2.py --repo-dir "..\Ecommerce-Data-MLOps" --sample-only 50 --aggressive

  # full aggressive run (creates results/function_commits_repaired_v2.json)
  python auto_repair_function_commits_v2.py --repo-dir "..\Ecommerce-Data-MLOps" --aggressive

  # replace original mapping (creates backup)
  python auto_repair_function_commits_v2.py --repo-dir "..\Ecommerce-Data-MLOps" --aggressive --replace

Notes:
 - Save this file next to your other pipeline scripts (Testing_GitHub_Code).
 - Run from your Testing_GitHub_Code folder and set --repo-dir to the repo root that contains .git.
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
import math

# ------------------------
# Config / regex
# ------------------------
SHA_RE = re.compile(r"[0-9a-fA-F]{7,40}")
DEF_RE = re.compile(r"^\s*(async\s+def|def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
DEF_SEARCH_RE = re.compile(r"\b(def|async def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.IGNORECASE)
PY_EXT = ".py"

# ------------------------
# IO helpers
# ------------------------
def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

# ------------------------
# Git / blame helpers
# ------------------------
def run_git_blame(repo_dir: str, file_rel: str, start: int, end: int, follow: bool = False) -> Tuple[List[str], Optional[str]]:
    """
    Run git blame --line-porcelain -Lstart,end -- file_rel
    If follow=True, add --follow to the blame invocation (best-effort).
    Returns list of SHAs and optional error message.
    """
    cmd = ["git", "-C", repo_dir, "blame", "--line-porcelain", f"-L{start},{end}", "--", file_rel]
    if follow:
        # git blame doesn't accept --follow; but we can attempt blame on file path and fallback strategies
        # We'll still try blame as-is (some versions/contexts accept it). For rename detection we attempt heuristics separately.
        pass
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        text = out.decode("utf-8", errors="replace")
    except subprocess.CalledProcessError as e:
        return [], f"git_blame_failed: {e.output[:2000].decode('utf-8', errors='replace')}"
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

def run_git_blame_wholefile(repo_dir: str, file_rel: str, follow: bool = False) -> Tuple[List[str], Optional[str]]:
    # safe wrapper to blame the whole file (lines 1..N)
    path = os.path.join(repo_dir, file_rel)
    if not os.path.exists(path):
        return [], "file_not_found"
    # get total lines
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            n = sum(1 for _ in fh)
    except Exception as e:
        return [], f"read_file_failed: {e}"
    return run_git_blame(repo_dir, file_rel, 1, max(1, n), follow=follow)

def try_git_show_parents(repo_dir: str, sha: str) -> List[str]:
    # Return parent SHAs for a commit (best-effort)
    try:
        out = subprocess.check_output(["git", "-C", repo_dir, "show", "--no-patch", "--pretty=%P", sha], stderr=subprocess.DEVNULL)
        s = out.decode("utf-8", errors="replace").strip()
        if not s:
            return []
        return s.split()
    except Exception:
        return []

def try_git_log_find_renames(repo_dir: str, file_rel: str, max_lookback=200) -> List[str]:
    """
    Try to find past file paths via git log --name-only --follow -n <max_lookback> -- <file_rel>
    Return list of candidate historical file paths (unique).
    """
    try:
        out = subprocess.check_output(["git", "-C", repo_dir, "log", "--follow", "--name-only", "--pretty=format:", f"-n{max_lookback}", "--", file_rel], stderr=subprocess.DEVNULL)
        txt = out.decode("utf-8", errors="replace")
        lines = [l.strip() for l in txt.splitlines() if l.strip()]
        uniq = []
        for l in lines:
            if l not in uniq:
                uniq.append(l)
        return uniq
    except Exception:
        return []

# ------------------------
# FS / search helpers
# ------------------------
def file_exists_under(repo_dir: str, path: str) -> Optional[str]:
    # Resolve an attribute file path to a repo-relative existing path, returning unix-style rel path or None
    if not path:
        return None
    # if absolute
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
    # try removing leading slash
    if path.startswith("/") and os.path.exists(path[1:]):
        return os.path.relpath(path[1:], repo_dir).replace("\\","/")
    # try searching by basename (fast bail-out)
    base = os.path.basename(path)
    matches = []
    for root, dirs, files in os.walk(repo_dir):
        if base in files:
            matches.append(os.path.join(root, base))
            if len(matches) >= 4:
                break
    if len(matches) == 1:
        return os.path.relpath(matches[0], repo_dir).replace("\\", "/")
    return None

def find_def_candidates(repo_dir: str, shortname: str, max_candidates: int = 10) -> List[Tuple[str,int]]:
    candidates = []
    for root, dirs, files in os.walk(repo_dir):
        for fname in files:
            if not fname.endswith(PY_EXT):
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
    path = os.path.join(repo_dir, file_rel)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
            n = len(lines)
            for j in range(start_lineno, n):
                if DEF_RE.match(lines[j]):
                    return j
            return n
    except Exception:
        return start_lineno

def read_file_text(repo_dir: str, file_rel: str) -> str:
    try:
        with open(os.path.join(repo_dir, file_rel), "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except Exception:
        return ""

# ------------------------
# selection heuristics
# ------------------------
def choose_best_sha_from_blame(shas: List[str], prefer_recent: Dict[str, float]) -> Optional[str]:
    if not shas:
        return None
    ctr = Counter(shas)
    # sort by count desc then prefer recent (timestamp float)
    def keyfn(item):
        sha, cnt = item
        ts = prefer_recent.get(sha) or prefer_recent.get(sha[:7]) or 0
        return (-cnt, -ts)
    candidates = sorted(list(ctr.items()), key=keyfn)
    return candidates[0][0] if candidates else None

# ------------------------
# main aggressive repair logic
# ------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default="results/nodes.json")
    p.add_argument("--func-commits", default="results/function_commits.json")
    p.add_argument("--repo-dir", default=".")
    p.add_argument("--commits", default=None)
    p.add_argument("--out", default="results/function_commits_repaired_v2.json")
    p.add_argument("--debug-out", default="results/kg_check/function_commits_aggressive_debug.json")
    p.add_argument("--max-candidates", type=int, default=10)
    p.add_argument("--sample-only", type=int, default=0, help="test only first N missing functions (0=all)")
    p.add_argument("--replace", action="store_true", help="Replace original function_commits.json with repaired mapping (backup created)")
    p.add_argument("--aggressive", action="store_true", help="Enable aggressive heuristics (recommended)")
    p.add_argument("--replace-all", action="store_true", help="If set, attempt to re-evaluate existing mappings as well (not default)")
    args = p.parse_args()

    nodes = load_json(args.nodes)
    if nodes is None:
        print("nodes.json not found:", args.nodes); return 2
    func_commits = load_json(args.func_commits) or {}
    commits = load_json(args.commits) if args.commits else None

    prefer_recent = {}
    if commits and isinstance(commits, list):
        for c in commits:
            sha = c.get("sha") or c.get("id')") if False else c.get("sha") or c.get("id")
            date = None
            if c.get("commit") and isinstance(c["commit"].get("author"), dict):
                date = c["commit"]["author"].get("date")
            if not date:
                date = c.get("date") or c.get("author_date")
            if sha and date:
                try:
                    import datetime as _dt
                    dt = _dt.datetime.fromisoformat(date.replace("Z", "+00:00"))
                    prefer_recent[sha] = dt.timestamp()
                    prefer_recent[sha[:7]] = dt.timestamp()
                except Exception:
                    pass

    # collect functions list and missing ones
    functions = [n for n in nodes if n.get("type") == "Function"]
    missing = []
    to_process = []
    for n in functions:
        q = n.get("qualified_name") or (n.get("id") and n.get("id").split(":",1)[-1])
        already = func_commits.get(q) or func_commits.get(n.get("id"))
        if already and not args.replace_all:
            continue
        if (not already) or args.replace_all:
            to_process.append(n)
    total_missing = len(to_process)
    print(f"Functions total: {len(functions)}; functions to process: {total_missing}")

    repaired = 0
    debug = {"summary": {}, "processed": []}
    limit = args.sample_only if args.sample_only>0 else None
    processed = 0

    for node in to_process:
        if limit is not None and processed >= limit:
            break
        processed += 1
        qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
        nodeid = node.get("id")
        attrs = node.get("attrs") or {}
        entry = {"id": nodeid, "qualified_name": qname, "attempts": [], "result": None}
        # skip if mapping already exists and we are not replace_all
        if func_commits.get(qname) and not args.replace_all:
            entry["result"] = {"status":"exists_skipped"}
            debug["processed"].append(entry)
            continue

        # Strategy 1: Node-provided file + lineno
        file_attr = attrs.get("file") or attrs.get("path") or attrs.get("filename")
        lineno = attrs.get("lineno")
        end_lineno = attrs.get("end_lineno")
        candidate_used = None
        top_sha = None

        if file_attr and lineno:
            file_rel = file_exists_under(args.repo_dir, file_attr)
            if file_rel:
                start = int(lineno)
                end = int(end_lineno) if end_lineno else infer_end_lineno_for_candidate(args.repo_dir, file_rel, start)
                entry["attempts"].append({"strategy":"blame_from_node", "file": file_rel, "start": start, "end": end})
                shas, err = run_git_blame(args.repo_dir, file_rel, start, end, follow=False)
                if err or not shas:
                    # try whole-file blame (aggressive)
                    entry["attempts"][-1]["error"] = err
                    if args.aggressive:
                        entry["attempts"].append({"strategy":"blame_whole_file_fallback", "file": file_rel})
                        shas_whole, err2 = run_git_blame_wholefile(args.repo_dir, file_rel, follow=True)
                        if err2:
                            entry["attempts"][-1]["error"] = err2
                        else:
                            entry["attempts"][-1]["shas_sample"] = shas_whole[:10]
                            c = choose_best_sha_from_blame(shas_whole, prefer_recent)
                            if c:
                                top_sha = c
                                candidate_used = {"file": file_rel, "start":1, "end":len(shas_whole)}
                else:
                    entry["attempts"][-1]["shas_sample"] = shas[:10]
                    c = choose_best_sha_from_blame(shas, prefer_recent)
                    if c:
                        top_sha = c
                        candidate_used = {"file": file_rel, "start": start, "end": end}
            else:
                entry["attempts"].append({"strategy":"file_not_found_under_repo", "file_attr": file_attr})

        # Strategy 2: aggressive search by def name
        if not top_sha and args.aggressive:
            short = qname.split(".")[-1] if qname else None
            if short:
                cands = find_def_candidates(args.repo_dir, short, max_candidates=args.max_candidates)
                entry["attempts"].append({"strategy":"search_candidates", "count":len(cands), "candidates": cands[:10]})
                for (f_rel, startln) in cands:
                    endln = infer_end_lineno_for_candidate(args.repo_dir, f_rel, startln)
                    entry["attempts"].append({"strategy":"candidate_blame_try", "file": f_rel, "start": startln, "end": endln})
                    shas, err = run_git_blame(args.repo_dir, f_rel, startln, endln, follow=False)
                    if err or not shas:
                        entry["attempts"][-1]["error"] = err
                        # try blame whole file if aggressive
                        shas_whole, err2 = run_git_blame_wholefile(args.repo_dir, f_rel, follow=True)
                        if shas_whole:
                            entry["attempts"][-1]["shas_sample_whole"] = shas_whole[:10]
                            c = choose_best_sha_from_blame(shas_whole, prefer_recent)
                            if c:
                                top_sha = c
                                candidate_used = {"file": f_rel, "start": 1, "end": len(shas_whole)}
                                break
                        continue
                    else:
                        entry["attempts"][-1]["shas_sample"] = shas[:10]
                        c = choose_best_sha_from_blame(shas, prefer_recent)
                        if c:
                            top_sha = c
                            candidate_used = {"file": f_rel, "start": startln, "end": endln}
                            break

        # Strategy 3: try historical paths (renames) then blame whole file
        if not top_sha and args.aggressive and file_attr:
            # attempt to discover historical names
            hist = try_git_log_find_renames(args.repo_dir, file_attr, max_lookback=200)
            entry["attempts"].append({"strategy":"try_git_log_find_renames", "found": len(hist), "sample": hist[:5]})
            for h in hist:
                h_rel = h.replace("\\","/")
                shas_whole, err = run_git_blame_wholefile(args.repo_dir, h_rel, follow=True)
                if shas_whole:
                    entry["attempts"].append({"strategy":"blame_historical_file", "file": h_rel, "sample_shas": shas_whole[:10]})
                    c = choose_best_sha_from_blame(shas_whole, prefer_recent)
                    if c:
                        top_sha = c
                        candidate_used = {"file": h_rel, "start":1, "end":len(shas_whole)}
                        break

        # Strategy 4: use neighbors / adjacent functions (best-effort)
        if not top_sha and args.aggressive and file_attr:
            file_rel = file_exists_under(args.repo_dir, file_attr) or file_attr
            text = read_file_text(args.repo_dir, file_rel)
            # try to find functions in same file and use their top commits (if present in mapping)
            neighbor_qs = []
            for m in DEF_SEARCH_RE.finditer(text):
                name = m.group(2)
                # form probable qname by replacing last token
                neighbor_qs.append(name)
            # attempt to use any neighbor mapping present in func_commits to infer commit
            for neigh in neighbor_qs[:20]:
                # attempt qname variations — this is a heuristic
                cand_keys = [f"{qname.rsplit('.',1)[0]}.{neigh}" if "." in qname else neigh, neigh]
                found = None
                for k in cand_keys:
                    if k in func_commits:
                        # extract top commit sha from mapping structure if possible
                        v = func_commits[k]
                        # try top_commit -> sha
                        if isinstance(v, dict) and v.get("top_commit") and v["top_commit"].get("sha"):
                            found = v["top_commit"]["sha"]
                        else:
                            # maybe mapping is list of shas
                            if isinstance(v, list) and v:
                                if isinstance(v[0], str) and SHA_RE.match(v[0]):
                                    found = v[0]
                        if found:
                            entry["attempts"].append({"strategy":"neighbor_infer", "neighbor":k, "sha": found})
                            top_sha = found
                            candidate_used = {"method":"neighbor_infer","neighbor":k}
                            break
                if top_sha:
                    break

        # done: if we found a top_sha, record mapping; else record failure
        if top_sha:
            mapped = {
                "file": candidate_used.get("file") if isinstance(candidate_used, dict) and candidate_used.get("file") else (file_attr or None),
                "lineno": candidate_used.get("start") if isinstance(candidate_used, dict) and candidate_used.get("start") else (lineno or None),
                "end_lineno": candidate_used.get("end") if isinstance(candidate_used, dict) and candidate_used.get("end") else (end_lineno or None),
                "blame_summary": { top_sha: {"count_est": None} },
                "top_commit": {"sha": top_sha}
            }
            func_commits[qname] = mapped
            repaired += 1
            entry["result"] = {"status":"repaired", "sha": top_sha, "candidate": candidate_used}
        else:
            entry["result"] = {"status":"failed", "reason":"no_sha_found", "attempts_count": len(entry["attempts"])}

        debug["processed"].append(entry)

    debug["summary"] = {
        "functions_total": len(functions),
        "processed_count": len(debug["processed"]),
        "repaired_count": repaired,
        "remaining": len(functions) - repaired
    }
    # write outputs
    write_json(args.out, func_commits)
    write_json(args.debug_out, debug)
    print(f"[INFO] Repaired: {repaired}; out: {args.out}; debug: {args.debug_out}")

    # optionally replace original file
    if args.replace:
        bak = args.func_commits + f".bak.{int(datetime.datetime.now().timestamp())}"
        try:
            write_json(bak, load_json(args.func_commits) or {})
            write_json(args.func_commits, func_commits)
            print(f"[INFO] Replaced original {args.func_commits} (backup at {bak})")
        except Exception as e:
            print("[WARN] Failed to replace original:", e)

if __name__ == "__main__":
    main()
