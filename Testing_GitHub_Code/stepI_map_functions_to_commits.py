#!/usr/bin/env python3
"""
stepI_map_functions_to_commits.py

Purpose:
  - Map each function/class/method (from symbol_table.json or discovered automatically)
    to the Git commits that last modified their source lines.
  - Uses `git blame -L <start>,<end> --line-porcelain <file>` to obtain per-line commit metadata,
    aggregates per-function, and selects the most recent / most-frequent commit(s).
  - Produces a structured JSON suitable for KG ingestion:
      results/function_commits.json
    Format:
      {
        "<qualified_symbol>": {
           "file": "rel/path.py",
           "lineno": start,
           "end_lineno": end,
           "blame_summary": {
               "<commit_sha>": {
                   "count": N,
                   "first_line": X,
                   "last_line": Y,
                   "author": "Name",
                   "author_email": "email",
                   "author_time": "ISO8601",
                   "summary": "commit message line",
               },
               ...
           },
           "top_commit": {
               "sha": "<sha>",
               "count": N,
               "author": "...",
               "author_email": "...",
               "author_time": "...",
               "summary": "..."
           }
        },
        ...
      }

Notes:
  - Requires `git` available on PATH and a local clone of the repo with full history.
  - Reads `results/symbol_table.json` (created by stepH_full_call_graph.py). If not found,
    it will scan the repository for Python files and build a simple symbol table on-the-fly.
  - Robust to parse errors and missing end_lineno: if end_lineno missing, uses heuristics:
      * For functions: looks up until next top-level def/class or end of file.
  - Outputs logs to stdout and a file: results/stepI_map_functions_to_commits.log

Usage:
  python stepI_map_functions_to_commits.py --repo-dir /path/to/repo --out-dir ./results

Author: Automated pipeline (professional script)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import pathlib
import shlex
from typing import Dict, Any, List, Optional, Tuple
import ast
import datetime

# -------------------------
# Defaults / paths
# -------------------------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), "results")
SYMBOL_TABLE_PATH = os.path.join(DEFAULT_OUT_DIR, "symbol_table.json")  # may be overridden
LOG_FILENAME = "stepI_map_functions_to_commits.log"
OUTPUT_FILENAME = "function_commits.json"

# -------------------------
# Logging
# -------------------------
def setup_logging(out_dir: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(out_dir, exist_ok=True)
    log_path = os.path.join(out_dir, LOG_FILENAME)
    logger = logging.getLogger("func-commits")
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger

# -------------------------
# Git helpers
# -------------------------
def git_available() -> bool:
    try:
        subprocess.run(["git", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except Exception:
        return False

def run_git_blame(repo_dir: str, relpath: str, start: int, end: int) -> Optional[str]:
    """
    Run `git blame -L start,end --line-porcelain -- <file>` and return raw output.
    """
    # Ensure file path is relative to repo and safe
    cmd = ["git", "-C", repo_dir, "blame", "-L", f"{start},{end}", "--line-porcelain", "--", relpath]
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        return proc.stdout
    except subprocess.CalledProcessError as e:
        return None

def parse_blame_porcelain(blame_text: str) -> List[Dict[str, Any]]:
    """
    Parse git blame porcelain output into a list of per-line metadata dictionaries:
      [{ "sha": "...", "lineno": <int>, "author": "...", "author_mail": "...", "author_time": <iso>, "summary": "..."}]
    The porcelain repeats a header block before each source line; we'll group by line.
    """
    lines = blame_text.splitlines()
    results = []
    current = {}
    for ln in lines:
        if not ln:
            # blank separates header and source, the next line is the source content - ignore content
            continue
        # porcelain header lines start with key or commit-sha line
        parts = ln.split(" ", 1)
        key = parts[0]
        rest = parts[1] if len(parts) > 1 else ""
        if key and len(key) == 40 and " " in ln:
            # first line of a header: "<sha> <orig-line> <final-line> <num>"
            sha = parts[0]
            current = {"sha": sha}
            # we can extract final-line/parsing if needed from rest
            # move on
        elif key == "author":
            current["author"] = rest
        elif key == "author-mail":
            current["author_mail"] = rest
        elif key == "author-time":
            try:
                epoch = int(rest.strip())
                current["author_time"] = datetime.datetime.utcfromtimestamp(epoch).isoformat() + "Z"
            except Exception:
                current["author_time"] = rest
        elif key == "summary":
            current["summary"] = rest
        elif key == "filename":
            current["filename"] = rest
        elif key.startswith("\t"):
            # source code line (ignored)
            # finalize current entry if sha exists
            if current:
                results.append(current)
            current = {}
        else:
            # other keys: committer, committer-mail, etc - capture if present
            if key:
                current[key] = rest
    # sometimes last entry may be pending
    if current:
        results.append(current)
    return results

# -------------------------
# AST helpers to compute fallback end_lineno
# -------------------------
def compute_function_ranges_from_file(abs_path: str) -> List[Dict[str, Any]]:
    """
    Parse a python file and return top-level function/class definitions with start/end lineno.
    Used when symbol_table.json missing or incomplete.
    """
    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
            src = f.read()
        tree = ast.parse(src)
    except Exception:
        return []
    results = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            results.append({
                "qualified_name": None,  # caller must assign
                "type": "function",
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None)
            })
        elif isinstance(node, ast.ClassDef):
            # include class and its methods
            results.append({
                "qualified_name": None,
                "type": "class",
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None)
            })
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    results.append({
                        "qualified_name": None,
                        "type": "method",
                        "lineno": getattr(child, "lineno", None),
                        "end_lineno": getattr(child, "end_lineno", None)
                    })
    return results

# -------------------------
# Main logic
# -------------------------
def load_symbol_table(path: str, repo_dir: str, logger: logging.Logger) -> Dict[str, Dict[str, Any]]:
    """
    Load results/symbol_table.json if present. If missing, attempt a fallback scanning.
    Expected symbol table format: canonical_name -> { type, module, qualified_name, file, lineno, end_lineno }
    """
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                tbl = json.load(f)
            logger.info("Loaded symbol_table with %d entries from %s", len(tbl), path)
            return tbl
        except Exception as e:
            logger.warning("Failed to load symbol_table.json (%s): %s", path, e)

    # fallback: scan repo and build simple symbol table
    logger.warning("symbol_table.json not found or unreadable; building fallback symbol table by scanning files (best-effort).")
    fallback = {}
    repo = pathlib.Path(repo_dir)
    for p in repo.rglob("*.py"):
        rel = str(p.relative_to(repo)).replace("\\", "/")
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                src = f.read()
            tree = ast.parse(src)
        except Exception:
            continue
        module = rel[:-3].replace("/", ".") if rel.endswith(".py") else rel.replace("/", ".")
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qn = f"{module}.{node.name}"
                fallback[qn] = {"type": "function", "module": module, "qualified_name": qn, "file": rel, "lineno": getattr(node, "lineno", None), "end_lineno": getattr(node, "end_lineno", None)}
            elif isinstance(node, ast.ClassDef):
                cq = f"{module}.{node.name}"
                fallback[cq] = {"type": "class", "module": module, "qualified_name": cq, "file": rel, "lineno": getattr(node, "lineno", None), "end_lineno": getattr(node, "end_lineno", None)}
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        mq = f"{cq}.{child.name}"
                        fallback[mq] = {"type": "method", "module": module, "qualified_name": mq, "file": rel, "lineno": getattr(child, "lineno", None), "end_lineno": getattr(child, "end_lineno", None)}
    logger.info("Fallback symbol table built with %d entries", len(fallback))
    return fallback

def determine_end_lineno_if_missing(symbol_info: Dict[str, Any], abs_file: str, logger: logging.Logger) -> Tuple[Optional[int], Optional[int]]:
    """
    If symbol_info lacks end_lineno, attempt to compute end by scanning AST for next sibling def/class.
    Returns (start, end)
    """
    start = symbol_info.get("lineno")
    end = symbol_info.get("end_lineno")
    if start and end:
        return start, end
    # parse file and find the smallest enclosing node range after start
    try:
        with open(abs_file, "r", encoding="utf-8", errors="replace") as f:
            src = f.read()
        tree = ast.parse(src)
    except Exception as e:
        logger.debug("Failed to parse file for end_lineno computation: %s", e)
        return start, end

    # collect candidate ranges (lineno, end_lineno)
    ranges = []
    for node in ast.walk(tree):
        if hasattr(node, "lineno") and hasattr(node, "end_lineno"):
            ranges.append((getattr(node, "lineno", None), getattr(node, "end_lineno", None)))
    # find the smallest end >= start
    candidates = [r for r in ranges if r[0] and r[1] and r[0] > start]
    if candidates:
        # pick the minimum start among those and set end to start-1
        next_start = min(c[0] for c in candidates)
        computed_end = next_start - 1
        return start, computed_end
    # fallback: end of file
    lines = src.splitlines()
    return start, len(lines) if lines else start

def aggregate_blame_entries(blame_lines_meta: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Given list of parsed blame entries (each is a small dict with sha, author, author_mail, author_time, summary),
    aggregate by commit sha to produce counts and first/last line indexes (approx).
    """
    agg: Dict[str, Dict[str, Any]] = {}
    for entry in blame_lines_meta:
        sha = entry.get("sha")
        if not sha:
            continue
        if sha not in agg:
            agg[sha] = {
                "count": 0,
                "author": entry.get("author"),
                "author_email": entry.get("author_mail"),
                "author_time": entry.get("author_time"),
                "summary": entry.get("summary"),
                "first_seen_index": None,
                "last_seen_index": None
            }
        agg_entry = agg[sha]
        agg_entry["count"] += 1
        # track order - we don't have explicit line numbers here; just approximate by append order
        # set first/last seen index
        if agg_entry["first_seen_index"] is None:
            agg_entry["first_seen_index"] = 0
        agg_entry["last_seen_index"] = agg_entry["count"] - 1
    return agg

# -------------------------
# Main entrypoint
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Map functions/methods to git commits via git blame.")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Path to repository root.")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Results directory (will be created).")
    p.add_argument("--symbol-table", default=SYMBOL_TABLE_PATH, help="Path to symbol_table.json to use (optional).")
    p.add_argument("--force-scan", action="store_true", help="If symbol table missing, force rebuild via scanning (default).")
    p.add_argument("--prefer-most-recent", action="store_true", help="Select most recent commit (by author_time) as top_commit instead of most frequent.")
    return p.parse_args()

def main():
    args = parse_args()
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    symbol_table_path = args.symbol_table
    prefer_recent = args.prefer_most_recent if hasattr(args, "prefer_most_recent") else args.prefer_most_recent

    logger = setup_logging(out_dir)
    logger.info("Starting function -> commit mapping")
    logger.info("Repo dir: %s", repo_dir)
    logger.info("Out dir: %s", out_dir)
    logger.info("Symbol table path: %s", symbol_table_path)

    if not git_available():
        logger.error("git not available on PATH. Please install git and ensure it is accessible.")
        sys.exit(2)

    # Load symbol table (fallback to scanning)
    symbols = load_symbol_table(symbol_table_path, repo_dir, logger)
    logger.info("Processing %d symbols", len(symbols))

    results: Dict[str, Any] = {}
    processed = 0
    skipped = 0
    for qname, info in symbols.items():
        file_rel = info.get("file")
        if not file_rel:
            logger.debug("Skipping symbol %s: no file info", qname)
            skipped += 1
            continue
        abs_file = os.path.join(repo_dir, file_rel)
        if not os.path.exists(abs_file):
            logger.warning("File missing for symbol %s -> %s (skipping)", qname, abs_file)
            skipped += 1
            continue

        start = info.get("lineno")
        end = info.get("end_lineno")
        if not start:
            logger.debug("Symbol %s has no lineno; skipping", qname)
            skipped += 1
            continue
        if not end:
            # compute heuristic end
            s, e = determine_end_lineno_if_missing(info, abs_file, logger)
            start = s
            end = e

        if not start or not end or end < start:
            logger.warning("Invalid range for %s: start=%s end=%s (skipping)", qname, start, end)
            skipped += 1
            continue

        # perform git blame
        blame_text = run_git_blame(repo_dir, file_rel, int(start), int(end))
        if blame_text is None:
            logger.warning("git blame failed for %s (%s:%s-%s)", qname, file_rel, start, end)
            skipped += 1
            continue

        # parse blame porcelain
        blame_meta = []
        # parse line-by-line: porcelain groups header per source line; we will parse with simple method
        # We'll split by lines and find "author", "author-mail", "author-time", "summary", and commit sha lines
        cur = {}
        for line in blame_text.splitlines():
            if not line:
                continue
            if len(line) >= 40 and line[:40].isalnum() and ' ' in line:
                # new header start
                if cur:
                    blame_meta.append(cur)
                cur = {"sha": line.split()[0]}
            elif line.startswith("author "):
                cur["author"] = line[len("author "):]
            elif line.startswith("author-mail "):
                cur["author_mail"] = line[len("author-mail "):]
            elif line.startswith("author-time "):
                try:
                    epoch = int(line[len("author-time "):].strip())
                    cur["author_time"] = datetime.datetime.utcfromtimestamp(epoch).isoformat() + "Z"
                except Exception:
                    cur["author_time"] = line[len("author-time "):].strip()
            elif line.startswith("summary "):
                cur["summary"] = line[len("summary "):]
            elif line.startswith("\t"):
                # source line - end of header for this line
                if cur:
                    blame_meta.append(cur)
                cur = {}
            else:
                # catch other keys (committer, etc.) - ignore for now
                pass
        if cur:
            blame_meta.append(cur)

        if not blame_meta:
            logger.warning("No blame metadata parsed for %s", qname)
            skipped += 1
            continue

        # aggregate by commit sha
        agg = {}
        for entry in blame_meta:
            sha = entry.get("sha")
            if not sha:
                continue
            if sha not in agg:
                agg[sha] = {
                    "count": 0,
                    "author": entry.get("author"),
                    "author_email": entry.get("author_mail"),
                    "author_time": entry.get("author_time"),
                    "summary": entry.get("summary")
                }
            agg[sha]["count"] += 1

        # pick top commit: by count or most recent author_time depending on flag
        top_sha = None
        if prefer_recent:
            # pick commit with max author_time (if parseable)
            best_time = None
            for sha, meta in agg.items():
                at = meta.get("author_time")
                try:
                    t = datetime.datetime.fromisoformat(at.replace("Z", "+00:00"))
                except Exception:
                    t = None
                if t and (best_time is None or t > best_time):
                    best_time = t
                    top_sha = sha
            if top_sha is None:
                # fallback to max count
                top_sha = max(agg.items(), key=lambda kv: kv[1]["count"])[0]
        else:
            top_sha = max(agg.items(), key=lambda kv: kv[1]["count"])[0]

        top_meta = agg.get(top_sha, {})

        results[qname] = {
            "file": file_rel,
            "lineno": start,
            "end_lineno": end,
            "blame_summary": agg,
            "top_commit": {
                "sha": top_sha,
                "count": top_meta.get("count"),
                "author": top_meta.get("author"),
                "author_email": top_meta.get("author_email"),
                "author_time": top_meta.get("author_time"),
                "summary": top_meta.get("summary")
            }
        }
        processed += 1
        if processed % 50 == 0:
            logger.info("Processed %d symbols...", processed)

    out_path = os.path.join(out_dir, OUTPUT_FILENAME)
    try:
        with open(out_path, "w", encoding="utf-8") as fo:
            json.dump(results, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote function->commit mapping for %d symbols to %s", len(results), out_path)
    except Exception as e:
        logger.error("Failed to write output: %s", e)
        sys.exit(2)

    logger.info("Finished. processed=%d skipped=%d", processed, skipped)

if __name__ == "__main__":
    main()
