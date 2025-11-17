#!/usr/bin/env python3
"""
interactive_resolve.py

Interactive helper to resolve remaining ambiguous Function nodes after automated passes.

Usage (from project root Testing_GitHub_Code):
  python .\results\interactive_resolve.py

Options:
  --nodes <path>        (default: results/nodes_disambiguated.json)
  --textreport <path>   (default: results/kg_check/textsearch_report.json)
  --remaining <path>    (default: results/kg_check/remaining_after_disambiguation.txt)
  --results-dir <dir>   (default: results)
  --repo-dir <path>     (used to show file snippets; default ../Ecommerce-Data-MLOps)
  --debug               print debug statements

Behavior:
 - For each remaining function it shows candidate matches (if present in text report).
 - Shows small snippet from candidate files to help you decide.
 - You select an option, and the script updates nodes and logs the action.
 - Writes:
     - results/nodes_resolved.json
     - results/kg_check/interactive_resolve_log.json
     - results/kg_check/remaining_after_interactive.txt
 - Keeps backup of original nodes file as nodes_disambiguated.json.bak.<ts>
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from typing import List, Tuple

SNIPPET_CTX_LINES = 4  # lines before/after

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"[ERROR] load_json {path}: {e}", file=sys.stderr)
        return None

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def read_remaining(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [l.strip() for l in fh if l.strip()]

def safe_rel(repo_dir, filepath):
    try:
        return os.path.relpath(os.path.join(repo_dir, filepath), repo_dir).replace("\\","/")
    except Exception:
        return filepath

def snippet_from_repo(repo_dir, relpath, lineno, ctx=SNIPPET_CTX_LINES):
    fp = os.path.join(repo_dir, relpath)
    if not os.path.exists(fp):
        return f"(file not found: {relpath})"
    try:
        with open(fp, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
    except Exception as e:
        return f"(could not open file {relpath}: {e})"
    idx = max(0, lineno-1)
    start = max(0, idx-ctx)
    end = min(len(lines), idx+ctx+1)
    snippet = []
    for i in range(start, end):
        prefix = ">> " if i==idx else "   "
        snippet.append(f"{i+1:5d}: {prefix}{lines[i].rstrip()}")
    return "\n".join(snippet)

def prompt_choice(prompt, choices):
    # display choices like (y/n)
    while True:
        r = input(prompt).strip()
        if r == "":
            continue
        if r.lower() in choices:
            return r.lower()
        print("Invalid input. Choices:", "/".join(choices))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default=os.path.join("results","nodes_disambiguated.json"))
    p.add_argument("--textreport", default=os.path.join("results","kg_check","textsearch_report.json"))
    p.add_argument("--remaining", default=os.path.join("results","kg_check","remaining_after_disambiguation.txt"))
    p.add_argument("--results-dir", default="results")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")))
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    nodes_path = args.nodes
    report_path = args.textreport
    remaining_path = args.remaining
    results_dir = args.results_dir
    repo_dir = args.repo_dir
    debug = args.debug

    if debug:
        print("[DEBUG] nodes_path:", nodes_path)
        print("[DEBUG] report_path:", report_path)
        print("[DEBUG] remaining_path:", remaining_path)
        print("[DEBUG] repo_dir:", repo_dir)

    nodes = load_json(nodes_path)
    if nodes is None:
        print("[ERROR] Cannot read nodes file:", nodes_path); return 1
    # map id -> node
    id2node = {n["id"]: n for n in nodes}

    report = load_json(report_path) or {}
    ambiguous_sample = report.get("ambiguous_sample") or report.get("ambiguous") or []
    # build candidate mapping: id -> list of (file,lineno)
    candidates = {}
    for it in ambiguous_sample:
        # item may be dict or list
        if isinstance(it, dict) and "id" in it and "candidates" in it:
            candidates[it["id"]] = it["candidates"]
        elif isinstance(it, list) and len(it)>=2:
            # older format [id, candidates]
            candidates[it[0]] = it[1]
    # fallback: no candidates in report -> attempt to load textsearch_report.json structure
    # NOW read remaining list
    remaining_ids = read_remaining(remaining_path)
    if not remaining_ids:
        print("No remaining IDs to resolve at", remaining_path)
        return 0

    # backup nodes file
    ts = int(time.time())
    backup_path = nodes_path + f".bak.{ts}"
    try:
        write_json(backup_path, nodes)
        print("Backup written to", backup_path)
    except Exception as e:
        print("[WARNING] could not write backup:", e)

    log = []
    for fid in remaining_ids:
        node = id2node.get(fid)
        print("\n" + "="*80)
        print(f"Function ID: {fid}")
        if node:
            print("Qualified name:", node.get("qualified_name"))
            print("Current file:", node.get("file"), "lineno:", node.get("lineno"))
        else:
            print("(node not found in nodes file)")

        cand = candidates.get(fid, [])
        if cand:
            print("\nCandidate definitions found:")
            for i, (fpath, ln) in enumerate(cand, start=1):
                print(f"  [{i}] {fpath}:{ln}")
                # show snippet
                snippet = snippet_from_repo(repo_dir, fpath, int(ln))
                print(snippet)
                print("-"*40)
        else:
            print("\nNo candidate list found in text report for this function.")
            # run a simple quick search (best-effort): scan repo for def name
            short = (node.get("qualified_name") or fid).split(".")[-1]
            print(f"Attempting quick search for 'def {short}(' in repo ... (may take a moment)")
            found = []
            for root, _, files in os.walk(repo_dir):
                for fname in files:
                    if not fname.endswith(".py"):
                        continue
                    fp = os.path.join(root, fname)
                    try:
                        with open(fp, "r", encoding="utf-8", errors="ignore") as fh:
                            for i, line in enumerate(fh, start=1):
                                if line.lstrip().startswith("def ") and short+"(" in line:
                                    rel = os.path.relpath(fp, repo_dir).replace("\\","/")
                                    found.append((rel, i))
                                    break
                    except Exception:
                        continue
            if found:
                print("Quick search matches:")
                for i, (fpath, ln) in enumerate(found, start=1):
                    print(f"  [{i}] {fpath}:{ln}")
                    print(snippet_from_repo(repo_dir, fpath, int(ln)))
                    print("-"*30)
                cand = found

        # Present choices to the user
        print("\nActions:")
        print("  Enter a number to accept that candidate (e.g., 1)")
        print("  's' -> skip (leave unresolved for later)")
        print("  'm' -> manual entry (enter file:lineno)")
        print("  'd' -> mark as document-only / not in source")
        print("  'q' -> quit script (save progress so far)")
        choice = input("Your choice: ").strip()
        if choice.lower() == "q":
            print("Quitting early; progress will be saved.")
            break
        if choice.lower() == "s":
            log.append({"id": fid, "action": "skipped"})
            continue
        if choice.lower() == "d":
            # annotate node
            if node:
                attrs = node.setdefault("attrs", {})
                attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"manual","note":"marked_as_document_only"}]
                log.append({"id": fid, "action":"marked_document_only"})
            else:
                log.append({"id": fid, "action":"marked_document_only_no_node"})
            continue
        if choice.lower() == "m":
            manual = input("Enter file:lineno (relative to repo root), e.g. src/foo.py:123 : ").strip()
            if ":" in manual:
                fpath, lstr = manual.rsplit(":",1)
                try:
                    ln = int(lstr)
                except:
                    print("Invalid lineno; skipping")
                    log.append({"id": fid, "action":"manual_invalid", "input": manual})
                    continue
                if node:
                    node["file"] = fpath.replace("\\","/")
                    node["lineno"] = ln
                    attrs = node.setdefault("attrs", {})
                    attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"manual","note":"user_entry","file":fpath,"lineno":ln}]
                    log.append({"id": fid, "action":"manual_set", "file":fpath, "lineno":ln})
                else:
                    log.append({"id": fid, "action":"manual_no_node","input":manual})
                continue
            else:
                print("Malformed input; skipping")
                log.append({"id": fid, "action":"manual_malformed","input":manual})
                continue
        # numeric selection
        try:
            idx = int(choice)
            if idx <= 0:
                raise ValueError()
            if idx > len(cand):
                print("Index out of range; skipping")
                log.append({"id": fid, "action":"index_out_of_range", "choice": choice})
                continue
            fpath, ln = cand[idx-1]
            if node:
                node["file"] = fpath.replace("\\","/")
                try:
                    node["lineno"] = int(ln)
                except:
                    # sometimes ln may be string; attempt int
                    try:
                        node["lineno"] = int(float(ln))
                    except:
                        node["lineno"] = None
                attrs = node.setdefault("attrs", {})
                attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"manual","note":"user_choice","file":fpath,"lineno":ln}]
                log.append({"id": fid, "action":"manual_choice", "file":fpath, "lineno": ln})
            else:
                log.append({"id": fid, "action":"manual_choice_no_node", "file":fpath, "lineno":ln})
        except ValueError:
            print("Unrecognized input; skipping")
            log.append({"id": fid, "action":"unrecognized_input", "input": choice})

    # save nodes file
    out_nodes = os.path.join(results_dir, "nodes_resolved.json")
    write_json(out_nodes, nodes)
    # save log and remaining list
    kg_check = os.path.join(results_dir, "kg_check")
    os.makedirs(kg_check, exist_ok=True)
    write_json(os.path.join(kg_check, "interactive_resolve_log.json"), {"log": log, "timestamp": int(time.time())})
    # compute new remaining
    new_remaining = []
    for fid in remaining_ids:
        n = id2node.get(fid)
        if not n or not n.get("file"):
            new_remaining.append(fid)
    with open(os.path.join(kg_check, "remaining_after_interactive.txt"), "w", encoding="utf-8") as fh:
        for r in new_remaining:
            fh.write(r + "\n")
    print("\nDone. Wrote:", out_nodes)
    print("Log:", os.path.join(kg_check, "interactive_resolve_log.json"))
    print("Remaining after interactive:", len(new_remaining), "see", os.path.join(kg_check, "remaining_after_interactive.txt"))
    return 0

if __name__ == "__main__":
    main()
