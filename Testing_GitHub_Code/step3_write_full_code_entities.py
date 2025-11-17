#!/usr/bin/env python3
"""
step3_write_full_code_entities.py

Professional, production-ready script to:
  - Walk a repository (or specified directories)
  - Read every Python source file
  - Compute file metadata (size, sha256)
  - Parse full AST and serialize a JSON-friendly AST representation
  - Extract module docstring, functions, classes, methods with start/end lines
  - Save full source text (optionally), or a bounded source_sample per node
  - Emit:
      results/code_entities_full.json   (array of enriched per-file entries)
      results/asts/<safe-relpath>.json  (JSON AST per file)
      results/raw_sources/<safe-relpath>.py  (raw source copy, optional)
      results/log (stdout + logging)
Usage:
  python step3_write_full_code_entities.py --repo-dir /path/to/repo
  python step3_write_full_code_entities.py    # assumes repo at ../Ecommerce-Data-MLOps

Notes:
  - Requires Python 3.8+ (uses ast end_lineno when available).
  - Be cautious enabling --write-full-source on very large repos (disk).
  - This script focuses on Python sources only.
"""

import os
import sys
import io
import json
import argparse
import logging
import hashlib
import pathlib
import traceback
from typing import Any, Dict, List, Optional
import ast

# ----------------------------
# Utilities: AST -> JSON safe
# ----------------------------
def ast_node_to_dict(node: ast.AST) -> Any:
    """
    Convert an ast.AST node into a JSON-serializable dict recursively.
    Keeps node type and important fields. Omits large literal contents.
    """
    if node is None:
        return None
    if isinstance(node, (str, int, float, bool)):
        return node
    if isinstance(node, list):
        return [ast_node_to_dict(n) for n in node]
    if not isinstance(node, ast.AST):
        return repr(node)
    d: Dict[str, Any] = {"_type": node.__class__.__name__}
    for field, value in ast.iter_fields(node):
        # skip ctx objects (load/store) detail
        if field == "ctx":
            continue
        # for large constants, include a short preview
        if field == "value" and isinstance(value, ast.Constant):
            const_val = value.value
            if isinstance(const_val, (str, bytes)):
                preview = const_val[:160] if isinstance(const_val, str) else (const_val[:160] if isinstance(const_val, bytes) else None)
                d[field] = {"_const_preview": preview}
                continue
        # recursive conversion
        d[field] = ast_node_to_dict(value)
    # include lineno/end_lineno when present
    if hasattr(node, "lineno"):
        d["lineno"] = getattr(node, "lineno", None)
    if hasattr(node, "end_lineno"):
        d["end_lineno"] = getattr(node, "end_lineno", None)
    return d

# ----------------------------
# File utilities
# ----------------------------
def compute_sha256(path: str, block_size: int = 65536) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_relpath(base_dir: str, path: str) -> str:
    return os.path.relpath(path, base_dir).replace("\\", "/")

def safe_name_from_rel(rel: str) -> str:
    return rel.replace("/", "__").replace("\\", "__")

# ----------------------------
# Extraction logic
# ----------------------------
def extract_file_info(path: str, repo_dir: str, max_source_lines: int, write_full_source: bool) -> Dict[str, Any]:
    """
    Read a python file, parse AST, extract module docstring, functions, classes, and AST dump.
    Returns a dictionary representing the enriched file entry.
    """
    entry: Dict[str, Any] = {}
    abs_path = os.path.abspath(path)
    rel = safe_relpath(repo_dir, abs_path)
    entry["relpath"] = rel
    entry["abs_path"] = abs_path
    try:
        stat = os.stat(abs_path)
        entry["size"] = stat.st_size
        entry["mtime"] = stat.st_mtime
        entry["sha256"] = compute_sha256(abs_path)
    except Exception as e:
        entry["error"] = f"stat_or_hash_failed: {e}"
        return entry

    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
            source = f.read()
    except Exception as e:
        entry["error"] = f"read_failed: {e}"
        return entry

    # optionally store raw source or sample later by the caller
    entry["has_source"] = True
    entry["source_length"] = len(source)

    # parse AST
    try:
        parsed = ast.parse(source)
    except Exception as e:
        entry["parse_error"] = f"{e}"
        # still include raw source sample if available
        entry["module_doc"] = ast.get_docstring(ast.parse(""), clean=False) if False else None
        entry["functions"] = []
        entry["classes"] = []
        entry["_ast"] = None
        return entry

    # module docstring
    entry["module_doc"] = ast.get_docstring(parsed, clean=False)

    # prepare mapping of functions and classes
    functions: List[Dict[str, Any]] = []
    classes: List[Dict[str, Any]] = []

    # iterate top-level nodes for defs (preserves order)
    for node in parsed.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_entry: Dict[str, Any] = {}
            func_entry["name"] = node.name
            func_entry["lineno"] = getattr(node, "lineno", None)
            func_entry["end_lineno"] = getattr(node, "end_lineno", None)
            # signature: simple param list
            try:
                args = []
                for a in node.args.args:
                    args.append(a.arg)
                if node.args.vararg:
                    args.append("*" + node.args.vararg.arg)
                if node.args.kwarg:
                    args.append("**" + node.args.kwarg.arg)
                func_entry["sig"] = "(" + ", ".join(args) + ")"
            except Exception:
                func_entry["sig"] = None
            func_entry["doc"] = ast.get_docstring(node, clean=False)
            # source sample
            if func_entry["lineno"] and func_entry["end_lineno"]:
                start = func_entry["lineno"] - 1
                end = func_entry["end_lineno"]
                lines = source.splitlines()
                segment = "\n".join(lines[start:end])
                if not write_full_source and segment.count("\n") > max_source_lines:
                    # limit by lines
                    segment = "\n".join(segment.splitlines()[:max_source_lines])
                func_entry["source"] = segment
            else:
                func_entry["source"] = None
            functions.append(func_entry)

        elif isinstance(node, ast.ClassDef):
            class_entry: Dict[str, Any] = {}
            class_entry["name"] = node.name
            class_entry["lineno"] = getattr(node, "lineno", None)
            class_entry["end_lineno"] = getattr(node, "end_lineno", None)
            class_entry["doc"] = ast.get_docstring(node, clean=False)
            methods: List[Dict[str, Any]] = []
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    mentry: Dict[str, Any] = {}
                    mentry["name"] = child.name
                    mentry["lineno"] = getattr(child, "lineno", None)
                    mentry["end_lineno"] = getattr(child, "end_lineno", None)
                    try:
                        args = []
                        for a in child.args.args:
                            args.append(a.arg)
                        if child.args.vararg:
                            args.append("*" + child.args.vararg.arg)
                        if child.args.kwarg:
                            args.append("**" + child.args.kwarg.arg)
                        mentry["sig"] = "(" + ", ".join(args) + ")"
                    except Exception:
                        mentry["sig"] = None
                    mentry["doc"] = ast.get_docstring(child, clean=False)
                    if mentry["lineno"] and mentry["end_lineno"]:
                        start = mentry["lineno"] - 1
                        end = mentry["end_lineno"]
                        lines = source.splitlines()
                        segment = "\n".join(lines[start:end])
                        if not write_full_source and segment.count("\n") > max_source_lines:
                            segment = "\n".join(segment.splitlines()[:max_source_lines])
                        mentry["source"] = segment
                    else:
                        mentry["source"] = None
                    methods.append(mentry)
            class_entry["methods"] = methods
            classes.append(class_entry)

    # attach lists
    entry["functions"] = functions
    entry["classes"] = classes

    # attach AST as JSON-serializable dict
    try:
        entry["_ast"] = ast_node_to_dict(parsed)
    except Exception as e:
        entry["_ast_error"] = f"ast_serialize_failed: {e}"
        entry["_ast"] = None

    # include small source sample at file level too
    if not write_full_source:
        # keep only first N lines for the file source sample
        lines = source.splitlines()
        sample = "\n".join(lines[:max_source_lines])
        entry["source_sample"] = sample
    else:
        entry["source"] = source

    return entry

# ----------------------------
# Main CLI
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Extract full Python code entities, ASTs and sources for KG ingestion.")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")),
                   help="Path to repository root (default: ../Ecommerce-Data-MLOps)")
    p.add_argument("--out-dir", default=os.path.join(os.getcwd(), "results"),
                   help="Output directory (default: ./results)")
    p.add_argument("--max-source-lines", type=int, default=200,
                   help="Maximum lines of source to keep per function/method/file sample (default: 200)")
    p.add_argument("--write-full-source", action="store_true",
                   help="Write full source texts to results/raw_sources (may be large).")
    p.add_argument("--pattern", nargs="*", default=["**/*.py"],
                   help="Glob patterns (relative to repo) for files to process (default all .py)")
    p.add_argument("--skip-ast-files", action="store_true",
                   help="Do not write separate AST JSON files per source (keeps only aggregated JSON).")
    p.add_argument("--workers", type=int, default=1,
                   help="Number of worker processes (not implemented; single-threaded).")
    return p.parse_args()

def find_files(repo_dir: str, patterns: List[str]) -> List[str]:
    files = []
    repo = pathlib.Path(repo_dir)
    for pat in patterns:
        for p in repo.glob(pat):
            if p.is_file():
                files.append(str(p.resolve()))
    # de-duplicate and sort
    files = sorted(list(dict.fromkeys(files)))
    return files

def ensure_dirs(base_out: str):
    os.makedirs(base_out, exist_ok=True)
    asts = os.path.join(base_out, "asts")
    raw = os.path.join(base_out, "raw_sources")
    os.makedirs(asts, exist_ok=True)
    os.makedirs(raw, exist_ok=True)
    return asts, raw

def main():
    args = parse_args()

    # logging
    os.makedirs(args.out_dir, exist_ok=True)
    log_path = os.path.join(args.out_dir, "step3_code_entities.log")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%H:%M:%S",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(log_path)])
    log = logging.getLogger("code-entities-full")

    log.info("Repo dir: %s", args.repo_dir)
    log.info("Output dir: %s", args.out_dir)
    log.info("Patterns: %s", args.pattern)
    log.info("Max source lines per node: %d", args.max_source_lines)
    log.info("Write full source files: %s", args.write_full_source)

    files = find_files(args.repo_dir, args.pattern)
    log.info("Found %d files matching patterns", len(files))
    if not files:
        log.warning("No files found; exiting.")
        return

    asts_dir, raw_dir = ensure_dirs(args.out_dir)
    aggregated: List[Dict[str, Any]] = []
    processed = 0
    failed = 0

    for path in files:
        try:
            info = extract_file_info(path, args.repo_dir, args.max_source_lines, args.write_full_source)
            aggregated.append(info)
            processed += 1
            # write per-file AST JSON if requested
            if not args.skip_ast_files and info.get("_ast") is not None:
                safe = safe_name_from_rel(info["relpath"]) + ".ast.json"
                ast_out = os.path.join(asts_dir, safe)
                try:
                    with open(ast_out, "w", encoding="utf-8") as fo:
                        json.dump(info["_ast"], fo, indent=2, ensure_ascii=False)
                except Exception as e:
                    log.warning("Failed to write AST for %s: %s", info["relpath"], e)
            # write raw source if requested
            if args.write_full_source and info.get("has_source"):
                safe_src = safe_name_from_rel(info["relpath"]) + ".py"
                src_out = os.path.join(raw_dir, safe_src)
                try:
                    # read original file and write verbatim
                    with open(info["abs_path"], "r", encoding="utf-8", errors="replace") as fr:
                        content = fr.read()
                    with open(src_out, "w", encoding="utf-8") as fw:
                        fw.write(content)
                except Exception as e:
                    log.warning("Failed to write raw source for %s: %s", info["relpath"], e)
            if processed % 50 == 0:
                log.info("Processed %d files...", processed)
        except Exception as e:
            failed += 1
            log.error("Failed processing %s : %s", path, e)
            log.debug(traceback.format_exc())

    # write aggregated JSON
    out_file = os.path.join(args.out_dir, "code_entities_full.json")
    try:
        with open(out_file, "w", encoding="utf-8") as fo:
            json.dump(aggregated, fo, indent=2, ensure_ascii=False)
        log.info("Wrote aggregated code entities to: %s (files=%d failed=%d)", out_file, processed, failed)
    except Exception as e:
        log.error("Failed to write aggregated JSON: %s", e)

if __name__ == "__main__":
    main()
