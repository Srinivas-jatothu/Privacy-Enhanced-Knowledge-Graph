#!/usr/bin/env python3
# stepO_merge_into_kg.py (replacement)
# Robust merge of pipeline artifacts into canonical KG:
#  - writes results/nodes.json, results/edges.json, results/kg_nodes.csv, results/kg_edges.csv
#  - tolerant to multiple artifact shapes (dict wrappers, lists, etc.)
# Author: generated for user (robust behavior for observed artifact shapes)

from __future__ import annotations
import argparse
import csv
import hashlib
import json
import logging
import os
import sys
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple

# ---------- Defaults ----------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_RESULTS_DIR = os.path.join(os.getcwd(), "results")
KG_SCHEMA_NAME = "kg_schema.json"      # here the schema will be stored 

# ---------- Logging ----------
def setup_logger(out_dir: str, level=logging.INFO):
    os.makedirs(out_dir, exist_ok=True)
    logger = logging.getLogger("kg-merge")
    logger.setLevel(level)
    if not logger.handlers:
        fh = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger

# ---------- I/O helpers ----------
def load_json(path: str) -> Optional[Any]:
    if not path:
        return None
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            # try to read small sample if JSON is huge/corrupt
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                    txt = fh.read(100000)
                    # fall back to None
            except Exception:
                pass
            return None
    return None

def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def canonical_node_id(node_type: str, identifier: str) -> str:
    ident = "" if identifier is None else str(identifier).replace("\\", "/")
    return f"{node_type}:{ident}"

def short_id(s: str, length: int = 8) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:length]

# ---------- Schema ----------
def produce_schema_if_missing(results_dir: str, logger):
    schema_path = os.path.join(results_dir, KG_SCHEMA_NAME)
    if not os.path.exists(schema_path):
        default_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "PEKG - Normal KG Schema (minimal)",
            "description": "Auto-generated schema by stepO_merge_into_kg.py",
            "node_types": ["File", "Module", "Function", "Class", "Commit", "PullRequest", "Package", "Config", "Document"],
            "edge_types": ["IMPORTS", "CALLS", "DEFINES", "MODIFIED_BY", "CLOSED_BY", "DEPENDS_ON", "MENTIONS", "CALLS_FILE_LEVEL"]
        }
        write_json(schema_path, default_schema)
        logger.info("Wrote default KG schema to %s", schema_path)
    else:
        logger.info("KG schema found: %s", schema_path)

# ---------- Load artifacts ----------
def load_artifacts(results_dir: str, logger) -> Dict[str, Any]:
    artifacts = {
        "manifest_enriched": load_json(os.path.join(results_dir, "manifest_enriched.json")),
        "manifest": load_json(os.path.join(results_dir, "manifest.json")),
        "symbol_table": load_json(os.path.join(results_dir, "symbol_table.json")),
        "module_index": load_json(os.path.join(results_dir, "module_index.json")),
        "import_aliases": load_json(os.path.join(results_dir, "import_aliases.json")),
        "call_graph_enriched": load_json(os.path.join(results_dir, "call_graph_enriched.json")),
        "call_graph": load_json(os.path.join(results_dir, "call_graph.json")),
        "file_dependency_graph": load_json(os.path.join(results_dir, "file_dependency_graph.json")),
        "function_commits": load_json(os.path.join(results_dir, "function_commits.json")),
        "commits": load_json(os.path.join(results_dir, "commits.json")),
        "pull_requests": load_json(os.path.join(results_dir, "pull_requests.json")),
        "commit_to_prs": load_json(os.path.join(results_dir, "commit_to_prs.json")),
        "dependencies": load_json(os.path.join(results_dir, "dependencies.json")),
        "dependencies_by_file": load_json(os.path.join(results_dir, "dependencies_by_file.json")),
        "code_entities_full": load_json(os.path.join(results_dir, "code_entities_full.json")),
        "code_entities_enriched": load_json(os.path.join(results_dir, "code_entities_enriched.json")),
        "extracted_text_dir": os.path.join(results_dir, "extracted_text"),
        "raw_sources_dir": os.path.join(results_dir, "raw_sources"),
    }
    for k, v in artifacts.items():
        if k.endswith("_dir"):
            exists = os.path.isdir(v)
            logger.info("Artifact %-25s : %s", k, ("FOUND" if exists else "MISSING"))
        else:
            logger.info("Artifact %-25s : %s", k, ("FOUND" if v is not None else "MISSING"))
    return artifacts

# ---------- Normalizers ----------
def unwrap_manifest(manifest):
    # Accept list, or dict with 'files' key, or dict mapping -> convert to list of entries
    if manifest is None:
        return None
    if isinstance(manifest, list):
        return manifest
    if isinstance(manifest, dict):
        if "files" in manifest and isinstance(manifest["files"], list):
            return manifest["files"]
        # if dict of path->meta, convert to list of dicts with 'path'
        entries = []
        for k, v in manifest.items():
            if isinstance(v, dict):
                entry = dict(v)
                # attempt to detect a path key
                if not any(k2 in entry for k2 in ("path", "file", "relpath", "name")):
                    entry["path"] = k
                entries.append(entry)
            else:
                entries.append({"path": k, "meta": v})
        return entries
    return None

def normalize_commits(commits_raw):
    # returns list of commit dicts or simple sha strings
    if commits_raw is None:
        return []
    if isinstance(commits_raw, list):
        return commits_raw
    if isinstance(commits_raw, dict):
        # common wrapper: {"commits": [...]}
        if "commits" in commits_raw and isinstance(commits_raw["commits"], list):
            return commits_raw["commits"]
        # dict of sha -> commitobj
        return list(commits_raw.values())
    # otherwise unknown -> empty
    return []

def normalize_prs(prs_raw):
    if prs_raw is None:
        return []
    if isinstance(prs_raw, list):
        return prs_raw
    if isinstance(prs_raw, dict):
        if "pull_requests" in prs_raw and isinstance(prs_raw["pull_requests"], list):
            return prs_raw["pull_requests"]
        return list(prs_raw.values())
    return []

# ---------- Build nodes ----------
def build_nodes_and_index(artifacts: Dict[str, Any], repo_dir: str, results_dir: str, logger):
    nodes: Dict[str, Dict[str, Any]] = {}
    id_by_file: Dict[str, str] = {}
    id_by_module: Dict[str, str] = {}
    id_by_symbol: Dict[str, str] = {}
    id_by_commit: Dict[str, str] = {}
    id_by_pr: Dict[str, str] = {}
    id_by_package: Dict[str, str] = {}

    # 1) Files from manifest
    manifest = artifacts.get("manifest_enriched") or artifacts.get("manifest")
    manifest_list = unwrap_manifest(manifest)
    if manifest_list and isinstance(manifest_list, list):
        for entry in manifest_list:
            path = entry.get("path") or entry.get("file") or entry.get("relpath") or entry.get("name")
            if not path:
                continue
            path = str(path).replace("\\", "/")
            file_id = canonical_node_id("File", path)
            nodes[file_id] = {
                "id": file_id,
                "type": "File",
                "label": os.path.basename(path),
                "path": path,
                "size": entry.get("size"),
                "mime": entry.get("mime"),
                "hash": entry.get("sha") or entry.get("hash")
            }
            id_by_file[path] = file_id
    else:
        logger.info("No manifest usable: discovering files under repo dir for File nodes")
        for root, dirs, files in os.walk(repo_dir):
            for f in files:
                rel = os.path.relpath(os.path.join(root, f), repo_dir).replace("\\", "/")
                fid = canonical_node_id("File", rel)
                nodes[fid] = {"id": fid, "type": "File", "label": f, "path": rel}
                id_by_file[rel] = fid

    # 2) Modules
    module_index = artifacts.get("module_index") or {}
    if isinstance(module_index, dict):
        for mod, info in module_index.items():
            mid = canonical_node_id("Module", mod)
            nodes[mid] = {"id": mid, "type": "Module", "label": mod, "name": mod, "file": info.get("file")}
            id_by_module[mod] = mid
            file_rel = info.get("file")
            if file_rel and file_rel not in id_by_file:
                fid = canonical_node_id("File", file_rel)
                id_by_file[file_rel] = fid
                if fid not in nodes:
                    nodes[fid] = {"id": fid, "type": "File", "label": os.path.basename(file_rel), "path": file_rel}

    # 3) Symbols (functions/classes)
    symbol_table = artifacts.get("symbol_table") or {}
    if isinstance(symbol_table, dict):
        for qname, info in symbol_table.items():
            typ = info.get("kind") or info.get("type") or "Function"
            ntype = "Class" if isinstance(typ, str) and typ.lower().startswith("class") else "Function"
            name = qname.split(".")[-1]
            file_rel = info.get("file") or info.get("path") or info.get("filename")
            lineno = info.get("lineno") or info.get("line")
            node_id = canonical_node_id(ntype, qname)
            nodes[node_id] = {
                "id": node_id,
                "type": ntype,
                "label": name,
                "qualified_name": qname,
                "file": file_rel,
                "lineno": lineno,
                "attrs": info
            }
            id_by_symbol[qname] = node_id
            if file_rel and file_rel not in id_by_file:
                fid = canonical_node_id("File", file_rel)
                id_by_file[file_rel] = fid
                nodes[fid] = {"id": fid, "type": "File", "label": os.path.basename(file_rel), "path": file_rel}

    # 4) Commits (robust)
    commits_raw = artifacts.get("commits")
    commits = normalize_commits(commits_raw)
    for c in commits:
        sha = None
        if isinstance(c, str):
            sha = c
        elif isinstance(c, dict):
            sha = c.get("sha") or c.get("id") or c.get("hash")
            if not sha:
                commit_block = c.get("commit") if isinstance(c.get("commit"), dict) else {}
                sha = commit_block.get("sha") or commit_block.get("id")
        if not sha:
            continue
        sha = str(sha)
        cid = canonical_node_id("Commit", sha)
        nodes[cid] = {
            "id": cid,
            "type": "Commit",
            "sha": sha,
            "message": (c.get("message") or (c.get("commit") or {}).get("message")) if isinstance(c, dict) else None,
            "author": (c.get("author") or (c.get("commit") or {}).get("author") or {}).get("name") if isinstance(c, dict) else None,
            "date": (c.get("date") or (c.get("commit") or {}).get("author") or {}).get("date") if isinstance(c, dict) else None,
            "attrs": c if isinstance(c, dict) else None
        }
        id_by_commit[sha] = cid

    # 5) Pull requests (robust author handling)
    prs_raw = artifacts.get("pull_requests")
    prs = normalize_prs(prs_raw)
    if isinstance(prs, list):
        for p in prs:
            if p is None:
                continue
            if not isinstance(p, dict):
                # if list contains numbers or ids
                try:
                    num = int(p)
                except Exception:
                    continue
                pid = canonical_node_id("PullRequest", str(num))
                nodes[pid] = {"id": pid, "type": "PullRequest", "number": int(num)}
                id_by_pr[str(num)] = pid
                continue
            num = p.get("number") or p.get("id")
            if num is None:
                continue
            user_field = p.get("user")
            if isinstance(user_field, dict):
                author = user_field.get("login") or user_field.get("name") or p.get("author")
            elif isinstance(user_field, str):
                author = user_field
            else:
                author = p.get("author")
            pid = canonical_node_id("PullRequest", str(num))
            nodes[pid] = {
                "id": pid,
                "type": "PullRequest",
                "number": int(num) if isinstance(num, (int, str)) and str(num).isdigit() else num,
                "title": p.get("title"),
                "author": author,
                "created_at": p.get("created_at"),
                "merged_at": p.get("merged_at"),
                "attrs": p
            }
            id_by_pr[str(num)] = pid

    # 6) Packages from dependencies
    deps = artifacts.get("dependencies") or []
    if isinstance(deps, list):
        for d in deps:
            name = d.get("name")
            version = d.get("version") or ""
            if not name:
                continue
            key = f"{name}@{version}"
            pid = canonical_node_id("Package", key)
            nodes[pid] = {"id": pid, "type": "Package", "name": name, "version": version}
            id_by_package[key] = pid

    # 7) Documents (extracted text)
    extracted_dir = artifacts.get("extracted_text_dir")
    if extracted_dir and os.path.isdir(extracted_dir):
        for root, dirs, files in os.walk(extracted_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, results_dir).replace("\\", "/")
                text_preview = None
                try:
                    with open(full, "r", encoding="utf-8", errors="replace") as fh:
                        text_preview = fh.read(2000)
                except Exception:
                    text_preview = None
                doc_id = canonical_node_id("Document", rel)
                nodes[doc_id] = {"id": doc_id, "type": "Document", "file": rel, "text_preview": text_preview}

    # 8) code_entities_full fallback
    code_entities = artifacts.get("code_entities_full") or artifacts.get("code_entities_enriched")
    if isinstance(code_entities, list):
        for ent in code_entities:
            fpath = ent.get("file") or ent.get("path")
            funcs = ent.get("functions") or []
            for f in funcs:
                qname = f.get("qualified_name") or f.get("name")
                if not qname:
                    continue
                if qname not in id_by_symbol:
                    node_id = canonical_node_id("Function", qname)
                    nodes[node_id] = {"id": node_id, "type": "Function", "qualified_name": qname, "file": fpath, "signature": f.get("sig"), "docstring": f.get("doc")}
                    id_by_symbol[qname] = node_id
                    if fpath and fpath not in id_by_file:
                        fid = canonical_node_id("File", fpath)
                        id_by_file[fpath] = fid
                        nodes[fid] = {"id": fid, "type": "File", "label": os.path.basename(fpath), "path": fpath}

    return nodes, id_by_file, id_by_module, id_by_symbol, id_by_commit, id_by_pr, id_by_package

# ---------- Build edges ----------
def build_edges(
    artifacts: Dict[str, Any],
    nodes_index: Dict[str, Dict[str, Any]],
    ids: Tuple[Dict[str, Any], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]],
    results_dir: str,
    logger,
):
    (_, id_by_file, id_by_module, id_by_symbol, id_by_commit, id_by_pr, id_by_package) = ids
    edges = {}  # key: (src, tgt, type) -> {"count": int, "provenance": defaultdict(int), "attrs": {}}

    def add_edge(src_id, tgt_id, etype, count=1, prov=None, attrs=None):
        if not src_id or not tgt_id:
            return
        key = (src_id, tgt_id, etype)
        if key not in edges:
            edges[key] = {"count": 0, "provenance": defaultdict(int), "attrs": {}}
        edges[key]["count"] += count or 1
        if prov:
            edges[key]["provenance"][prov] += 1
        if attrs:
            # shallow merge
            edges[key]["attrs"].update(attrs)

    # IMPORTS from file_dependency_graph
    file_dep = artifacts.get("file_dependency_graph") or []
    if isinstance(file_dep, list):
        for rec in file_dep:
            src = rec.get("source")
            tgt = rec.get("target")
            if not src or not tgt:
                continue
            src_id = id_by_file.get(src) or canonical_node_id("File", src)
            tgt_id = id_by_file.get(tgt) or canonical_node_id("File", tgt)
            if src_id not in nodes_index:
                nodes_index[src_id] = {"id": src_id, "type": "File", "path": src, "label": os.path.basename(src)}
            if tgt_id not in nodes_index:
                nodes_index[tgt_id] = {"id": tgt_id, "type": "File", "path": tgt, "label": os.path.basename(tgt)}
            add_edge(src_id, tgt_id, "IMPORTS", count=rec.get("count", 1), prov="file_dependency_graph", attrs={"reasons": rec.get("reasons")})

    # CALLS from call_graph_enriched or call_graph
    call_graph = artifacts.get("call_graph_enriched") or artifacts.get("call_graph") or []
    if isinstance(call_graph, list):
        for rec in call_graph:
            caller_file = rec.get("caller_file") or rec.get("caller_filepath") or rec.get("caller")
            caller_qual = rec.get("caller_qualified") or rec.get("caller_symbol")
            callee_resolved = None
            if isinstance(rec.get("enrichments"), dict):
                callee_resolved = rec["enrichments"].get("resolved_symbol")
            callee_chain = rec.get("callee_chain") or rec.get("callee")
            lineno = rec.get("site_lineno") or rec.get("caller_lineno")
            caller_id = None
            if caller_qual and caller_qual in id_by_symbol:
                caller_id = id_by_symbol[caller_qual]
            else:
                if caller_file:
                    func_ident = f"{caller_file}:{lineno or '0'}"
                    caller_id = canonical_node_id("Function", func_ident)
                    if caller_id not in nodes_index:
                        nodes_index[caller_id] = {"id": caller_id, "type": "Function", "label": os.path.basename(caller_file), "file": caller_file, "lineno": lineno}
            callee_id = None
            if callee_resolved and callee_resolved in id_by_symbol:
                callee_id = id_by_symbol[callee_resolved]
            elif callee_chain:
                short = callee_chain.split(".")[-1]
                cand = None
                for q, nid in id_by_symbol.items():
                    if q.endswith(f".{short}") or q.endswith(short):
                        cand = nid
                        break
                callee_id = cand
            if caller_id and callee_id:
                add_edge(caller_id, callee_id, "CALLS", count=1, prov="call_graph_enriched" if artifacts.get("call_graph_enriched") else "call_graph", attrs={"callee_chain": callee_chain})
            else:
                if caller_file and callee_resolved and callee_resolved in id_by_symbol:
                    callee_file = nodes_index[id_by_symbol[callee_resolved]].get("file")
                    src_file_id = id_by_file.get(caller_file) or canonical_node_id("File", caller_file)
                    tgt_file_id = id_by_file.get(callee_file) or canonical_node_id("File", callee_file)
                    add_edge(src_file_id, tgt_file_id, "CALLS_FILE_LEVEL", count=1, prov="call_graph_fallback")

    # DEFINES (file -> function/class)
    for nid, node in list(nodes_index.items()):
        if not node:
            continue
        if node.get("type") in ("Function", "Class"):
            fpath = node.get("file")
            if fpath:
                fid = id_by_file.get(fpath) or canonical_node_id("File", fpath)
                if fid not in nodes_index:
                    nodes_index[fid] = {"id": fid, "type": "File", "path": fpath, "label": os.path.basename(fpath)}
                add_edge(fid, nid, "DEFINES", count=1, prov="symbol_table")

    # MODIFIED_BY (function -> commit) from function_commits
    func_commits = artifacts.get("function_commits") or {}
    if isinstance(func_commits, dict):
        for func, commits_val in func_commits.items():
            sha_list = []
            if isinstance(commits_val, dict):
                # top_commit might be dict with 'sha'
                tc = commits_val.get("top_commit")
                if isinstance(tc, dict) and tc.get("sha"):
                    sha_list.append(tc.get("sha"))
                # blame_summary: keys often are shas
                bs = commits_val.get("blame_summary")
                if isinstance(bs, dict):
                    for k, v in bs.items():
                        # prioritize keys that look like a sha (hex) or have counts
                        sha_list.append(k)
            elif isinstance(commits_val, list):
                for item in commits_val:
                    if isinstance(item, dict):
                        s = item.get("sha") or item.get("id") or item.get("hash")
                        if s:
                            sha_list.append(s)
                    elif isinstance(item, str):
                        sha_list.append(item)
            func_id = id_by_symbol.get(func) or canonical_node_id("Function", func)
            if func_id not in nodes_index:
                nodes_index[func_id] = {"id": func_id, "type": "Function", "qualified_name": func}
            for sha in sha_list:
                if not sha:
                    continue
                commit_node = id_by_commit.get(sha) or canonical_node_id("Commit", sha)
                if commit_node not in nodes_index:
                    nodes_index[commit_node] = {"id": commit_node, "type": "Commit", "sha": sha}
                add_edge(func_id, commit_node, "MODIFIED_BY", count=1, prov="function_commits")

    # CLOSED_BY (commit -> PR) from commit_to_prs
    commit_to_prs = artifacts.get("commit_to_prs") or {}
    if isinstance(commit_to_prs, dict):
        for sha, pr_list in commit_to_prs.items():
            commit_node = id_by_commit.get(sha) or canonical_node_id("Commit", sha)
            if commit_node not in nodes_index:
                nodes_index[commit_node] = {"id": commit_node, "type": "Commit", "sha": sha}
            # pr_list may be list of dicts or ints
            if isinstance(pr_list, dict):
                # dict mapping? convert to values
                candidates = list(pr_list.values())
            else:
                candidates = pr_list or []
            for pr in candidates:
                prnum = pr.get("number") if isinstance(pr, dict) else pr
                if prnum is None:
                    continue
                pr_node = id_by_pr.get(str(prnum)) or canonical_node_id("PullRequest", str(prnum))
                if pr_node not in nodes_index:
                    nodes_index[pr_node] = {"id": pr_node, "type": "PullRequest", "number": prnum}
                add_edge(commit_node, pr_node, "CLOSED_BY", count=1, prov="commit_to_prs")

    # DEPENDS_ON (file -> package)
    deps_by_file = artifacts.get("dependencies_by_file") or {}
    if isinstance(deps_by_file, dict):
        for file_rel, deps in deps_by_file.items():
            src_fid = id_by_file.get(file_rel) or canonical_node_id("File", file_rel)
            if src_fid not in nodes_index:
                nodes_index[src_fid] = {"id": src_fid, "type": "File", "path": file_rel}
            for d in deps or []:
                name = d.get("name")
                ver = d.get("version") or ""
                if not name:
                    continue
                key = f"{name}@{ver}"
                pkg_node = id_by_package.get(key) or canonical_node_id("Package", key)
                if pkg_node not in nodes_index:
                    nodes_index[pkg_node] = {"id": pkg_node, "type": "Package", "name": name, "version": ver}
                add_edge(src_fid, pkg_node, "DEPENDS_ON", count=1, prov="dependencies_by_file")
    else:
        deps = artifacts.get("dependencies") or []
        if isinstance(deps, list):
            for d in deps:
                file_rel = d.get("file")
                name = d.get("name")
                ver = d.get("version") or ""
                if not name:
                    continue
                pkg_node = id_by_package.get(f"{name}@{ver}") or canonical_node_id("Package", f"{name}@{ver}")
                if pkg_node not in nodes_index:
                    nodes_index[pkg_node] = {"id": pkg_node, "type": "Package", "name": name, "version": ver}
                if file_rel:
                    src_fid = id_by_file.get(file_rel) or canonical_node_id("File", file_rel)
                    add_edge(src_fid, pkg_node, "DEPENDS_ON", count=1, prov="dependencies")

    # MENTIONS: file -> Document via extracted_text match
    extracted_dir = artifacts.get("extracted_text_dir")
    if extracted_dir and os.path.isdir(extracted_dir):
        for root, dirs, files in os.walk(extracted_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, results_dir).replace("\\", "/")
                doc_id = canonical_node_id("Document", rel)
                base = os.path.splitext(f)[0]
                for file_rel, fid in id_by_file.items():
                    if base in os.path.basename(file_rel):
                        add_edge(fid, doc_id, "MENTIONS", count=1, prov="extracted_text_match")

    return edges

# ---------- Write outputs ----------
def edges_to_lists(edges: Dict[Tuple[str,str,str], Dict[str,Any]]):
    out = []
    for (src, tgt, etype), meta in edges.items():
        rec = {
            "source": src,
            "target": tgt,
            "type": etype,
            "count": int(meta["count"]),
            "provenance": dict(meta["provenance"]),
            "attrs": meta.get("attrs", {})
        }
        out.append(rec)
    return out

def write_outputs(results_dir: str, nodes_index: Dict[str, Any], edges_list: List[Dict[str,Any]], logger):
    os.makedirs(results_dir, exist_ok=True)
    nodes_out = os.path.join(results_dir, "nodes.json")
    edges_out = os.path.join(results_dir, "edges.json")
    nodes_csv = os.path.join(results_dir, "kg_nodes.csv")
    edges_csv = os.path.join(results_dir, "kg_edges.csv")

    write_json(nodes_out, list(nodes_index.values()))
    write_json(edges_out, edges_list)
    logger.info("Wrote nodes.json (%d entries) and edges.json (%d entries)", len(nodes_index), len(edges_list))

    # nodes CSV
    with open(nodes_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["id", "type", "label", "properties"])
        for nid, node in nodes_index.items():
            label = node.get("label") or node.get("id")
            properties = dict(node)
            properties.pop("id", None)
            properties.pop("label", None)
            properties.pop("type", None)
            w.writerow([nid, node.get("type"), label, json.dumps(properties, ensure_ascii=False)])

    # edges CSV
    with open(edges_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["source", "target", "type", "count", "provenance", "attrs"])
        for e in edges_list:
            w.writerow([e["source"], e["target"], e["type"], e.get("count", 1), json.dumps(e.get("provenance", {}), ensure_ascii=False), json.dumps(e.get("attrs", {}), ensure_ascii=False)])
    logger.info("Wrote CSV exports: %s, %s", nodes_csv, edges_csv)

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Merge pipeline artifacts into canonical KG (nodes.json, edges.json, CSVs).")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository root (source files).")
    p.add_argument("--results-dir", default=DEFAULT_RESULTS_DIR, help="Directory containing artifacts and to write KG outputs.")
    p.add_argument("--log-level", default="INFO", help="Logging level.")
    return p.parse_args()

def main():
    args = parse_args()
    logger = setup_logger(args.results_dir, getattr(logging, args.log_level.upper(), logging.INFO))
    logger.info("Starting KG merge (robust)")
    logger.info("Repo dir: %s", args.repo_dir)
    logger.info("Results dir: %s", args.results_dir)

    produce_schema_if_missing(args.results_dir, logger)
    artifacts = load_artifacts(args.results_dir, logger)
    nodes_index, id_by_file, id_by_module, id_by_symbol, id_by_commit, id_by_pr, id_by_package = build_nodes_and_index(artifacts, args.repo_dir, args.results_dir, logger)
    edges = build_edges(artifacts, nodes_index, (nodes_index, id_by_file, id_by_module, id_by_symbol, id_by_commit, id_by_pr, id_by_package), args.results_dir, logger)
    edge_list = edges_to_lists(edges)
    write_outputs(args.results_dir, nodes_index, edge_list, logger)

    # Basic summary
    logger.info("KG merge complete: nodes=%d edges=%d", len(nodes_index), len(edge_list))
    outdeg = defaultdict(int)
    indeg = defaultdict(int)
    for e in edge_list:
        outdeg[e["source"]] += e.get("count", 1)
        indeg[e["target"]] += e.get("count", 1)
    top_out = sorted(outdeg.items(), key=lambda x: x[1], reverse=True)[:10]
    top_in = sorted(indeg.items(), key=lambda x: x[1], reverse=True)[:10]
    logger.info("Top 10 sources by out-degree: %s", top_out)
    logger.info("Top 10 targets by in-degree: %s", top_in)

if __name__ == "__main__":
    main()


