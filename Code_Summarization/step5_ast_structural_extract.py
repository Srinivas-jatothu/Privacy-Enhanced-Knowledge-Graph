#!/usr/bin/env python3
"""
step5_ast_structural_extract.py

Purpose:
  - For each target mapping in results/target_ast_map.jsonl, load the corresponding
    AST JSON file and extract a compact structural summary useful for KG-driven
    code summarization.

  - Produce results/ast_summaries.jsonl with fields:
      {
        node_id,
        path,
        ast_file,
        ast_node_id,
        signature,      # short signature or function def line if available
        params,         # list of parameter names (best-effort)
        calls,          # list of called function names or node ids found in AST subtree
        control_flow,   # list of control-flow hints e.g. ['for','if','try','with']
        literals,       # short list of string/numeric literals seen
        start_line, end_line,
        reason,
        filled_from     # 'ast' / 'code_entities' / 'none'
      }

Notes:
  - This script assumes AST JSON files are the output of a parser that contains
    node objects with fields like: node_type, lineno, end_lineno, name, attr, value, children.
    The script is defensive and searches for nodes by span if explicit AST node ids are not provided.
  - Uses code_entities_full.json as a fallback for function signature / params.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_TARGET_AST_MAP = Path("./results/target_ast_map.jsonl")
DEFAULT_AST_DIR = Path("../Testing_GitHub_Code/results/asts")
DEFAULT_OUTPUT = Path("./results/ast_summaries.jsonl")
DEFAULT_CODE_ENTITIES = Path("../Testing_GitHub_Code/results/code_entities_full.json")
DEFAULT_NODES = Path("../Testing_GitHub_Code/results/nodes.json")

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("step5_ast_structural_extract")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# -----------------------------
# Helpers: JSON load / traversal
# -----------------------------
def load_json(path: Path) -> Any:
    logger.debug(f"Loading JSON from: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out = []
    logger.debug(f"Loading JSONL from: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f"Loaded {len(out)} records")
    return out

def find_ast_root(ast_json: Any) -> Any:
    """Return the AST root node from common shapes."""
    if isinstance(ast_json, dict) and ('node_type' in ast_json or 'type' in ast_json or 'root' in ast_json):
        return ast_json
    if isinstance(ast_json, dict):
        for k in ('ast', 'root', 'tree'):
            if k in ast_json:
                return ast_json[k]
    return ast_json

def node_has_span(node: Dict[str, Any], start: Optional[int], end: Optional[int]) -> bool:
    try:
        if start is None or end is None:
            return False
        nstart = node.get('lineno') or node.get('start_line') or node.get('start_lineno') or node.get('line')
        nend = node.get('end_lineno') or node.get('end_line') or node.get('endline') or node.get('endline')
        if nstart is None or nend is None:
            return False
        return (int(nstart) <= int(end)) and (int(nend) >= int(start))
    except Exception:
        return False

def normalize_ntype(node: Dict[str, Any]) -> str:
    """
    Safely extract a node-type string from common AST node shapes.
    Returns a lowercase string (or empty string) without raising.
    """
    try:
        raw = node.get('node_type') or node.get('type') or node.get('ast_type') or ''
        if isinstance(raw, dict):
            for k in ('name', 'node_type', 'type', 'ast_type', 'kind'):
                maybe = raw.get(k)
                if isinstance(maybe, str) and maybe:
                    return maybe.lower()
            return str(raw).lower()
        if not isinstance(raw, str):
            return str(raw).lower() if raw else ''
        return raw.lower()
    except Exception:
        return ''

def traverse_and_collect(node: Any, collected: Dict[str, Any], depth: int = 0):
    """
    Recursive traversal that extracts calls, control-flow, literals, params, and signature.
    Defensive against unexpected AST shapes (dicts inside fields, nested dict nodes, etc.)
    """
    if node is None:
        return
    if isinstance(node, dict):
        ntype = normalize_ntype(node)

        # collect function def signature and params
        if 'function' in ntype or 'def' in ntype:
            name = node.get('name') or node.get('id') or node.get('label')
            if name and not collected.get('signature'):
                collected['signature'] = name
            # params may appear under several keys
            args = []
            for k in ('args', 'arguments', 'args_list', 'parameters', 'params'):
                a = node.get(k)
                if isinstance(a, list):
                    for param in a:
                        if isinstance(param, dict):
                            pname = param.get('arg') or param.get('name') or param.get('id')
                            if pname and pname not in args:
                                args.append(pname)
                        elif isinstance(param, str):
                            if param not in args:
                                args.append(param)
                elif isinstance(a, dict):
                    sub = a.get('args') or a.get('parameters') or a.get('params')
                    if isinstance(sub, list):
                        for p in sub:
                            if isinstance(p, dict):
                                pname = p.get('arg') or p.get('name') or p.get('id')
                                if pname and pname not in args:
                                    args.append(pname)
                            elif isinstance(p, str):
                                if p not in args:
                                    args.append(p)
            if args:
                for p in args:
                    if p not in collected['params']:
                        collected['params'].append(p)

        # detect calls
        if 'call' in ntype or 'callexpr' in ntype:
            func = node.get('func') or node.get('value') or node.get('func_name') or node.get('name') or node.get('attr')
            fname = None
            if isinstance(func, dict):
                fname = func.get('attr') or func.get('id') or func.get('name')
            else:
                fname = func
            if fname:
                s = str(fname)
                if s not in collected['calls']:
                    collected['calls'].append(s)

        # control flow
        if any(x in ntype for x in ('for', 'while', 'if', 'try', 'with', 'asyncfor')):
            k = 'for' if 'for' in ntype else 'if' if 'if' in ntype else 'try' if 'try' in ntype else 'with' if 'with' in ntype else ntype
            if k not in collected['control_flow']:
                collected['control_flow'].append(k)

        # literals
        if any(x in ntype for x in ('constant', 'num', 'str', 'string', 'literal')) or 'literal' in ntype:
            val = node.get('value') or node.get('s') or node.get('n') or node.get('v')
            if val is not None:
                s = str(val)
                if s not in collected['literals']:
                    collected['literals'].append(s)

        # traverse common child containers
        for child_key in ('children', 'body', 'args', 'orelse', 'finalbody', 'handlers', 'values', 'targets'):
            child = node.get(child_key)
            if isinstance(child, list):
                for c in child:
                    traverse_and_collect(c, collected, depth+1)
            elif isinstance(child, dict):
                traverse_and_collect(child, collected, depth+1)

        # also traverse any dict-valued fields heuristically (but avoid re-traversing large known fields)
        for k, v in node.items():
            if k in ('children', 'body', 'args', 'value', 'func', 'func_name', 'values', 'targets'):
                continue
            if isinstance(v, dict):
                traverse_and_collect(v, collected, depth+1)
            elif isinstance(v, list):
                for it in v:
                    if isinstance(it, dict) or isinstance(it, list):
                        traverse_and_collect(it, collected, depth+1)

    elif isinstance(node, list):
        for it in node:
            traverse_and_collect(it, collected, depth+1)

# -----------------------------
# Fallback helpers (code_entities)
# -----------------------------
def build_code_entities_index(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    index = {}
    if not path.exists():
        return index
    try:
        logger.debug(f"Loading code entities fallback from: {path}")
        raw = load_json(path)
        # entries likely list of module dicts with 'relpath' and 'functions'
        records = []
        if isinstance(raw, list):
            records = raw
        elif isinstance(raw, dict):
            records = raw.get("items") or raw.get("entities") or list(raw.values())
        for entry in records:
            rel = entry.get("relpath") or entry.get("path") or entry.get("file") or entry.get("filepath")
            if not rel:
                continue
            funcs = entry.get("functions") or []
            for f in funcs:
                index.setdefault(rel, []).append(f)
    except Exception:
        logger.exception("Failed building code entities index")
    logger.debug(f"Indexed code entities for fallback: {len(index)} files")
    return index

def fallback_from_code_entities(index: dict, relpath: str, func_name: Optional[str]):
    """
    Return (signature, params_list) or (None, []) if not found.
    """
    cand = index.get(relpath) or index.get(relpath.replace("./", "")) or []
    if not cand:
        for p, arr in index.items():
            if p.endswith(relpath) or relpath.endswith(p):
                cand = arr
                break
    if not cand:
        return None, []
    # prefer exact name
    for f in cand:
        if (f.get("name") or "").lower() == (func_name or "").lower():
            sig = f.get("sig") or f.get("signature") or None
            params = []
            if sig and "(" in sig and ")" in sig:
                try:
                    inside = sig.split("(", 1)[1].rsplit(")", 1)[0]
                    params = [p.strip().split("=")[0].strip() for p in inside.split(",") if p.strip()]
                except Exception:
                    params = []
            return sig, params
    # fallback: return first entry
    f = cand[0]
    sig = f.get("sig") or f.get("signature") or None
    params = []
    if sig and "(" in sig and ")" in sig:
        try:
            inside = sig.split("(", 1)[1].rsplit(")", 1)[0]
            params = [p.strip().split("=")[0].strip() for p in inside.split(",") if p.strip()]
        except Exception:
            params = []
    return sig, params

# -----------------------------
# Main
# -----------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step5_ast_structural_extract.py")
    p.add_argument("--map", default=str(DEFAULT_TARGET_AST_MAP), help="Path to target_ast_map.jsonl")
    p.add_argument("--asts", default=str(DEFAULT_AST_DIR), help="AST files directory")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSONL path")
    p.add_argument("--code-entities", default=str(DEFAULT_CODE_ENTITIES), help="code_entities_full.json fallback")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step5_ast_structural_extract")

    map_path = Path(args.map)
    ast_dir = Path(args.asts)
    out_path = Path(args.output)
    code_entities_path = Path(args.code_entities)

    if not map_path.exists():
        logger.error(f"Target AST map not found: {map_path}")
        return

    records = load_jsonl(map_path)
    code_entities_index = build_code_entities_index(code_entities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, 'w', encoding='utf-8') as outf:
        for r in records:
            node_id = r.get('node_id')
            path = r.get('path')
            ast_file = r.get('ast_file')
            ast_node_id = r.get('ast_node_id')
            start = r.get('ast_start_line')
            end = r.get('ast_end_line')
            reason = r.get('reason')

            summary = {
                'node_id': node_id,
                'path': path,
                'ast_file': ast_file,
                'ast_node_id': ast_node_id,
                'signature': None,
                'params': [],
                'calls': [],
                'control_flow': [],
                'literals': [],
                'start_line': start,
                'end_line': end,
                'reason': reason,
                'filled_from': None
            }

            extracted_from_ast = False

            if ast_file:
                ast_path = Path(ast_file)
                if not ast_path.exists():
                    ast_path = ast_dir / Path(ast_file).name
                if ast_path.exists():
                    try:
                        ast_json = load_json(ast_path)
                        root = find_ast_root(ast_json)
                        target_node = None

                        # 1) try to find node by ast_node_id (if ast_node_id is a native id)
                        if ast_node_id and isinstance(root, (dict, list)):
                            def search_for_id(node):
                                if not isinstance(node, dict):
                                    return None
                                if node.get('id') == ast_node_id or node.get('node_id') == ast_node_id:
                                    return node
                                for v in node.values():
                                    if isinstance(v, dict):
                                        found = search_for_id(v)
                                        if found:
                                            return found
                                    elif isinstance(v, list):
                                        for it in v:
                                            if isinstance(it, dict):
                                                found = search_for_id(it)
                                                if found:
                                                    return found
                                return None
                            try:
                                target_node = search_for_id(root)
                            except Exception:
                                target_node = None

                        # 2) fallback: find node by span (start/end)
                        if not target_node and start and end:
                            candidates = []
                            def collect_by_span(node):
                                if not isinstance(node, dict):
                                    return
                                try:
                                    if node_has_span(node, start, end):
                                        candidates.append(node)
                                except Exception:
                                    pass
                                for v in node.values():
                                    if isinstance(v, dict):
                                        collect_by_span(v)
                                    elif isinstance(v, list):
                                        for it in v:
                                            if isinstance(it, dict):
                                                collect_by_span(it)
                            try:
                                collect_by_span(root)
                            except Exception:
                                logger.exception('error collecting by span')
                            chosen = None
                            for c in candidates:
                                ntype = normalize_ntype(c)
                                if 'function' in ntype or 'def' in ntype:
                                    chosen = c
                                    break
                            if not chosen and candidates:
                                chosen = candidates[0]
                            target_node = chosen

                        # 3) If we have a target node, extract structural data from it
                        if target_node:
                            collected = {'signature': None, 'params': [], 'calls': [], 'control_flow': [], 'literals': []}
                            traverse_and_collect(target_node, collected)
                            summary['signature'] = collected.get('signature')
                            summary['params'] = collected.get('params') or []
                            summary['calls'] = collected.get('calls') or []
                            summary['control_flow'] = collected.get('control_flow') or []
                            summary['literals'] = (collected.get('literals') or [])[:20]
                            if not summary['start_line']:
                                summary['start_line'] = target_node.get('lineno') or target_node.get('start_line')
                            if not summary['end_line']:
                                summary['end_line'] = target_node.get('end_lineno') or target_node.get('end_line')
                            extracted_from_ast = True
                            summary['filled_from'] = 'ast'
                        else:
                            # no target node found: attempt loose traversal on root
                            collected = {'signature': None, 'params': [], 'calls': [], 'control_flow': [], 'literals': []}
                            traverse_and_collect(root, collected)
                            summary['signature'] = collected.get('signature')
                            summary['params'] = collected.get('params') or []
                            summary['calls'] = (collected.get('calls') or [])[:50]
                            summary['control_flow'] = collected.get('control_flow') or []
                            summary['literals'] = (collected.get('literals') or [])[:20]
                            summary['filled_from'] = 'ast_root_loose'
                    except Exception:
                        logger.exception(f"Failed to parse AST file: {ast_path}")
                else:
                    logger.debug(f"AST path not found or missing: {ast_file}")
            else:
                logger.debug(f"No ast_file for target {node_id}; skipping AST extraction")

            # Fallback: if signature/params are empty, try code_entities_full.json
            if (not summary.get('signature') or not summary.get('params')) and code_entities_index:
                cand_name = None
                chosen_neighbor = r.get('chosen_neighbor') or r.get('chosen') or None
                if chosen_neighbor:
                    cand_name = chosen_neighbor.get('label') or chosen_neighbor.get('id') or cand_name
                if not cand_name:
                    cand_name = r.get('candidate_entity_id') or None
                sig_fb, params_fb = fallback_from_code_entities(code_entities_index, path, cand_name)
                if sig_fb and not summary.get('signature'):
                    summary['signature'] = sig_fb
                if params_fb and (not summary.get('params') or len(summary.get('params', [])) == 0):
                    summary['params'] = params_fb
                # if we filled anything from code_entities and no AST extraction earlier, mark it
                if not extracted_from_ast and (sig_fb or params_fb):
                    summary['filled_from'] = 'code_entities'

            # final safety: ensure lists exist
            for k in ('params', 'calls', 'control_flow', 'literals'):
                if summary.get(k) is None:
                    summary[k] = []

            outf.write(json.dumps(summary, ensure_ascii=False) + '\n')
            written += 1

    logger.info(f"Wrote {written} AST summaries to: {out_path}")

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step5_ast_structural_extract')
        raise
