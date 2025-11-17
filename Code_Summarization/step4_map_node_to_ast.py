"""
step4_map_node_to_ast.py

Purpose:
  - Map each target (from target_metadata.jsonl) to a canonical AST file and span.
  - Prefer the `preferred` neighbor from target_graph_context.jsonl when present.
  - Fallbacks:
      * Use neighbor start_line/end_line if available
      * Use code_entities_full.json to find function spans by path+name
      * Use nodes.json or ast index if necessary
  - Output: results/target_ast_map.jsonl containing for each target:
      {
        node_id, path, chosen_neighbor, ast_file, ast_node_id, ast_start_line, ast_end_line, reason
      }

Usage:
  - Default (from Code_Summarization directory):
      python step4_map_node_to_ast.py

  - CLI example:
      python step4_map_node_to_ast.py --targets results/target_metadata.jsonl \
         --context results/target_graph_context.jsonl --code-entities ../Testing_GitHub_Code/results/code_entities_full.json \
         --asts ../Testing_GitHub_Code/results/asts --output results/target_ast_map.jsonl

Notes:
  - This script is defensive and logs why a mapping was chosen or which fallbacks were used.
  - It does not modify the KG.

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
DEFAULT_TARGETS = Path("./results/target_metadata.jsonl")
DEFAULT_CONTEXT = Path("./results/target_graph_context.jsonl")
DEFAULT_CODE_ENTITIES = Path("../Testing_GitHub_Code/results/code_entities_full.json")
DEFAULT_AST_DIR = Path("../Testing_GitHub_Code/results/asts")
DEFAULT_NODES = Path("../Testing_GitHub_Code/results/nodes.json")
DEFAULT_OUTPUT = Path("./results/target_ast_map.jsonl")


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("step4_map_node_to_ast")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# -----------------------------
# Helpers
# -----------------------------

def load_json(path: Path) -> Any:
    logger.debug(f"Loading JSON from: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    logger.debug(f"Loading JSONL from: {path}")
    out = []
    with open(path, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f"Loaded {len(out)} records from JSONL")
    return out


def build_code_entity_index(code_entities_raw: Any) -> Dict[str, List[Dict[str, Any]]]:
    """Return mapping path -> list of function records for quick lookup."""
    logger.debug("Building code-entity index (path -> [funcs])")
    records = []
    if isinstance(code_entities_raw, dict) and ("items" in code_entities_raw or "entities" in code_entities_raw):
        records = code_entities_raw.get('items') or code_entities_raw.get('entities')
    elif isinstance(code_entities_raw, list):
        records = code_entities_raw
    elif isinstance(code_entities_raw, dict):
        records = list(code_entities_raw.values())
    out = {}
    for r in records:
        # recognize flattened module with 'functions' list
        rel = r.get('relpath') or r.get('path') or r.get('file') or r.get('filepath')
        if isinstance(r.get('functions'), list) and rel:
            for f in r.get('functions'):
                rec = {
                    'name': f.get('name'),
                    'start_line': f.get('lineno'),
                    'end_line': f.get('end_lineno'),
                    'signature': f.get('sig') or f.get('signature'),
                    'source': f.get('source'),
                    'module_doc': r.get('module_doc')
                }
                out.setdefault(rel, []).append(rec)
        else:
            # if r looks like function-level record
            p = rel
            if p:
                rec = {
                    'name': r.get('name') or r.get('id'),
                    'start_line': r.get('start_line') or r.get('lineno') or r.get('line'),
                    'end_line': r.get('end_line') or r.get('endline') or r.get('end_lineno'),
                    'signature': r.get('signature') or r.get('sig'),
                    'source': r.get('source') or r.get('code')
                }
                out.setdefault(p, []).append(rec)
    logger.debug(f"Indexed code entities for {len(out)} file paths")
    return out


def find_in_code_entities(index: Dict[str, List[Dict[str, Any]]], path: str, name: str) -> Optional[Dict[str, Any]]:
    """Return best-matching function record from code entities for the given path+name."""
    candidates = index.get(path) or index.get(path.replace('./','')) or []
    if not candidates:
        # try suffix-based path matching
        for p, arr in index.items():
            if p.endswith(path) or path.endswith(p):
                candidates = arr
                break
    if not candidates:
        return None
    # prefer exact name
    for c in candidates:
        if (c.get('name') or '').lower() == (name or '').lower():
            return c
    # otherwise prefer largest span
    candidates.sort(key=lambda x: ((x.get('end_line') or 0) - (x.get('start_line') or 0)), reverse=True)
    return candidates[0]


def guess_ast_file_from_path(ast_dir: Path, relpath: str) -> Optional[Path]:
    """Attempt to locate an ast json for the given relative path.
    Common naming: replace .py with .ast.json or <relpath>.json
    """
    if not ast_dir.exists():
        return None
    # candidate names
    p = relpath.replace('\\','/')
    candidates = [p + '.json', p + '.ast.json', Path(p).name + '.json']
    for c in candidates:
        candidate_path = ast_dir / c
        if candidate_path.exists():
            return candidate_path
    # try scanning directory for files that contain the basename
    base = Path(relpath).name
    for f in ast_dir.glob('**/*'):
        if f.is_file() and base in f.name:
            return f
    return None


# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step4_map_node_to_ast.py")
    p.add_argument("--targets", default=str(DEFAULT_TARGETS), help="Path to target_metadata.jsonl")
    p.add_argument("--context", default=str(DEFAULT_CONTEXT), help="Path to target_graph_context.jsonl")
    p.add_argument("--code-entities", default=str(DEFAULT_CODE_ENTITIES), help="Path to code_entities_full.json")
    p.add_argument("--asts", default=str(DEFAULT_AST_DIR), help="Path to asts directory")
    p.add_argument("--nodes", default=str(DEFAULT_NODES), help="Optional nodes.json path for fallback")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output jsonl path")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step4_map_node_to_ast")

    targets_path = Path(args.targets)
    context_path = Path(args.context)
    code_entities_path = Path(args.code_entities)
    ast_dir = Path(args.asts)
    nodes_path = Path(args.nodes)
    out_path = Path(args.output)

    if not targets_path.exists():
        logger.error(f"Targets file not found: {targets_path}")
        return
    if not context_path.exists():
        logger.error(f"Context file not found: {context_path}")
        return

    targets = load_jsonl(targets_path)
    contexts = load_jsonl(context_path)

    # build quick maps
    context_map = {c['node_id']: c for c in contexts}

    code_entities_raw = load_json(code_entities_path) if code_entities_path.exists() else {}
    code_index = build_code_entity_index(code_entities_raw)

    # optional nodes.json for fallback lookups
    nodes_raw = load_json(nodes_path) if nodes_path.exists() else None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, 'w', encoding='utf-8') as outf:
        for t in targets:
            node_id = t.get('node_id')
            path = t.get('path')
            start = t.get('start_line')
            end = t.get('end_line')
            chosen = None
            reason = None
            ast_file = None
            ast_node_id = None
            ast_start = None
            ast_end = None

            ctx = context_map.get(node_id) or {}
            neighbors = ctx.get('neighbors') or []

            # 1) prefer preferred neighbor if present
            pref = None
            for n in neighbors:
                node = n.get('node')
                if node and node.get('preferred'):
                    pref = node
                    break
            if pref:
                chosen = pref
                reason = 'preferred_neighbor'
                ast_start = chosen.get('start_line')
                ast_end = chosen.get('end_line')
                logger.debug(f"Using preferred neighbor for {node_id}: {chosen.get('id')}")
            # 2) otherwise pick first neighbor with start_line/end_line
            if not chosen:
                for n in neighbors:
                    node = n.get('node')
                    if node and node.get('start_line') and node.get('end_line'):
                        chosen = node
                        reason = 'neighbor_with_span'
                        ast_start = chosen.get('start_line')
                        ast_end = chosen.get('end_line')
                        logger.debug(f"Using neighbor span for {node_id}: {chosen.get('id')}")
                        break
            # 3) fallback: search code_entities_full by path+name
            if not chosen:
                cand_name = t.get('candidate_entity_id')
                found = find_in_code_entities(code_index, path, cand_name)
                if found:
                    chosen = {
                        'id': f"code_entities:{cand_name}",
                        'path': path,
                        'label': cand_name,
                        'start_line': found.get('start_line'),
                        'end_line': found.get('end_line'),
                    }
                    reason = 'code_entities_lookup'
                    ast_start = chosen.get('start_line')
                    ast_end = chosen.get('end_line')
                    logger.debug(f"Found span in code_entities for {node_id}: {ast_start}-{ast_end}")

            # 4) try nodes.json for span info if still missing
            if not chosen and nodes_path.exists():
                try:
                    nodes = load_json(nodes_path)
                    # nodes may be list or dict
                    candidate = None
                    if isinstance(nodes, dict):
                        for v in nodes.values():
                            if v.get('path') and v.get('path').replace('\\','/') .endswith(path) and (v.get('name') or '').lower().endswith((t.get('candidate_entity_id') or '').lower()):
                                candidate = v
                                break
                    elif isinstance(nodes, list):
                        for v in nodes:
                            if v.get('path') and v.get('path').replace('\\','/') .endswith(path) and (v.get('name') or '').lower().endswith((t.get('candidate_entity_id') or '').lower()):
                                candidate = v
                                break
                    if candidate:
                        chosen = {
                            'id': candidate.get('id'),
                            'path': candidate.get('path'),
                            'label': candidate.get('name') or candidate.get('label'),
                            'start_line': candidate.get('start_line') or candidate.get('lineno'),
                            'end_line': candidate.get('end_line') or candidate.get('end_lineno')
                        }
                        reason = 'nodes_json_lookup'
                        ast_start = chosen.get('start_line')
                        ast_end = chosen.get('end_line')
                        logger.debug(f"Found span in nodes.json for {node_id}: {ast_start}-{ast_end}")
                except Exception:
                    logger.exception('Failed to search nodes.json')

            # 5) If we have span info, try to find an AST file
            if ast_start and ast_end:
                ast_path = guess_ast_file_from_path(ast_dir, path)
                if ast_path:
                    ast_file = str(ast_path)
                    # try to set ast_node_id (best-effort): use node_id or code_entities id
                    ast_node_id = chosen.get('id') if chosen else node_id
                    logger.debug(f"Resolved AST file for {node_id}: {ast_file}")
                else:
                    logger.debug(f"No AST file located for {path}")
            else:
                # try to still guess ast file even without spans
                ast_path = guess_ast_file_from_path(ast_dir, path)
                if ast_path:
                    ast_file = str(ast_path)
                    reason = reason or 'ast_file_guess_no_span'
                    logger.debug(f"Guessed AST file for {node_id} (no span): {ast_file}")

            out_rec = {
                'node_id': node_id,
                'path': path,
                'chosen_neighbor': chosen,
                'ast_file': ast_file,
                'ast_node_id': ast_node_id,
                'ast_start_line': ast_start,
                'ast_end_line': ast_end,
                'reason': reason
            }
            outf.write(json.dumps(out_rec, ensure_ascii=False) + '\n')
            written += 1

    logger.info(f"Wrote {written} AST mapping records to: {out_path}")


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step4_map_node_to_ast')
        raise
