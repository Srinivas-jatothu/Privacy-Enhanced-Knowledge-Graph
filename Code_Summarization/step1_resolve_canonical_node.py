"""
step1_resolve_canonical_node.py

Purpose:
  - Map a normalized user request (function_name + file_path) to one or more canonical
    function nodes present in the KG (node_v2.json / code_entities_full.json).
  - Produce a newline-delimited JSON file with candidate targets and reasons for selection.

Usage:
  - Default (from Code_Summarization directory):
      python step1_resolve_canonical_node.py

  - CLI override examples:
      python step1_resolve_canonical_node.py --request results/summarizer_request.json \
        --kg-dir "../Testing_GitHub_Code/results" --output results/targets_list.jsonl --top-k 10

Inputs (defaults):
  - Code_Summarization/results/summarizer_request.json
  - ../Testing_GitHub_Code/results/code_entities_full.json
  - ../Testing_GitHub_Code/results/node_v2.json

Outputs (defaults):
  - Code_Summarization/results/targets_list.jsonl
    Each line is a JSON object: { node_id, path, start_line, end_line, reason, match_score }

Improvements in this version:
  - Handles module-shaped entities where code_entities_full.json stores a module record
    with a `functions` list (each function has name, lineno, end_lineno, sig, source).
    The script flattens module -> function entries into per-function candidate entities.
  - Adds a --top-k argument to always emit up to K best candidates even if scores are low.
  - Adds relaxed fallback matching when exact path heuristics fail:
      * basename contains
      * path contains (anywhere)
      * name substring matches
  - When no candidates are found by heuristics, the script now emits the top-K scored entities
    (global best-effort) and writes debugging info to the log so you can inspect why.

"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


# -----------------------------
# Defaults — adjust if you need
# -----------------------------
DEFAULT_REQUEST_PATH = Path("./results/summarizer_request.json")
DEFAULT_KG_DIR = Path("../Testing_GitHub_Code/results")
DEFAULT_CODE_ENTITIES = DEFAULT_KG_DIR / "code_entities_full.json"
DEFAULT_NODE_V2 = DEFAULT_KG_DIR / "node_v2.json"
DEFAULT_OUTPUT = Path("./results/targets_list.jsonl")
DEFAULT_TOP_K = 10


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("step1_resolve_canonical_node")
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


def simple_name_fields(entity: Dict[str, Any]) -> List[str]:
    """Return candidate text fields that may contain the function's name."""
    fields = []
    for k in ("name", "id", "qualname", "qualified_name", "signature", "label", "sig"):
        v = entity.get(k)
        if isinstance(v, str) and v:
            fields.append(v)
    return fields


def entity_path(entity: Dict[str, Any]) -> Optional[str]:
    # Common fields where path might live
    for k in ("path", "relpath", "file", "filepath", "filename", "abs_path"):
        v = entity.get(k)
        if isinstance(v, str) and v:
            return v.replace("\\", "/")
    return None


def match_score_for(entity: Dict[str, Any], req_name: str, req_path: str) -> float:
    """Produce a simple heuristic score (higher=better)."""
    score = 0.0
    ent_path = (entity_path(entity) or "").lower()
    req_path_l = req_path.lower()
    # exact path match is strong
    if ent_path and req_path_l and ent_path.endswith(req_path_l):
        score += 5.0
    # path contains
    if ent_path and req_path_l and req_path_l in ent_path:
        score += 2.0
    # match on last two path segments (robust to prefixes)
    try:
        if ent_path and req_path_l:
            ent_tail = "/".join(ent_path.split("/")[-2:])
            req_tail = "/".join(req_path_l.split("/")[-2:])
            if ent_tail == req_tail:
                score += 3.5
    except Exception:
        pass
    # exact name match on common fields
    for f in simple_name_fields(entity):
        f_l = f.lower()
        if f_l == req_name.lower():
            score += 3.0
        elif req_name.lower() in f_l:
            score += 1.5
    # prefer entries that have line/span info (more precise)
    if entity.get("start_line") or entity.get("lineno") or entity.get("end_line") or entity.get("end_lineno"):
        score += 0.5
    # small boost if file extension matches python (prefer .py but this is optional)
    if ent_path.endswith('.py'):
        score += 0.1
    return score


def find_candidates(entities: List[Dict[str, Any]], req_name: str, req_path: str) -> List[Dict[str, Any]]:
    """Return candidate entities sorted by score (desc)."""
    scored = []
    for e in entities:
        score = match_score_for(e, req_name, req_path)
        if score > 0:
            scored.append((score, e))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in scored]


def relaxed_fallback_search(entities: List[Dict[str, Any]], req_name: str, req_path: str) -> List[Dict[str, Any]]:
    """Perform more relaxed matching if strict heuristics return nothing.

    Strategies:
      - match by basename (e.g., user path 'dags/src/a.py' -> basename 'a.py')
      - match if req_path fragment appears anywhere in entity path
      - match by name substring across name-like fields
    """
    candidates = []
    req_basename = Path(req_path).name.lower()
    req_path_l = req_path.lower()
    for e in entities:
        ent_path = (entity_path(e) or "").lower()
        names = [s.lower() for s in simple_name_fields(e)]
        reason = None
        if req_basename and req_basename in ent_path:
            reason = f"basename match: {req_basename}" 
        elif req_path_l and req_path_l in ent_path:
            reason = f"path contains fragment: {req_path_l}"
        else:
            for n in names:
                if req_name.lower() == n:
                    reason = f"exact name match (global)"
                    break
                if req_name.lower() in n:
                    reason = f"name substring match (global): {n}"
                    break
        if reason:
            e_copy = dict(e)
            e_copy["_relaxed_reason"] = reason
            candidates.append(e_copy)
    # score these using the standard score function and sort
    candidates.sort(key=lambda ent: match_score_for(ent, req_name, req_path), reverse=True)
    return candidates


def resolve_canonical_node_id(node_v2: List[Dict[str, Any]], candidate: Dict[str, Any]) -> Optional[str]:
    """Try to resolve candidate entity to a canonical node id in node_v2.json.

    Heuristics:
      - match on id if candidate.id looks like a node id present in node_v2
      - match on path and name
      - otherwise return None
    """
    cand_id = candidate.get("id")
    cand_path = entity_path(candidate) or ""
    cand_names = simple_name_fields(candidate)

    # Build quick lookup maps on the fly (could be cached for large KG)
    for n in node_v2:
        # match by id directly
        if cand_id and n.get("id") == cand_id:
            return n.get("id")
        # match by path + name
        npath = (n.get("path") or n.get("file") or "").replace("\\", "/")
        if cand_path and npath.endswith(cand_path.replace("\\", "/")):
            # check name fields
            for nf in (n.get("name"), n.get("label"), n.get("signature")):
                if nf and any(cand_name == nf or cand_name in nf for cand_name in cand_names):
                    return n.get("id")
    return None


def flatten_module_entities(raw_entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert module-shaped entities (with 'functions' list) into per-function candidate entities.

    Example input entity shape (module):
      {
        'relpath': 'dags/src/customers_behavior.py',
        'abs_path': 'C:/.../dags/src/customers_behavior.py',
        'functions': [ { 'name': 'customers_behavior', 'lineno': 16, 'end_lineno': 69, 'sig': '(... )', 'source': 'def ...' }, ... ]
      }

    Output: list of entities with fields: id, path, name, start_line, end_line, signature, module_doc
    """
    out = []
    for e in raw_entities:
        # if entity already looks like a function-level record, keep as-is
        # detect module-shaped by presence of 'functions' list
        if isinstance(e, dict) and isinstance(e.get('functions'), list) and e.get('relpath'):
            base_path = e.get('relpath') or entity_path(e) or ''
            module_doc = e.get('module_doc')
            for f in e.get('functions'):
                ent = {
                    'id': f.get('name') and f.get('name') or None,
                    'candidate_kind': 'flattened_from_module',
                    'path': base_path,
                    'relpath': base_path,
                    'abs_path': e.get('abs_path'),
                    'name': f.get('name'),
                    'signature': f.get('sig') or f.get('signature'),
                    'start_line': f.get('lineno') or f.get('start_line'),
                    'end_line': f.get('end_lineno') or f.get('end_line'),
                    'doc': f.get('doc'),
                    'source': f.get('source'),
                    'module_doc': module_doc,
                }
                out.append(ent)
        else:
            # leave the entity as-is (but normalize common path fields)
            normalized = dict(e)
            p = entity_path(e)
            if p:
                normalized['path'] = p
            out.append(normalized)
    return out


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step1_resolve_canonical_node.py")
    p.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to summarizer_request.json")
    p.add_argument("--kg-dir", default=str(DEFAULT_KG_DIR), help="Path to Testing_GitHub_Code/results folder")
    p.add_argument("--code-entities", default=str(DEFAULT_CODE_ENTITIES), help="Path to code_entities_full.json")
    p.add_argument("--node-v2", default=str(DEFAULT_NODE_V2), help="Path to node_v2.json")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSONL path for targets")
    p.add_argument("--top-k", type=int, default=DEFAULT_TOP_K, help="Maximum candidates to emit (best-effort)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step1_resolve_canonical_node")

    req_path = Path(args.request)
    kg_dir = Path(args.kg_dir)
    code_entities_path = Path(args.code_entities)
    node_v2_path = Path(args.node_v2)
    out_path = Path(args.output)
    top_k = args.top_k

    # load request
    if not req_path.exists():
        logger.error(f"Request file not found: {req_path}")
        return
    request = load_json(req_path)
    req_name = request.get("function_name", "").strip()
    req_file = request.get("file_path", "").strip().replace("\\", "/")

    logger.debug(f"Request: function={req_name!r}, file={req_file!r}")

    # load KG inputs
    if not code_entities_path.exists():
        code_entities_path = kg_dir / "code_entities_full.json"
    if not node_v2_path.exists():
        node_v2_path = kg_dir / "node_v2.json"

    if not code_entities_path.exists() or not node_v2_path.exists():
        logger.error("Required KG files not found. Please ensure code_entities_full.json and node_v2.json are present in the KG dir")
        return

    raw_code_entities = load_json(code_entities_path)
    node_v2 = load_json(node_v2_path)

    # normalize entities list extraction — handle if code_entities is dict with 'items' or a list
    if isinstance(raw_code_entities, dict) and ("items" in raw_code_entities or "entities" in raw_code_entities):
        raw_entities = raw_code_entities.get("items") or raw_code_entities.get("entities")
    elif isinstance(raw_code_entities, list):
        raw_entities = raw_code_entities
    else:
        # fallback: interpret dict values as list
        raw_entities = list(raw_code_entities.values())

    # Similarly node_v2 could be dict or list
    if isinstance(node_v2, dict) and "nodes" in node_v2:
        node_v2_list = node_v2.get("nodes")
    elif isinstance(node_v2, list):
        node_v2_list = node_v2
    else:
        node_v2_list = list(node_v2.values())

    logger.debug(f"Loaded {len(raw_entities)} raw code entities; {len(node_v2_list)} canonical nodes")

    # Flatten module-shaped entities into per-function entries
    entities = flatten_module_entities(raw_entities)
    logger.debug(f"After flattening, {len(entities)} candidate entities available")

    # Find candidates using heuristic scoring
    candidates = find_candidates(entities, req_name, req_file)
    logger.debug(f"Found {len(candidates)} candidates after heuristic scoring")

    # If none found with path matching, try relaxed fallback heuristics
    if not candidates:
        logger.debug("No strong candidates found with strict heuristics; performing relaxed fallback search")
        candidates = relaxed_fallback_search(entities, req_name, req_file)
        logger.debug(f"Relaxed fallback search returned {len(candidates)} candidates")

    # If still none, emit global top-K by score (best-effort)
    if not candidates:
        logger.debug("Relaxed search returned 0; scoring all entities globally and emitting top-K best-effort")
        scored_all = [(match_score_for(e, req_name, req_file), e) for e in entities]
        scored_all.sort(key=lambda x: x[0], reverse=True)
        candidates = [e for s, e in scored_all[:top_k] if s >= 0]
        logger.debug(f"Global top-K produced {len(candidates)} candidates (including score=0 entries if necessary)")

    # Prepare output directory
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results = []
    with open(out_path, "w", encoding="utf-8") as outf:
        for ent in candidates[:top_k]:
            ent_path = ent.get('path') or entity_path(ent) or ""
            start_line = ent.get("start_line") or ent.get("lineno") or ent.get("line")
            end_line = ent.get("end_line") or ent.get("endline") or ent.get('end_lineno')
            reason_parts = []
            # build reason
            if ent.get("_relaxed_reason"):
                reason_parts.append(ent.get("_relaxed_reason"))
            if ent_path and req_file and ent_path.endswith(req_file):
                reason_parts.append("path matched (endswith)")
            names = simple_name_fields(ent)
            if req_name in names:
                reason_parts.append("exact name match")
            else:
                # check substring
                for nm in names:
                    if req_name in nm:
                        reason_parts.append(f"name substring match: {nm}")
                        break
            # score
            score = match_score_for(ent, req_name, req_file)
            # try to resolve canonical node id
            canonical_id = resolve_canonical_node_id(node_v2_list, ent)
            node_id = canonical_id or ent.get("id") or f"entity_unresolved_{abs(hash(str(ent))) % (10**9)}"

            out_rec = {
                "node_id": node_id,
                "candidate_entity_id": ent.get("id"),
                "path": ent_path,
                "start_line": start_line,
                "end_line": end_line,
                "reason": "; ".join(reason_parts) if reason_parts else "heuristic_match",
                "match_score": score,
            }
            outf.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            results.append(out_rec)
            logger.debug(f"Candidate => id={out_rec['node_id']}, score={score}, reason={out_rec['reason']}")

    logger.info(f"Wrote {len(results)} target candidates to: {out_path}")

    # If no candidates at all, log some helpful debugging info to help diagnosis
    if not results:
        logger.warning("No candidate targets found. Helpful debug hints:")
        logger.warning(f"Requested function: {req_name!r}")
        logger.warning(f"Requested file: {req_file!r}")
        # show top 10 entity paths and names to help you inspect
        sample_preview = []
        for e in entities[:min(10, len(entities))]:
            sample_preview.append({
                "id": e.get("id"),
                "path": e.get('path') or entity_path(e),
                "names": simple_name_fields(e)[:3]
            })
        logger.warning(f"Sample entities (first 10): {json.dumps(sample_preview, ensure_ascii=False)}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Unhandled error in step1_resolve_canonical_node")
        raise
