"""
step3_retrieve_graph_context.py

Purpose:
  - For each target (from step2 target_metadata.jsonl), retrieve structural neighborhood
    information from the KG: callers, callees, same-file nodes, and documentation nodes.
  - Output a newline-delimited JSON file `results/target_graph_context.jsonl` that
    contains ordered, prioritized neighbor lists and lightweight summaries useful for
    payload assembly.

Usage:
  - Default (from Code_Summarization directory):
      python step3_retrieve_graph_context.py

  - CLI example:
      python step3_retrieve_graph_context.py --targets results/target_metadata.jsonl \
        --edges ../Testing_GitHub_Code/results/edges.json \
        --call-graph ../Testing_GitHub_Code/results/call_graph_enriched.json \
        --node-v2 ../Testing_GitHub_Code/results/node_v2.json \
        --output results/target_graph_context.jsonl --depth 1 --max-neighbors 20

Inputs (defaults):
  - ./results/target_metadata.jsonl
  - ../Testing_GitHub_Code/results/edges.json
  - ../Testing_GitHub_Code/results/call_graph_enriched.json
  - ../Testing_GitHub_Code/results/node_v2.json

Outputs (defaults):
  - ./results/target_graph_context.jsonl

Behavior & Logic:
  - For each target, use `call_graph_enriched.json` (if present) to find callers and
    callees (preferred). If missing, fall back to `edges.json` using edge types (CALLS).
  - Also collect same-file nodes by scanning `node_v2.json` for nodes with the same path.
  - Collect doc-like nodes (README, module doc nodes, extracted_text nodes) by matching
    node types or path patterns.
  - Prioritize results: callers first, then callees, then same-file, then docs.
  - Apply configurable limits: depth (1 by default), max_neighbors per category, and
    overall max neighbors. For multi-hop, perform a simple BFS limited by depth.
  - Attach small context snippets per neighbor (id, path, node_type, label, short_signature)
    to help downstream assembly without loading full files.

Notes:
  - This script is read-only and does not modify the KG.
  - Detailed DEBUG logs are emitted indicating which data source was used for each neighbor.

"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# -----------------------------
# Defaults
# -----------------------------
DEFAULT_TARGETS = Path("./results/target_metadata.jsonl")
DEFAULT_EDGES = Path("../Testing_GitHub_Code/results/edges.json")
DEFAULT_CALL_GRAPH = Path("../Testing_GitHub_Code/results/call_graph_enriched.json")
DEFAULT_NODE_V2 = Path("../Testing_GitHub_Code/results/node_v2.json")
DEFAULT_OUTPUT = Path("./results/target_graph_context.jsonl")
DEFAULT_DEPTH = 1
DEFAULT_MAX_PER_CATEGORY = 10
DEFAULT_MAX_TOTAL = 50


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("step3_retrieve_graph_context")
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


def load_targets(path: Path) -> List[Dict[str, Any]]:
    logger.debug(f"Loading targets from: {path}")
    out = []
    with open(path, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f"Loaded {len(out)} targets")
    return out


def build_edge_index(edges_raw: Any) -> Dict[str, List[Dict[str, Any]]]:
    """Return an index mapping source -> list of outgoing edges and target -> incoming edges
    Edges are expected to have fields like {source, target, type}
    """
    logger.debug("Building edge indices from edges.json")
    if isinstance(edges_raw, dict) and "edges" in edges_raw:
        edges = edges_raw.get("edges")
    elif isinstance(edges_raw, list):
        edges = edges_raw
    else:
        # fallback if shape is unknown
        edges = list(edges_raw.values()) if isinstance(edges_raw, dict) else []

    out_src: Dict[str, List[Dict[str, Any]]] = {}
    out_tgt: Dict[str, List[Dict[str, Any]]] = {}
    for e in edges:
        s = e.get("source") or e.get("from") or e.get("src")
        t = e.get("target") or e.get("to") or e.get("tgt")
        if not s or not t:
            continue
        out_src.setdefault(s, []).append(e)
        out_tgt.setdefault(t, []).append(e)
    logger.debug(f"Indexed edges: {len(out_src)} sources, {len(out_tgt)} targets")
    return {"outgoing": out_src, "incoming": out_tgt}


def build_node_v2_index(node_v2_raw: Any) -> Dict[str, Dict[str, Any]]:
    """Create an id -> node map for node_v2 structures. Accepts list/dict shapes."""
    logger.debug("Building node_v2 index (id -> node)")
    if isinstance(node_v2_raw, list):
        nodes = node_v2_raw
    elif isinstance(node_v2_raw, dict) and "nodes" in node_v2_raw:
        nodes = node_v2_raw.get("nodes")
    elif isinstance(node_v2_raw, dict):
        nodes = list(node_v2_raw.values())
    else:
        nodes = []
    index = {}
    for n in nodes:
        nid = n.get("id")
        if nid:
            index[nid] = n
    logger.debug(f"Indexed {len(index)} canonical nodes")
    return index


def build_callgraph_index(call_raw: Any) -> Dict[str, Dict[str, List[str]]]:
    """Expect call_graph_enriched to contain explicit caller->callee mappings.
    Return structure: { 'callers': {node_id: [caller_ids...]}, 'callees': {node_id: [callee_ids...]} }
    Supports a few input shapes.
    """
    logger.debug("Building call-graph index (caller <-> callee)")
    callers_idx: Dict[str, List[str]] = {}
    callees_idx: Dict[str, List[str]] = {}

    # Common shapes: list of {caller, callee} or dict with 'callers'/'callees'
    if isinstance(call_raw, dict):
        # try known shapes
        if "callers" in call_raw and "callees" in call_raw:
            callers_idx = call_raw.get("callers") or {}
            callees_idx = call_raw.get("callees") or {}
        elif "edges" in call_raw:
            # edges like CALLS in call graph
            for e in call_raw.get("edges", []):
                if e.get("type") and e.get("type").upper() == "CALLS":
                    s = e.get("source")
                    t = e.get("target")
                    if s and t:
                        callees_idx.setdefault(s, []).append(t)
                        callers_idx.setdefault(t, []).append(s)
        else:
            # attempt to parse list-like values
            maybe_list = call_raw.get("call_graph") or call_raw.get("edges")
            if maybe_list and isinstance(maybe_list, list):
                for e in maybe_list:
                    if e.get("type") and e.get("type").upper() == "CALLS":
                        s = e.get("source")
                        t = e.get("target")
                        if s and t:
                            callees_idx.setdefault(s, []).append(t)
                            callers_idx.setdefault(t, []).append(s)
    elif isinstance(call_raw, list):
        for e in call_raw:
            if e.get("type") and e.get("type").upper() == "CALLS":
                s = e.get("source")
                t = e.get("target")
                if s and t:
                    callees_idx.setdefault(s, []).append(t)
                    callers_idx.setdefault(t, []).append(s)
    logger.debug(f"Call graph: {len(callers_idx)} nodes have callers; {len(callees_idx)} nodes have callees")
    return {"callers": callers_idx, "callees": callees_idx}


def node_summary(node: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact summary extracted from a node_v2 record or similar."""
    s = {
        "id": node.get("id"),
        "path": node.get("path") or node.get("file") or node.get("relpath") ,
        "type": node.get("type") or node.get("kind") or ("Function" if node.get("name") else "File"),
        "label": node.get("label") or node.get("name") or node.get("id"),
        "signature": node.get("signature") or node.get("sig") or node.get("name"),
    }
    return s


def collect_same_file_nodes(node_index: Dict[str, Dict[str, Any]], target_path: str, exclude_id: Optional[str]=None) -> List[Dict[str, Any]]:
    out = []
    target_low = (target_path or "").lower()
    for nid, n in node_index.items():
        np = (n.get("path") or n.get("file") or n.get("relpath") or "").replace("\\", "/")
        if not np:
            continue
        if np.lower().endswith(target_low) or target_low in np.lower():
            if exclude_id and nid == exclude_id:
                continue
            out.append(node_summary(n))
    return out


def collect_doc_nodes(node_index: Dict[str, Dict[str, Any]], target_path: str) -> List[Dict[str, Any]]:
    """Heuristic: find README, docs, or extracted_text nodes near the target path."""
    docs = []
    targ_dir = "/".join((target_path or "").split("/")[:-1])
    for nid, n in node_index.items():
        label = (n.get("label") or "").lower()
        path = (n.get("path") or n.get("file") or "").lower()
        if any(x in label for x in ("readme","read_me","module_doc","doc","description")):
            if targ_dir and path.startswith(targ_dir):
                docs.append(node_summary(n))
        elif path.endswith("readme.md") or path.endswith("readme"):
            docs.append(node_summary(n))
    return docs


def bfs_collect_neighbors(seed_ids: List[str], callers_idx: Dict[str,List[str]], callees_idx: Dict[str,List[str]], depth: int) -> Tuple[Set[str], Set[str]]:
    """Perform limited BFS up to `depth` for callers and callees separately. Returns (callers_set, callees_set)"""
    callers_seen: Set[str] = set()
    callees_seen: Set[str] = set()

    # BFS for callers (walk incoming edges)
    frontier = set(seed_ids)
    for d in range(depth):
        next_frontier = set()
        for nid in frontier:
            for caller in callers_idx.get(nid, []):
                if caller not in callers_seen and caller not in seed_ids:
                    callers_seen.add(caller)
                    next_frontier.add(caller)
        frontier = next_frontier
    # BFS for callees (walk outgoing edges)
    frontier = set(seed_ids)
    for d in range(depth):
        next_frontier = set()
        for nid in frontier:
            for callee in callees_idx.get(nid, []):
                if callee not in callees_seen and callee not in seed_ids:
                    callees_seen.add(callee)
                    next_frontier.add(callee)
        frontier = next_frontier
    return callers_seen, callees_seen


# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step3_retrieve_graph_context.py")
    p.add_argument("--targets", default=str(DEFAULT_TARGETS), help="Path to target_metadata.jsonl")
    p.add_argument("--edges", default=str(DEFAULT_EDGES), help="Path to edges.json")
    p.add_argument("--call-graph", default=str(DEFAULT_CALL_GRAPH), help="Path to call_graph_enriched.json")
    p.add_argument("--node-v2", default=str(DEFAULT_NODE_V2), help="Path to node_v2.json")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="output JSONL path")
    p.add_argument("--depth", type=int, default=DEFAULT_DEPTH, help="hop depth for BFS")
    p.add_argument("--max-per-category", type=int, default=DEFAULT_MAX_PER_CATEGORY, help="limit per category (callers/callees/samefile/docs)")
    p.add_argument("--max-total", type=int, default=DEFAULT_MAX_TOTAL, help="overall neighbor cap")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step3_retrieve_graph_context")

    targets_path = Path(args.targets)
    edges_path = Path(args.edges)
    call_graph_path = Path(args.call_graph)
    node_v2_path = Path(args.node_v2)
    out_path = Path(args.output)
    depth = int(args.depth)
    max_per = int(args.max_per_category)
    max_total = int(args.max_total)

    if not targets_path.exists():
        logger.error(f"Targets file not found: {targets_path}")
        return
    if not node_v2_path.exists():
        logger.error(f"node_v2.json not found: {node_v2_path}")
        return

    targets = load_targets(targets_path)
    node_v2_raw = load_json(node_v2_path)
    node_index = build_node_v2_index(node_v2_raw)

    # load edges & call graph (either may be missing)
    edges_raw = load_json(edges_path) if edges_path.exists() else []
    edge_idx = build_edge_index(edges_raw) if edges_raw else {"outgoing":{}, "incoming":{}}
    call_raw = load_json(call_graph_path) if call_graph_path.exists() else []
    call_idx = build_callgraph_index(call_raw) if call_raw else {"callers":{}, "callees":{}}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, "w", encoding="utf-8") as outf:
        for t in targets:
            seed_id = t.get('node_id')
            seed_path = t.get('path')
            seed_start = t.get('start_line')
            seed_end = t.get('end_line')

            logger.debug(f"Processing target: {seed_id} ({seed_path})")

            callers = []
            callees = []
            same_file = []
            docs = []

            # Prefer call_graph index first
            if seed_id in call_idx.get('callers', {}) or seed_id in call_idx.get('callees', {}):
                c_callers = call_idx.get('callers', {}).get(seed_id, [])
                c_callees = call_idx.get('callees', {}).get(seed_id, [])
                logger.debug(f"Found {len(c_callers)} callers and {len(c_callees)} callees from call_graph_enriched")
                # collect summaries
                for cid in c_callers:
                    if cid in node_index:
                        callers.append(node_summary(node_index[cid]))
                for did in c_callees:
                    if did in node_index:
                        callees.append(node_summary(node_index[did]))
            else:
                # fallback to edges.json using incoming/outgoing indexes
                incoming = edge_idx.get('incoming', {}).get(seed_id, [])
                outgoing = edge_idx.get('outgoing', {}).get(seed_id, [])
                # consider edges of type CALLS as higher priority
                for e in incoming:
                    if (e.get('type') or '').upper() == 'CALLS':
                        src = e.get('source')
                        if src and src in node_index:
                            callers.append(node_summary(node_index[src]))
                for e in outgoing:
                    if (e.get('type') or '').upper() == 'CALLS':
                        tgt = e.get('target')
                        if tgt and tgt in node_index:
                            callees.append(node_summary(node_index[tgt]))
                logger.debug(f"Fallback edges: callers={len(callers)}, callees={len(callees)})")

            # If BFS depth >1, run limited BFS to gather neighbors
            if depth > 1:
                seed_list = [seed_id]
                b_callers, b_callees = bfs_collect_neighbors(seed_list, call_idx.get('callers', {}), call_idx.get('callees', {}), depth)
                for cid in list(b_callers)[:max_per]:
                    if cid in node_index:
                        callers.append(node_summary(node_index[cid]))
                for did in list(b_callees)[:max_per]:
                    if did in node_index:
                        callees.append(node_summary(node_index[did]))

            # same-file nodes
            same_file_nodes = collect_same_file_nodes(node_index, seed_path, exclude_id=seed_id)
            same_file = same_file_nodes[:max_per]

            # docs
            doc_nodes = collect_doc_nodes(node_index, seed_path)
            docs = doc_nodes[:max_per]

            # deduplicate preserving order with priority: callers > callees > same-file > docs
            seen = set()
            prioritized = []
            for cat, lst in (('callers',callers), ('callees',callees), ('same_file',same_file), ('docs',docs)):
                for n in lst:
                    nid = n.get('id')
                    if nid and nid not in seen:
                        seen.add(nid)
                        prioritized.append({'category':cat,'node':n})
            # cap overall
            prioritized = prioritized[:max_total]

            out_rec = {
                'node_id': seed_id,
                'path': seed_path,
                'start_line': seed_start,
                'end_line': seed_end,
                'neighbors': prioritized,
                'meta': {
                    'num_callers': len(callers),
                    'num_callees': len(callees),
                    'num_same_file': len(same_file),
                    'num_docs': len(docs),
                }
            }
            outf.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            written += 1
            logger.debug(f"Wrote context for {seed_id}: callers={len(callers)}, callees={len(callees)}, same_file={len(same_file)}, docs={len(docs)}")

    logger.info(f"Wrote {written} context records to: {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Unhandled error in step3_retrieve_graph_context")
        raise
