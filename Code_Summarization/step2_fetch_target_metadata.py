"""
step2_fetch_target_metadata.py

Purpose:
  - Read candidate targets produced by step1 (results/targets_list.jsonl) and
    fetch authoritative metadata for each candidate from node_v2.json and optionally
    nodes_with_commit.json.
  - Produce a newline-delimited JSON file `results/target_metadata.jsonl` with
    enriched metadata used by downstream steps (AST mapping, payload assembly).

Usage:
  - Default (from Code_Summarization directory):
      python step2_fetch_target_metadata.py

  - CLI override example:
      python step2_fetch_target_metadata.py --targets results/targets_list.jsonl \
        --node-v2 ../Testing_GitHub_Code/results/node_v2.json \
        --nodes-with-commit ../Testing_GitHub_Code/results/nodes_with_commit.json \
        --output results/target_metadata.jsonl

Inputs (defaults):
  - ./results/targets_list.jsonl          (produced by step1)
  - ../Testing_GitHub_Code/results/node_v2.json
  - ../Testing_GitHub_Code/results/nodes_with_commit.json  (optional)

Outputs (defaults):
  - ./results/target_metadata.jsonl

Output record shape (per line):
  {
    "node_id": "<id from targets_list or node_v2>",
    "candidate_entity_id": "<id from code_entities_full if present>",
    "path": "<path>",
    "start_line": <int|null>,
    "end_line": <int|null>,

    # authoritative fields fetched from node_v2.json when available
    "canonical": {
      "id": "<node_v2 id>",
      "path": "<node_v2 path>",
      "hash": "<file hash if present>",
      "signature": "<signature if present>",
      "introduced_by_commit": "<sha or null>",
      "introduced_by_pr": "<pr id or null>",
      "modified_by_commits": [ ... ]
    },

    "source_fields": { ... }  # optional raw fields from code_entities_full flattened entity
    "reasons": "...",
    "match_score": 17.1
  }

Notes:
  - The script is defensive: if node_v2.json does not contain the node_id, it will
    attempt to find a matching node by path and name and still produce a best-effort
    canonical block.
  - Detailed DEBUG logging is emitted to help trace mapping decisions.

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
DEFAULT_TARGETS = Path("./results/targets_list.jsonl")
DEFAULT_NODE_V2 = Path("../Testing_GitHub_Code/results/node_v2.json")
DEFAULT_NODES_WITH_COMMIT = Path("../Testing_GitHub_Code/results/nodes_with_commit.json")
DEFAULT_OUTPUT = Path("./results/target_metadata.jsonl")


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("step2_fetch_target_metadata")
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
            try:
                out.append(json.loads(ln))
            except Exception:
                logger.exception("Failed to parse target line; skipping")
    logger.debug(f"Loaded {len(out)} targets")
    return out


def build_node_v2_index(node_v2_raw: Any) -> Dict[str, Dict[str, Any]]:
    # Accept list or dict shapes and return id->node map
    logger.debug("Building node_v2 index (id -> node)")
    if isinstance(node_v2_raw, list):
        nodes = node_v2_raw
    elif isinstance(node_v2_raw, dict) and "nodes" in node_v2_raw:
        nodes = node_v2_raw.get("nodes")
    elif isinstance(node_v2_raw, dict):
        # fallback: values might be nodes
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


def find_node_by_path_and_name(node_index: Dict[str, Dict[str, Any]], path: str, name: str) -> Optional[Dict[str, Any]]:
    # best-effort search in node_v2 for matching path and name
    path_l = (path or "").lower()
    name_l = (name or "").lower()
    for n in node_index.values():
        npath = (n.get("path") or n.get("file") or "").replace("\\", "/").lower()
        nname = (n.get("name") or n.get("label") or n.get("signature") or "").lower()
        if path_l and npath.endswith(path_l) and name_l and (name_l == nname or name_l in nname):
            return n
    # fallback: match by path endswith only
    for n in node_index.values():
        npath = (n.get("path") or n.get("file") or "").replace("\\", "/").lower()
        if path_l and npath.endswith(path_l):
            return n
    return None


# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step2_fetch_target_metadata.py")
    p.add_argument("--targets", default=str(DEFAULT_TARGETS), help="Path to targets_list.jsonl")
    p.add_argument("--node-v2", default=str(DEFAULT_NODE_V2), help="Path to node_v2.json")
    p.add_argument("--nodes-with-commit", default=str(DEFAULT_NODES_WITH_COMMIT), help="Optional path to nodes_with_commit.json")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSONL path")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step2_fetch_target_metadata")

    targets_path = Path(args.targets)
    node_v2_path = Path(args.node_v2)
    nodes_with_commit_path = Path(args.nodes_with_commit)
    out_path = Path(args.output)

    if not targets_path.exists():
        logger.error(f"Targets file not found: {targets_path}")
        return
    if not node_v2_path.exists():
        logger.error(f"node_v2.json not found: {node_v2_path}")
        return

    targets = load_targets(targets_path)
    node_v2_raw = load_json(node_v2_path)
    node_index = build_node_v2_index(node_v2_raw)

    nodes_with_commit = None
    if nodes_with_commit_path.exists():
        try:
            nodes_with_commit = load_json(nodes_with_commit_path)
            # normalize to id->entry map if possible
            if isinstance(nodes_with_commit, dict) and not all(isinstance(k, int) for k in nodes_with_commit.keys()):
                # assume id keyed map
                nodes_with_commit_index = nodes_with_commit
            elif isinstance(nodes_with_commit, list):
                nodes_with_commit_index = {n.get('id'): n for n in nodes_with_commit if n.get('id')}
            else:
                nodes_with_commit_index = {}
            logger.debug(f"Loaded nodes_with_commit entries: {len(nodes_with_commit_index)}")
        except Exception:
            logger.exception("Failed to load nodes_with_commit.json; ignoring")
            nodes_with_commit_index = {}
    else:
        nodes_with_commit_index = {}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, "w", encoding="utf-8") as outf:
        for t in targets:
            node_id = t.get("node_id")
            candidate_entity_id = t.get("candidate_entity_id")
            path = t.get("path")
            start_line = t.get("start_line")
            end_line = t.get("end_line")
            reason = t.get("reason")
            score = t.get("match_score")

            canonical_block = None
            if node_id and node_id in node_index:
                canonical_block = node_index[node_id]
                logger.debug(f"Found canonical node for {node_id}")
            else:
                logger.debug(f"No direct canonical mapping for {node_id}; attempting path+name search")
                candidate_name = candidate_entity_id or ''
                found = find_node_by_path_and_name(node_index, path or '', candidate_name)
                if found:
                    canonical_block = found
                    logger.debug(f"Path+name lookup resolved to canonical id: {found.get('id')}")

            # build canonical metadata extract
            canonical_meta = {}
            if canonical_block:
                canonical_meta['id'] = canonical_block.get('id')
                canonical_meta['path'] = canonical_block.get('path') or canonical_block.get('file')
                canonical_meta['hash'] = canonical_block.get('hash') or canonical_block.get('file_hash')
                canonical_meta['signature'] = canonical_block.get('signature') or canonical_block.get('sig') or canonical_block.get('name')
                canonical_meta['introduced_by_commit'] = canonical_block.get('introduced_by_commit')
                canonical_meta['introduced_by_pr'] = canonical_block.get('introduced_by_pr')
                canonical_meta['modified_by_commits'] = canonical_block.get('modified_by_commits') or canonical_block.get('modified_by') or []
            else:
                # best-effort empty canonical block
                canonical_meta = {
                    'id': None,
                    'path': path,
                    'hash': None,
                    'signature': None,
                    'introduced_by_commit': None,
                    'introduced_by_pr': None,
                    'modified_by_commits': []
                }

            # if nodes_with_commit provides richer commit info, attach it
            nodes_commit_info = nodes_with_commit_index.get(canonical_meta.get('id')) if nodes_with_commit_index else None

            out_rec = {
                'node_id': canonical_meta.get('id') or node_id,
                'candidate_entity_id': candidate_entity_id,
                'path': canonical_meta.get('path') or path,
                'start_line': start_line,
                'end_line': end_line,
                'canonical': canonical_meta,
                'nodes_with_commit': nodes_commit_info,
                'reasons': reason,
                'match_score': score,
            }
            outf.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            written += 1
    logger.info(f"Wrote {written} metadata records to: {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Unhandled error in step2_fetch_target_metadata")
        raise
