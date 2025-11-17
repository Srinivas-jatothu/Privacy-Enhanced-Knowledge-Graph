"""
step8_re_rank_neighbors.py

Purpose:
  - Re-rank and select top-K neighbor nodes for each target to include in the LLM payload.
  - Uses a hybrid heuristic score: graph distance (callers weighted), path similarity, and
    optional semantic similarity (if embeddings index provided).

Inputs (defaults):
  - results/target_metadata.jsonl
  - results/target_graph_context.jsonl
  - ../Testing_GitHub_Code/results/edges.json    (optional, for graph distance)
  - optional embeddings file path (jsonl mapping node_id -> embedding vector)

Outputs:
  - results/payloads_ranked.jsonl  (top-K neighbors per target with scores)
  - results/retrieval_debug/ (debug artifacts)

Scoring logic (configurable weights):
  score = w_callers * is_caller + w_callee * is_callee + w_samefile * same_file_score
        + w_name_sim * name_similarity + w_graph_dist * (1 / (1 + distance))
  - is_caller/is_callee are binary indicators (1/0) but callers are given higher weight.
  - name_similarity: simple string-based score (shared tokens ratio)
  - distance: shortest-path length between nodes (if edges.json provided); if not, omitted.
  - if an embeddings index is provided, a semantic similarity (cosine) component is added.

Behavior:
  - For each target, compute scores for neighbors returned in target_graph_context.jsonl
  - Add neighbors from edges.json within 1-2 hops if needed
  - Produce top-K (default K=8) neighbors and save them in results/payloads_ranked.jsonl

Usage:
  python step8_re_rank_neighbors.py

CLI options:
  --targets, --context, --edges, --embeddings, --out, --k, --weights

"""

import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_TARGETS = Path('./results/target_metadata.jsonl')
DEFAULT_CONTEXT = Path('./results/target_graph_context.jsonl')
DEFAULT_EDGES = Path('../Testing_GitHub_Code/results/edges.json')
DEFAULT_OUTPUT = Path('./results/payloads_ranked.jsonl')
DEFAULT_DEBUG_DIR = Path('./results/retrieval_debug')
DEFAULT_K = 8

# default weights
DEFAULT_WEIGHTS = {
    'w_callers': 3.0,
    'w_callees': 1.5,
    'w_samefile': 1.0,
    'w_name_sim': 1.0,
    'w_graph_dist': 1.0,
    'w_emb_sim': 2.0
}

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger('step8_re_rank_neighbors')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# -----------------------------
# Helpers
# -----------------------------

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out = []
    logger.debug(f'Loading JSONL from: {path}')
    with open(path, 'r', encoding='utf-8') as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f'Loaded {len(out)} records from {path.name}')
    return out


def load_json(path: Path) -> Any:
    logger.debug(f'Loading JSON from: {path}')
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def build_edge_graph(edges_raw: Any) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """Return adjacency lists (src->targets, target->srcs)"""
    src_map = defaultdict(list)
    tgt_map = defaultdict(list)
    if isinstance(edges_raw, list):
        for e in edges_raw:
            s = e.get('source') or e.get('src') or e.get('from')
            t = e.get('target') or e.get('tgt') or e.get('to')
            if s and t:
                src_map[s].append(t)
                tgt_map[t].append(s)
    elif isinstance(edges_raw, dict):
        for e in edges_raw.get('edges', []) if edges_raw.get('edges') else []:
            s = e.get('source'); t = e.get('target')
            if s and t:
                src_map[s].append(t); tgt_map[t].append(s)
    logger.debug(f'Built edge graph: {len(src_map)} sources')
    return src_map, tgt_map


def shortest_path_length(src_map: Dict[str, List[str]], start: str, goal: str, max_hops: int = 4) -> Optional[int]:
    if start == goal:
        return 0
    q = deque([(start, 0)])
    seen = {start}
    while q:
        node, d = q.popleft()
        if d >= max_hops:
            continue
        for nb in src_map.get(node, []):
            if nb == goal:
                return d + 1
            if nb not in seen:
                seen.add(nb)
                q.append((nb, d+1))
    return None


def name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta = set([x for x in a.replace('.', ' ').replace('_',' ').split() if x])
    tb = set([x for x in b.replace('.', ' ').replace('_',' ').split() if x])
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    denom = max(len(ta), len(tb))
    return inter / denom


def cosine_sim(vec1: List[float], vec2: List[float]) -> float:
    try:
        dot = sum(x*y for x,y in zip(vec1, vec2))
        mag1 = math.sqrt(sum(x*x for x in vec1))
        mag2 = math.sqrt(sum(y*y for y in vec2))
        if mag1==0 or mag2==0:
            return 0.0
        return dot / (mag1*mag2)
    except Exception:
        return 0.0


def load_embeddings(path: Path) -> Dict[str, List[float]]:
    if not path.exists():
        return {}
    emb = {}
    try:
        if path.suffix.lower() in ('.json', '.js'):
            raw = load_json(path)
            if isinstance(raw, dict):
                for k,v in raw.items():
                    emb[k] = v
            elif isinstance(raw, list):
                for it in raw:
                    if isinstance(it, dict) and it.get('id') and it.get('embedding'):
                        emb[it['id']] = it['embedding']
        else:
            # try jsonl
            with open(path, 'r', encoding='utf-8') as fh:
                for ln in fh:
                    if not ln.strip():
                        continue
                    j = json.loads(ln)
                    if j.get('id') and j.get('embedding'):
                        emb[j['id']] = j['embedding']
    except Exception:
        logger.exception('Failed loading embeddings')
    logger.debug(f'Loaded embeddings for {len(emb)} nodes')
    return emb

# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog='step8_re_rank_neighbors.py')
    p.add_argument('--targets', default=str(DEFAULT_TARGETS))
    p.add_argument('--context', default=str(DEFAULT_CONTEXT))
    p.add_argument('--edges', default=str(DEFAULT_EDGES))
    p.add_argument('--embeddings', default='')
    p.add_argument('--out', default=str(DEFAULT_OUTPUT))
    p.add_argument('--debug-dir', default=str(DEFAULT_DEBUG_DIR))
    p.add_argument('-k', type=int, default=DEFAULT_K)
    p.add_argument('--weights', default=None, help='JSON string of weights to override defaults')
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug('Starting step8_re_rank_neighbors')

    targets_path = Path(args.targets)
    context_path = Path(args.context)
    edges_path = Path(args.edges)
    emb_path = Path(args.embeddings) if args.embeddings else None
    out_path = Path(args.out)
    debug_dir = Path(args.debug_dir)
    K = int(args.k)

    weights = DEFAULT_WEIGHTS.copy()
    if args.weights:
        try:
            w = json.loads(args.weights)
            weights.update(w)
        except Exception:
            logger.exception('Failed to parse weights JSON; using defaults')

    if not targets_path.exists() or not context_path.exists():
        logger.error('Targets or context not found; aborting')
        return

    targets = load_jsonl(targets_path)
    contexts = {r['node_id']: r for r in load_jsonl(context_path)}

    edges_raw = load_json(edges_path) if edges_path.exists() else None
    src_map, tgt_map = build_edge_graph(edges_raw) if edges_raw else ({}, {})

    embeddings = load_embeddings(emb_path) if emb_path else {}

    debug_dir.mkdir(parents=True, exist_ok=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, 'w', encoding='utf-8') as outf:
        for t in targets:
            node_id = t.get('node_id')
            ctx = contexts.get(node_id) or {}
            neighbors = ctx.get('neighbors') or []

            scores = []
            for n in neighbors:
                cat = n.get('category')
                node = n.get('node') or {}
                nid = node.get('id') or node.get('label') or node.get('path')
                label = node.get('label') or nid or ''
                score = 0.0
                # caller/callee flags
                if cat == 'callers':
                    score += weights['w_callers']
                if cat == 'callees':
                    score += weights['w_callees']
                if cat == 'same_file':
                    score += weights['w_samefile']
                # name similarity
                name_sim = name_similarity(label, (t.get('candidate_entity_id') or t.get('function_name') or ''))
                score += weights['w_name_sim'] * name_sim
                # graph distance
                dist = None
                if src_map:
                    try:
                        dist = shortest_path_length(src_map, nid, node_id, max_hops=4)
                    except Exception:
                        dist = None
                if dist is not None:
                    score += weights['w_graph_dist'] * (1.0 / (1.0 + dist))
                # embedding similarity (if available)
                emb_sim = 0.0
                if embeddings and nid in embeddings and node_id in embeddings:
                    emb_sim = cosine_sim(embeddings[nid], embeddings[node_id])
                    score += weights['w_emb_sim'] * emb_sim
                scores.append({'neighbor_id': nid, 'label': label, 'category': cat, 'score': score, 'name_sim': name_sim, 'graph_dist': dist, 'emb_sim': emb_sim})

            # also consider adding same-file neighbors if none present
            if not neighbors:
                # try using edges to expand 1-hop neighbors
                if src_map.get(node_id):
                    for nb in src_map.get(node_id, [])[:K]:
                        scores.append({'neighbor_id': nb, 'label': nb, 'category': 'expanded', 'score': 0.5, 'name_sim': 0.0, 'graph_dist': 1, 'emb_sim': 0.0})

            # sort and pick top-K
            scores_sorted = sorted(scores, key=lambda x: x['score'], reverse=True)
            topk = scores_sorted[:K]

            out_rec = {
                'node_id': node_id,
                'path': t.get('path'),
                'top_neighbors': topk,
                'debug_all': scores_sorted
            }
            outf.write(json.dumps(out_rec, ensure_ascii=False) + '\n')
            written += 1
            logger.debug(f'Wrote ranked neighbors for {node_id} (top {len(topk)})')

            # write per-target debug
            (debug_dir / f'{node_id.replace("/","__").replace(":","_")}.json').write_text(json.dumps(out_rec, ensure_ascii=False), encoding='utf-8')

    logger.info(f'Wrote {written} ranked neighbor records to: {out_path}')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step8_re_rank_neighbors')
        raise
