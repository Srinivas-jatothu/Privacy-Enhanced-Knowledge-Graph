#!/usr/bin/env python3
# scripts/kg_extraction_eval.py

import os, json, collections

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    PROJECT_ROOT = os.getcwd()

def load_json(path, default=None):
    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return default
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)

results_dir = os.path.join(PROJECT_ROOT, "results")
nodes = load_json(os.path.join(results_dir, "node_v2.json"), [])
edges = load_json(os.path.join(results_dir, "edges.json"), [])
gold_entities = load_json(os.path.join(results_dir, "kg_eval_gold_entities.json"), [])
gold_triples = load_json(os.path.join(results_dir, "kg_eval_gold_triples.json"), [])

def prf(tp, fp, fn):
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
    return prec, rec, f1

# --- Entity evaluation --------------------------------------------------------
print("=== Entity Extraction Evaluation ===")
gold_entity_ids = {e["id"] for e in gold_entities if "id" in e}

pred_entity_ids = {n["id"] for n in nodes if "id" in n and n["id"] in gold_entity_ids}

tp_e = len(pred_entity_ids & gold_entity_ids)
fp_e = len(pred_entity_ids - gold_entity_ids)
fn_e = len(gold_entity_ids - pred_entity_ids)

prec_e, rec_e, f1_e = prf(tp_e, fp_e, fn_e)
print("Gold entities:", len(gold_entity_ids))
print("Predicted entities (restricted to gold ids):", len(pred_entity_ids))
print(f"TP={tp_e}, FP={fp_e}, FN={fn_e}")
print(f"Precision: {prec_e:.3f}, Recall: {rec_e:.3f}, F1: {f1_e:.3f}")

# --- Triple / relation evaluation --------------------------------------------
print("\n=== Relation / Triple Extraction Evaluation ===")

gold_triple_set = set()
for t in gold_triples:
    s = t.get("subject")
    p = t.get("predicate")
    o = t.get("object")
    if s and p and o:
        gold_triple_set.add((s, p, o))

# Extract predicted triples from edges matching gold predicates only
pred_triple_set = set()
for e in edges:
    s = e.get("source") or e.get("start_id") or e.get("start")
    o = e.get("target") or e.get("end_id") or e.get("end")
    p = e.get("type") or (e.get("attrs") or {}).get("type")
    if not (s and p and o):
        continue
    # you can restrict to gold predicates for fair comparison
    if any(gt[1] == p for gt in gold_triple_set):
        pred_triple_set.add((s, p, o))

tp_t = len(pred_triple_set & gold_triple_set)
fp_t = len(pred_triple_set - gold_triple_set)
fn_t = len(gold_triple_set - pred_triple_set)

prec_t, rec_t, f1_t = prf(tp_t, fp_t, fn_t)
print("Gold triples:", len(gold_triple_set))
print("Predicted triples (subset by gold predicates):", len(pred_triple_set))
print(f"TP={tp_t}, FP={fp_t}, FN={fn_t}")
print(f"Precision: {prec_t:.3f}, Recall: {rec_t:.3f}, F1: {f1_t:.3f}")

print("\n[NOTE] You can report these as 'entity extraction F1' and 'relation extraction F1' in the paper.\n"
      "Optionally, you can define stricter metrics (graph-match F1, graph-edit distance) on the induced subgraph.")
