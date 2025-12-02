# #!/usr/bin/env python3
# """
# compute_metrics.py

# Compute BLEU and ROUGE (and optionally CodeBLEU) between pipeline-generated summaries
# and human reference summaries.

# Usage:
#   python compute_metrics.py \
#     --gen results/auto_summaries.jsonl \
#     --refs results/human_summaries.jsonl \
#     --out-dir results/metrics \
#     [--codebleu-script /path/to/CodeBLEU/compute_codebleu.py]

# Outputs:
#   - results/metrics/metrics_summary.csv  (overall scores)
#   - results/metrics/per_node_scores.csv  (per-node scores)
#   - results/metrics/predictions_with_refs.jsonl (joined records)
# """

# import argparse
# import csv
# import json
# import logging
# import os
# import sys
# from collections import defaultdict
# from pathlib import Path
# from typing import Dict, List, Optional

# # third-party libs
# try:
#     import sacrebleu
# except Exception:
#     sacrebleu = None
# try:
#     from rouge_score import rouge_scorer
# except Exception:
#     rouge_scorer = None

# # Logging
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger("compute_metrics")

# # ---------------------------
# # Helpers: load files & normalize
# # ---------------------------
# def load_jsonl(path: Path) -> List[Dict]:
#     out = []
#     if not path.exists():
#         logger.warning(f"Missing JSONL: {path}")
#         return out
#     with open(path, "r", encoding="utf-8") as fh:
#         for ln in fh:
#             ln = ln.strip()
#             if not ln:
#                 continue
#             out.append(json.loads(ln))
#     logger.debug(f"Loaded {len(out)} records from {path}")
#     return out

# def load_csv_refs(path: Path) -> Dict[str, List[str]]:
#     out = defaultdict(list)
#     if not path.exists():
#         return out
#     with open(path, newline="", encoding="utf-8") as fh:
#         reader = csv.DictReader(fh)
#         for r in reader:
#             nid = r.get("node_id") or r.get("id")
#             ref = r.get("reference") or r.get("ref") or r.get("summary")
#             if nid and ref:
#                 out[nid].append(ref)
#     logger.debug(f"Loaded refs for {len(out)} node_ids from CSV {path}")
#     return out

# def extract_prediction_text(gen_rec: Dict) -> str:
#     # The pipeline stores outputs in different fields. Try common ones:
#     for key in ("one_liner","summary_one_liner","summary","short_summary"):
#         if gen_rec.get(key):
#             return gen_rec.get(key)
#     # fallback: if we have bullets, join them
#     if gen_rec.get("bullets") and isinstance(gen_rec.get("bullets"), list):
#         return " ".join(gen_rec.get("bullets"))
#     # fallback to raw
#     return gen_rec.get("raw_model_output") or gen_rec.get("raw_output") or gen_rec.get("generated_text") or ""

# # ---------------------------
# # Metric computations
# # ---------------------------
# def compute_corpus_bleu(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
#     if sacrebleu is None:
#         logger.error("sacrebleu not installed. pip install sacrebleu")
#         return {"bleu": None}
#     # sacrebleu expects references as list of reference lists (each inner list is all refs for each item)
#     # But corpus_bleu wants refs as list-of-lists-of-refs where each inner list is all references for that corpus index.
#     # sacrebleu.corpus_bleu expects: sys (preds), [refs1, refs2, ...] where each refsX is list-of-refstrings per item
#     # So reformat to: refs_by_refindex: list where each element is list of reference for whole corpus for a single reference index.
#     # Simpler: if multiple refs per item, convert to list-of-lists: e.g. refs = list(zip(*list_of_refs))
#     # Build refs_for_sacrebleu as list-of-refstrings-lists
#     num_items = len(preds)
#     # pad references to same count per item
#     max_refs = max(len(r) for r in list_of_refs) if list_of_refs else 0
#     refs_per_index = []
#     for ri in range(max_refs):
#         this_ref_list = []
#         for r in list_of_refs:
#             if ri < len(r):
#                 this_ref_list.append(r[ri])
#             else:
#                 # if missing this ref index for this item, duplicate first ref
#                 this_ref_list.append(r[0] if r else "")
#         refs_per_index.append(this_ref_list)
#     bleu = sacrebleu.corpus_bleu(preds, refs_per_index)
#     return {"bleu": bleu.score, "bleu_obj": bleu}

# def compute_rouge_scores(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
#     if rouge_scorer is None:
#         logger.error("rouge_score not installed. pip install rouge_score")
#         return {}
#     # We'll compute ROUGE-1/2/L using only first reference per item (common practice).
#     scorer = rouge_scorer.RougeScorer(["rouge1","rouge2","rougeL"], use_stemmer=True)
#     agg = {"rouge1": [], "rouge2": [], "rougeL": []}
#     for p, refs in zip(preds, list_of_refs):
#         ref = refs[0] if refs else ""
#         scores = scorer.score(ref, p)
#         agg["rouge1"].append(scores["rouge1"].fmeasure)
#         agg["rouge2"].append(scores["rouge2"].fmeasure)
#         agg["rougeL"].append(scores["rougeL"].fmeasure)
#     import statistics
#     return {
#         "rouge1": 100 * statistics.mean(agg["rouge1"]) if agg["rouge1"] else None,
#         "rouge2": 100 * statistics.mean(agg["rouge2"]) if agg["rouge2"] else None,
#         "rougeL": 100 * statistics.mean(agg["rougeL"]) if agg["rougeL"] else None,
#     }

# # A small wrapper to optionally call external CodeBLEU script (if user installed it)
# def run_codebleu_if_available(codebleu_script: Optional[str], preds_file: Path, refs_file: Path) -> Optional[Dict]:
#     """
#     codebleu_script: path to CodeBLEU compute script (python). Example: /path/to/CodeBLEU/evaluate.py
#     preds_file, refs_file: plain text files, one line per example, aligned.
#     """
#     if not codebleu_script:
#         return None
#     codebleu_script = Path(codebleu_script)
#     if not codebleu_script.exists():
#         logger.error("CodeBLEU script not found at: %s", codebleu_script)
#         return None
#     # run external script
#     import subprocess, shlex, tempfile
#     cmd = [sys.executable, str(codebleu_script), "--refs", str(refs_file), "--cands", str(preds_file)]
#     logger.info("Running CodeBLEU: %s", " ".join(shlex.quote(x) for x in cmd))
#     try:
#         p = subprocess.run(cmd, capture_output=True, text=True, check=False)
#         logger.debug("CodeBLEU stdout:\n%s", p.stdout)
#         logger.debug("CodeBLEU stderr:\n%s", p.stderr)
#         # Try to parse numeric results from stdout (best-effort)
#         out = {"raw_stdout": p.stdout, "raw_stderr": p.stderr, "returncode": p.returncode}
#         return out
#     except Exception as e:
#         logger.exception("Failed to run CodeBLEU script")
#         return None

# # ---------------------------
# # Main
# # ---------------------------
# def main(argv=None):
#     p = argparse.ArgumentParser()
#     p.add_argument("--gen", default="results/auto_summaries.jsonl", help="Generated summaries (JSONL)")
#     p.add_argument("--refs", default="results/human_summaries.jsonl", help="Human references (JSONL or CSV)")
#     p.add_argument("--out-dir", default="results/metrics", help="Output folder for metrics")
#     p.add_argument("--codebleu-script", default="", help="Optional: path to CodeBLEU evaluation script")
#     p.add_argument("--text-field", default="one_liner", help="Which field in generated JSON to use as prediction")
#     args = p.parse_args(argv)

#     out_dir = Path(args.out_dir)
#     out_dir.mkdir(parents=True, exist_ok=True)

#     gen_path = Path(args.gen)
#     refs_path = Path(args.refs)

#     gen = load_jsonl(gen_path)
#     # build map: node_id -> generated text (string)
#     gen_map = {}
#     for r in gen:
#         nid = r.get("node_id") or r.get("node") or r.get("id")
#         txt = r.get(args.text_field) or extract_prediction_text(r)
#         gen_map[nid] = txt.strip() if isinstance(txt, str) else ""

#     # load refs (support JSONL or CSV)
#     refs_map = defaultdict(list)
#     if refs_path.suffix.lower() == ".jsonl":
#         refs_records = load_jsonl(refs_path)
#         for r in refs_records:
#             nid = r.get("node_id") or r.get("node") or r.get("id")
#             if not nid:
#                 continue
#             if "references" in r and isinstance(r.get("references"), list):
#                 refs_map[nid].extend([s.strip() for s in r.get("references") if s])
#             elif r.get("reference"):
#                 refs_map[nid].append(r.get("reference").strip())
#             elif r.get("reference_text"):
#                 refs_map[nid].append(r.get("reference_text").strip())
#     else:
#         csv_map = load_csv_refs(refs_path)
#         for k,v in csv_map.items():
#             refs_map[k].extend(v)

#     # Align items: we will only evaluate on nodes that exist in both sets
#     common = [nid for nid in gen_map.keys() if nid in refs_map and refs_map[nid]]
#     if not common:
#         logger.error("No overlapping node_ids with references found. Check your gen and refs files.")
#         # Write out a helpful mapping file for manual inspection
#         with open(out_dir / "gen_nodes.txt","w", encoding="utf-8") as fh:
#             fh.write("\n".join(sorted(list(gen_map.keys() or []))))
#         with open(out_dir / "ref_nodes.txt","w", encoding="utf-8") as fh:
#             fh.write("\n".join(sorted(list(refs_map.keys() or []))))
#         logger.info("Wrote gen_nodes.txt and ref_nodes.txt for debugging")
#         return

#     preds = [gen_map[nid] for nid in common]
#     refs_list = [refs_map[nid] for nid in common]  # list of lists

#     # Save aligned preds/refs text files for CodeBLEU (one reference per line uses first ref only)
#     preds_txt = out_dir / "preds.txt"
#     refs_txt = out_dir / "refs.txt"
#     with open(preds_txt, "w", encoding="utf-8") as f1, open(refs_txt, "w", encoding="utf-8") as f2:
#         for p, rlist in zip(preds, refs_list):
#             f1.write((p or "").replace("\n"," ") + "\n")
#             # use first reference only for easy interoperability
#             f2.write((rlist[0] if rlist else "").replace("\n"," ") + "\n")

#     # Compute BLEU
#     bleu_res = compute_corpus_bleu(preds, refs_list) if sacrebleu else {"bleu": None}
#     logger.info("BLEU score: %s", bleu_res.get("bleu"))

#     # Compute ROUGE
#     rouge_res = compute_rouge_scores(preds, refs_list) if rouge_scorer else {}
#     logger.info("ROUGE scores: %s", rouge_res)

#     # Optionally run CodeBLEU
#     codebleu_out = None
#     if args.codebleu_script:
#         codebleu_out = run_codebleu_if_available(args.codebleu_script, preds_txt, refs_txt)
#         logger.info("CodeBLEU output: %s", codebleu_out and codebleu_out.get("raw_stdout","")[:300])

#     # Per-node ROUGE (optional) for CSV
#     per_rows = []
#     if rouge_scorer:
#         scorer = rouge_scorer.RougeScorer(["rouge1","rouge2","rougeL"], use_stemmer=True)
#         for nid, p, rlist in zip(common, preds, refs_list):
#             rtext = rlist[0] if rlist else ""
#             sc = scorer.score(rtext, p)
#             per_rows.append({
#                 "node_id": nid,
#                 "pred": p,
#                 "ref": rtext,
#                 "rouge1": sc["rouge1"].fmeasure,
#                 "rouge2": sc["rouge2"].fmeasure,
#                 "rougeL": sc["rougeL"].fmeasure,
#             })
#     else:
#         for nid, p, rlist in zip(common, preds, refs_list):
#             per_rows.append({"node_id": nid, "pred": p, "ref": (rlist[0] if rlist else "")})

#     # Write overall CSV
#     overall_csv = out_dir / "metrics_summary.csv"
#     with open(overall_csv, "w", newline="", encoding="utf-8") as fh:
#         writer = csv.writer(fh)
#         writer.writerow(["metric","value"])
#         writer.writerow(["BLEU", bleu_res.get("bleu")])
#         writer.writerow(["ROUGE-1", rouge_res.get("rouge1")])
#         writer.writerow(["ROUGE-2", rouge_res.get("rouge2")])
#         writer.writerow(["ROUGE-L", rouge_res.get("rougeL")])
#         if codebleu_out:
#             writer.writerow(["CodeBLEU_raw_output", codebleu_out.get("raw_stdout","")[:1000].replace("\n"," ")])

#     # Write per-node CSV
#     per_csv = out_dir / "per_node_scores.csv"
#     keys = list(per_rows[0].keys()) if per_rows else []
#     with open(per_csv, "w", newline="", encoding="utf-8") as fh:
#         writer = csv.DictWriter(fh, fieldnames=keys)
#         writer.writeheader()
#         for r in per_rows:
#             writer.writerow(r)

#     # Write joined JSONL for debugging & manual checks
#     joined = out_dir / "predictions_with_refs.jsonl"
#     with open(joined, "w", encoding="utf-8") as fh:
#         for nid, p, rlist in zip(common, preds, refs_list):
#             fh.write(json.dumps({"node_id": nid, "pred": p, "refs": rlist}, ensure_ascii=False) + "\n")

#     logger.info("Wrote metrics summary to %s and per-node scores to %s", overall_csv, per_csv)

# if __name__ == "__main__":
#     main()




#!/usr/bin/env python3
"""
compute_metrics.py

Compute BLEU, ROUGE, and additional metrics (METEOR, chrF, lexical overlaps)
between pipeline-generated summaries and human reference summaries.
Optionally, also call an external CodeBLEU script.

Usage:
  python compute_metrics.py \
    --gen results/auto_summaries.jsonl \
    --refs results/human_summaries.jsonl \
    --out-dir results/metrics \
    [--codebleu-script /path/to/CodeBLEU/compute_codebleu.py]

Outputs:
  - results/metrics/metrics_summary.csv          (overall scores)
  - results/metrics/per_node_scores.csv          (per-node scores)
  - results/metrics/predictions_with_refs.jsonl  (joined records)
  - results/metrics/preds.txt / refs.txt         (aligned text for CodeBLEU)
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# third-party libs
try:
    import sacrebleu
except Exception:
    sacrebleu = None

try:
    from rouge_score import rouge_scorer
except Exception:
    rouge_scorer = None

# METEOR (optional, requires nltk)
try:
    from nltk.translate.meteor_score import single_meteor_score
except Exception:
    single_meteor_score = None

# Logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("compute_metrics")

# ---------------------------
# Helpers: load files & normalize
# ---------------------------
def load_jsonl(path: Path) -> List[Dict]:
    out = []
    if not path.exists():
        logger.warning(f"Missing JSONL: {path}")
        return out
    with open(path, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f"Loaded {len(out)} records from {path}")
    return out

def load_csv_refs(path: Path) -> Dict[str, List[str]]:
    out = defaultdict(list)
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            nid = r.get("node_id") or r.get("id")
            ref = r.get("reference") or r.get("ref") or r.get("summary")
            if nid and ref:
                out[nid].append(ref)
    logger.debug(f"Loaded refs for {len(out)} node_ids from CSV {path}")
    return out

def extract_prediction_text(gen_rec: Dict) -> str:
    # The pipeline stores outputs in different fields. Try common ones:
    for key in ("one_liner", "summary_one_liner", "summary", "short_summary"):
        if gen_rec.get(key):
            return gen_rec.get(key)
    # fallback: if we have bullets, join them
    if gen_rec.get("bullets") and isinstance(gen_rec.get("bullets"), list):
        return " ".join(gen_rec.get("bullets"))
    # fallback to raw
    return (
        gen_rec.get("raw_model_output")
        or gen_rec.get("raw_output")
        or gen_rec.get("generated_text")
        or ""
    )

# ---------------------------
# Tokenization & overlap helpers
# ---------------------------
def tokenize_simple(text: str) -> List[str]:
    """
    Very simple tokenizer: lowercase + whitespace split.
    Enough for lightweight lexical statistics.
    """
    if not text:
        return []
    return text.lower().strip().split()

def compute_token_overlap(
    preds: List[str], refs_list: List[List[str]]
) -> Dict[str, float]:
    """
    Compute simple token-level precision/recall/F1 and length statistics
    between predictions and first reference.
    """
    import statistics

    precisions = []
    recalls = []
    f1s = []
    pred_len_tokens = []
    ref_len_tokens = []
    pred_len_chars = []
    ref_len_chars = []

    for p, rlist in zip(preds, refs_list):
        ref = rlist[0] if rlist else ""
        p_tokens = tokenize_simple(p)
        r_tokens = tokenize_simple(ref)

        pred_len_tokens.append(len(p_tokens))
        ref_len_tokens.append(len(r_tokens))
        pred_len_chars.append(len(p or ""))
        ref_len_chars.append(len(ref or ""))

        if not p_tokens and not r_tokens:
            precisions.append(1.0)
            recalls.append(1.0)
            f1s.append(1.0)
            continue
        if not p_tokens or not r_tokens:
            precisions.append(0.0)
            recalls.append(0.0)
            f1s.append(0.0)
            continue

        p_set = set(p_tokens)
        r_set = set(r_tokens)
        inter = len(p_set & r_set)

        prec = inter / len(p_set) if p_set else 0.0
        rec = inter / len(r_set) if r_set else 0.0
        if prec + rec > 0:
            f1 = 2 * prec * rec / (prec + rec)
        else:
            f1 = 0.0

        precisions.append(prec)
        recalls.append(rec)
        f1s.append(f1)

    def avg(xs: List[float]) -> Optional[float]:
        return statistics.mean(xs) if xs else None

    return {
        "lex_precision": avg(precisions),
        "lex_recall": avg(recalls),
        "lex_f1": avg(f1s),
        "avg_pred_len_tokens": avg(pred_len_tokens),
        "avg_ref_len_tokens": avg(ref_len_tokens),
        "avg_pred_len_chars": avg(pred_len_chars),
        "avg_ref_len_chars": avg(ref_len_chars),
    }

# ---------------------------
# Metric computations
# ---------------------------
def compute_corpus_bleu(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
    if sacrebleu is None:
        logger.error("sacrebleu not installed. pip install sacrebleu")
        return {"bleu": None}
    # sacrebleu.corpus_bleu expects: sys (preds), [refs1, refs2, ...]
    # where each refsX is list-of-refstrings per item
    num_items = len(preds)
    max_refs = max(len(r) for r in list_of_refs) if list_of_refs else 0
    refs_per_index = []
    for ri in range(max_refs):
        this_ref_list = []
        for r in list_of_refs:
            if ri < len(r):
                this_ref_list.append(r[ri])
            else:
                this_ref_list.append(r[0] if r else "")
        refs_per_index.append(this_ref_list)
    bleu = sacrebleu.corpus_bleu(preds, refs_per_index)
    return {"bleu": bleu.score, "bleu_obj": bleu}

def compute_chrf(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
    """
    Compute chrF (character-level F-score) using sacrebleu.
    Uses only the first reference per item.
    """
    if sacrebleu is None:
        logger.error("sacrebleu not installed. pip install sacrebleu")
        return {"chrf": None}
    first_refs = [refs[0] if refs else "" for refs in list_of_refs]
    chrf_res = sacrebleu.corpus_chrf(preds, [first_refs])
    return {"chrf": chrf_res.score, "chrf_obj": chrf_res}

def compute_rouge_scores(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
    if rouge_scorer is None:
        logger.error("rouge_score not installed. pip install rouge_score")
        return {}
    scorer = rouge_scorer.RougeScorer(["rouge1", "rouge2", "rougeL"], use_stemmer=True)
    agg = {"rouge1": [], "rouge2": [], "rougeL": []}
    for p, refs in zip(preds, list_of_refs):
        ref = refs[0] if refs else ""
        scores = scorer.score(ref, p)
        agg["rouge1"].append(scores["rouge1"].fmeasure)
        agg["rouge2"].append(scores["rouge2"].fmeasure)
        agg["rougeL"].append(scores["rougeL"].fmeasure)
    import statistics
    return {
        "rouge1": 100 * statistics.mean(agg["rouge1"]) if agg["rouge1"] else None,
        "rouge2": 100 * statistics.mean(agg["rouge2"]) if agg["rouge2"] else None,
        "rougeL": 100 * statistics.mean(agg["rougeL"]) if agg["rougeL"] else None,
    }

def compute_meteor(preds: List[str], list_of_refs: List[List[str]]) -> Dict:
    """
    Compute corpus-level METEOR as the mean of example-level scores.
    Uses only the first reference per item.
    """
    if single_meteor_score is None:
        logger.error("nltk METEOR not available. pip install nltk and download METEOR resources.")
        return {"meteor": None}

    import statistics

    scores = []
    for p, refs in zip(preds, list_of_refs):
        ref = refs[0] if refs else ""
        # nltk expects: reference as list[str], hypothesis as str
        try:
            s = single_meteor_score([ref], p) if ref or p else 0.0
        except Exception as e:
            logger.debug("METEOR failed for example, setting 0.0: %s", e)
            s = 0.0
        scores.append(s)

    return {"meteor": 100 * statistics.mean(scores) if scores else None}

# A small wrapper to optionally call external CodeBLEU script (if user installed it)
def run_codebleu_if_available(
    codebleu_script: Optional[str], preds_file: Path, refs_file: Path
) -> Optional[Dict]:
    """
    codebleu_script: path to CodeBLEU compute script (python). Example: /path/to/CodeBLEU/evaluate.py
    preds_file, refs_file: plain text files, one line per example, aligned.
    """
    if not codebleu_script:
        return None
    codebleu_script = Path(codebleu_script)
    if not codebleu_script.exists():
        logger.error("CodeBLEU script not found at: %s", codebleu_script)
        return None
    import subprocess, shlex
    cmd = [sys.executable, str(codebleu_script), "--refs", str(refs_file), "--cands", str(preds_file)]
    logger.info("Running CodeBLEU: %s", " ".join(shlex.quote(x) for x in cmd))
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=False)
        logger.debug("CodeBLEU stdout:\n%s", p.stdout)
        logger.debug("CodeBLEU stderr:\n%s", p.stderr)
        out = {"raw_stdout": p.stdout, "raw_stderr": p.stderr, "returncode": p.returncode}
        return out
    except Exception:
        logger.exception("Failed to run CodeBLEU script")
        return None

# ---------------------------
# Main
# ---------------------------
def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--gen", default="results/auto_summaries.jsonl", help="Generated summaries (JSONL)")
    p.add_argument("--refs", default="results/human_summaries.jsonl", help="Human references (JSONL or CSV)")
    p.add_argument("--out-dir", default="results/metrics", help="Output folder for metrics")
    p.add_argument("--codebleu-script", default="", help="Optional: path to CodeBLEU evaluation script")
    p.add_argument("--text-field", default="one_liner", help="Which field in generated JSON to use as prediction")
    args = p.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    gen_path = Path(args.gen)
    refs_path = Path(args.refs)

    gen = load_jsonl(gen_path)
    # build map: node_id -> generated text (string)
    gen_map: Dict[str, str] = {}
    for r in gen:
        nid = r.get("node_id") or r.get("node") or r.get("id")
        if not nid:
            continue
        txt = r.get(args.text_field) or extract_prediction_text(r)
        gen_map[nid] = txt.strip() if isinstance(txt, str) else ""

    # load refs (support JSONL or CSV)
    refs_map: Dict[str, List[str]] = defaultdict(list)
    if refs_path.suffix.lower() == ".jsonl":
        refs_records = load_jsonl(refs_path)
        for r in refs_records:
            nid = r.get("node_id") or r.get("node") or r.get("id")
            if not nid:
                continue
            if "references" in r and isinstance(r.get("references"), list):
                refs_map[nid].extend([s.strip() for s in r.get("references") if s])
            elif r.get("reference"):
                refs_map[nid].append(r.get("reference").strip())
            elif r.get("reference_text"):
                refs_map[nid].append(r.get("reference_text").strip())
    else:
        csv_map = load_csv_refs(refs_path)
        for k, v in csv_map.items():
            refs_map[k].extend(v)

    # Align items: we will only evaluate on nodes that exist in both sets
    common = [nid for nid in gen_map.keys() if nid in refs_map and refs_map[nid]]
    if not common:
        logger.error("No overlapping node_ids with references found. Check your gen and refs files.")
        with open(out_dir / "gen_nodes.txt", "w", encoding="utf-8") as fh:
            fh.write("\n".join(sorted(list(gen_map.keys() or []))))
        with open(out_dir / "ref_nodes.txt", "w", encoding="utf-8") as fh:
            fh.write("\n".join(sorted(list(refs_map.keys() or []))))
        logger.info("Wrote gen_nodes.txt and ref_nodes.txt for debugging")
        return

    preds = [gen_map[nid] for nid in common]
    refs_list = [refs_map[nid] for nid in common]  # list of lists

    # Save aligned preds/refs text files for CodeBLEU (one reference per line uses first ref only)
    preds_txt = out_dir / "preds.txt"
    refs_txt = out_dir / "refs.txt"
    with open(preds_txt, "w", encoding="utf-8") as f1, open(refs_txt, "w", encoding="utf-8") as f2:
        for ptxt, rlist in zip(preds, refs_list):
            f1.write((ptxt or "").replace("\n", " ") + "\n")
            f2.write((rlist[0] if rlist else "").replace("\n", " ") + "\n")

    # Compute BLEU
    bleu_res = compute_corpus_bleu(preds, refs_list) if sacrebleu else {"bleu": None}
    logger.info("BLEU score: %s", bleu_res.get("bleu"))

    # Compute ROUGE
    rouge_res = compute_rouge_scores(preds, refs_list) if rouge_scorer else {}
    logger.info("ROUGE scores: %s", rouge_res)

    # Compute chrF
    chrf_res = compute_chrf(preds, refs_list) if sacrebleu else {"chrf": None}
    logger.info("chrF: %s", chrf_res.get("chrf"))

    # Compute METEOR
    meteor_res = compute_meteor(preds, refs_list)
    logger.info("METEOR: %s", meteor_res.get("meteor"))

    # Lexical overlap & length stats
    lex_res = compute_token_overlap(preds, refs_list)
    logger.info("Lexical overlap stats: %s", lex_res)

    # Optionally run CodeBLEU
    codebleu_out = None
    if args.codebleu_script:
        codebleu_out = run_codebleu_if_available(args.codebleu_script, preds_txt, refs_txt)
        logger.info("CodeBLEU output (truncated): %s",
                    (codebleu_out and codebleu_out.get("raw_stdout", "")[:300]) or "")

    # Per-node metrics
    per_rows = []
    if rouge_scorer:
        scorer = rouge_scorer.RougeScorer(["rouge1", "rouge2", "rougeL"], use_stemmer=True)
    else:
        scorer = None

    per_has_meteor = single_meteor_score is not None

    for nid, ptxt, rlist in zip(common, preds, refs_list):
        ref = rlist[0] if rlist else ""

        row = {
            "node_id": nid,
            "pred": ptxt,
            "ref": ref,
        }

        # ROUGE per-node
        if scorer:
            sc = scorer.score(ref, ptxt)
            row["rouge1"] = sc["rouge1"].fmeasure
            row["rouge2"] = sc["rouge2"].fmeasure
            row["rougeL"] = sc["rougeL"].fmeasure

        # METEOR per-node
        if per_has_meteor:
            try:
                row["meteor"] = single_meteor_score([ref], ptxt) if (ref or ptxt) else 0.0
            except Exception as e:
                logger.debug("Per-node METEOR failed for %s: %s", nid, e)
                row["meteor"] = 0.0

        # Lexical overlaps per-node
        p_tokens = tokenize_simple(ptxt)
        r_tokens = tokenize_simple(ref)
        row["pred_len_tokens"] = len(p_tokens)
        row["ref_len_tokens"] = len(r_tokens)
        row["pred_len_chars"] = len(ptxt or "")
        row["ref_len_chars"] = len(ref or "")

        if not p_tokens and not r_tokens:
            row["token_precision"] = 1.0
            row["token_recall"] = 1.0
            row["token_f1"] = 1.0
        elif not p_tokens or not r_tokens:
            row["token_precision"] = 0.0
            row["token_recall"] = 0.0
            row["token_f1"] = 0.0
        else:
            p_set = set(p_tokens)
            r_set = set(r_tokens)
            inter = len(p_set & r_set)
            prec = inter / len(p_set) if p_set else 0.0
            rec = inter / len(r_set) if r_set else 0.0
            if prec + rec > 0:
                f1 = 2 * prec * rec / (prec + rec)
            else:
                f1 = 0.0
            row["token_precision"] = prec
            row["token_recall"] = rec
            row["token_f1"] = f1

        per_rows.append(row)

    # Write overall CSV
    overall_csv = out_dir / "metrics_summary.csv"
    with open(overall_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "value"])
        writer.writerow(["BLEU", bleu_res.get("bleu")])
        writer.writerow(["ROUGE-1", rouge_res.get("rouge1")])
        writer.writerow(["ROUGE-2", rouge_res.get("rouge2")])
        writer.writerow(["ROUGE-L", rouge_res.get("rougeL")])
        writer.writerow(["chrF", chrf_res.get("chrf")])
        writer.writerow(["METEOR", meteor_res.get("meteor")])
        writer.writerow(["Lexical_Precision", lex_res.get("lex_precision")])
        writer.writerow(["Lexical_Recall", lex_res.get("lex_recall")])
        writer.writerow(["Lexical_F1", lex_res.get("lex_f1")])
        writer.writerow(["Avg_Pred_Len_Tokens", lex_res.get("avg_pred_len_tokens")])
        writer.writerow(["Avg_Ref_Len_Tokens", lex_res.get("avg_ref_len_tokens")])
        writer.writerow(["Avg_Pred_Len_Chars", lex_res.get("avg_pred_len_chars")])
        writer.writerow(["Avg_Ref_Len_Chars", lex_res.get("avg_ref_len_chars")])
        if codebleu_out:
            writer.writerow([
                "CodeBLEU_raw_output",
                codebleu_out.get("raw_stdout", "")[:1000].replace("\n", " "),
            ])

    # Write per-node CSV
    per_csv = out_dir / "per_node_scores.csv"
    keys = list(per_rows[0].keys()) if per_rows else []
    with open(per_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        for r in per_rows:
            writer.writerow(r)

    # Write joined JSONL for debugging & manual checks
    joined = out_dir / "predictions_with_refs.jsonl"
    with open(joined, "w", encoding="utf-8") as fh:
        for nid, ptxt, rlist in zip(common, preds, refs_list):
            fh.write(
                json.dumps({"node_id": nid, "pred": ptxt, "refs": rlist}, ensure_ascii=False)
                + "\n"
            )

    logger.info("Wrote metrics summary to %s and per-node scores to %s", overall_csv, per_csv)

if __name__ == "__main__":
    main()
