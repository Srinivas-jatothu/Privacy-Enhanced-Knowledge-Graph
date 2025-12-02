#!/usr/bin/env python3
"""
normalize_predictions.py

Creates a normalized predictions JSONL file from results/auto_summaries.jsonl,
ensuring each record has a single text field 'pred_text' suitable for BLEU/ROUGE.

Writes:
  - results/auto_summaries_normalized.jsonl
  - results/auto_summaries_normalized_preds.txt  (one-line per example for CodeBLEU)
"""

import json
import re
from pathlib import Path
import unicodedata
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("normalize_predictions")

IN = Path("results/auto_summaries.jsonl")
OUT = Path("results/auto_summaries_normalized.jsonl")
OUT_TXT = Path("results/auto_summaries_normalized_preds.txt")

def collapse_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def try_extract_first_sentence(text: str) -> str:
    # naive first-sentence extractor: split on period, question, or exclamation (keep up to 2 sentences)
    if not text:
        return ""
    # remove common LLM markup headings (like "**Summary:**")
    text = re.sub(r"^\s*[*#=-]+\s*", "", text)
    # collapse bullets/newlines
    text = text.replace("\r", "\n")
    lines = [l.strip(" -\u2022\t") for l in text.splitlines() if l.strip()]
    joined = " ".join(lines)
    # fallback: use up to 2 sentences
    sentences = re.split(r'(?<=[.!?])\s+', joined)
    if not sentences:
        return collapse_whitespace(joined)[:500]
    first_two = " ".join(sentences[:2])
    return collapse_whitespace(first_two)

def normalize_unicode(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # Normalize to NFC and fix common mojibake sequences
    s = unicodedata.normalize("NFC", s)
    # Fix common garbled apostrophe sequences (if present)
    s = s.replace("\u2019", "'").replace("â€™", "'").replace("â€œ", '"').replace("â€\x9d", '"')
    return s

def build_pred_text(rec: dict) -> str:
    # 1) prefer explicit one-liner fields
    for k in ("one_liner", "summary_one_liner", "one_line", "one_line_summary", "short_summary"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip() and not v.strip().startswith("**"):
            return normalize_unicode(collapse_whitespace(v))
    # 2) if bullets exist, join them
    if rec.get("bullets") and isinstance(rec["bullets"], list):
        joined = " ".join([str(x) for x in rec["bullets"] if x])
        if joined.strip():
            # prefer first line / first sentence
            return normalize_unicode(try_extract_first_sentence(joined))
    # 3) if raw_output/raw_model_output/raw is present
    for k in ("raw_output", "raw_model_output", "generated_text", "raw", "raw_response"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return normalize_unicode(try_extract_first_sentence(v))
    # 4) last resort: use whole record concatenated fields
    parts = []
    for k in ("title", "header", "instructions"):
        if rec.get(k):
            parts.append(str(rec[k]))
    # join some other likely fields
    for k in ("description", "details", "explanation"):
        if rec.get(k):
            parts.append(str(rec[k]))
    fallback = " ".join(parts)
    return normalize_unicode(try_extract_first_sentence(fallback) or "")[:800]

def main():
    if not IN.exists():
        logger.error("Missing input: %s", IN)
        return
    OUT.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    normalized = []
    with open(IN, "r", encoding="utf-8") as fh_in, open(OUT, "w", encoding="utf-8") as fh_out, open(OUT_TXT, "w", encoding="utf-8") as fh_txt:
        for ln in fh_in:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rec = json.loads(ln)
            except Exception as e:
                logger.warning("Skipping invalid json line: %s", e)
                continue
            nid = rec.get("node_id") or rec.get("node") or rec.get("id")
            pred_text = build_pred_text(rec)
            # Ensure we have some text; if empty, try to use any small fallback like header
            if not pred_text:
                # try to use first non-empty value in rec
                for v in rec.values():
                    if isinstance(v, str) and v.strip():
                        pred_text = normalize_unicode(try_extract_first_sentence(v))
                        break
            out_rec = {
                "node_id": nid,
                "pred_text": pred_text,
                # keep original for traceability
                "original": rec
            }
            fh_out.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            fh_txt.write((pred_text or "").replace("\n", " ") + "\n")
            count += 1
    logger.info("Wrote %d normalized predictions to %s and text file %s", count, OUT, OUT_TXT)

if __name__ == "__main__":
    main()
