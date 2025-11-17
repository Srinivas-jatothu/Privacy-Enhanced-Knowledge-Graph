#!/usr/bin/env python3
"""
step9_author_signal_detector.py

Safe detector of author-identifying signals in generation payloads.

Purpose:
 - Scan prepared LLM payloads and code snippets for signals that *could* be
   used to attribute the code to an individual (emails, usernames in paths,
   commit/PR metadata, distinct identifiers, hard-coded secrets, etc).
 - Produce a privacy-risk summary per payload.
 - Optionally ask an LLM (OpenRouter) to *describe* the signals and suggest
   safe remediation steps — explicitly forbidding the model from guessing or
   naming any author.

Important safety: This script **does not** attempt to identify or name persons.
It only detects signals and asks the model to comment on their potential for
attribution and mitigations.

Outputs:
 - results/author_signals.jsonl       (deterministic extracted signals + risk score)
 - results/author_signals_model.jsonl (optional: model explanations per payload)

Usage:
 - offline-only (no model): python step9_author_signal_detector.py --backend local
 - with OpenRouter model (requires OPENROUTER_API_KEY in .env): 
     pip install python-dotenv requests
     python step9_author_signal_detector.py --backend openrouter --model "openai/gpt-4o"

"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

# Try to import requests and dotenv if available; script runs without them in local mode
try:
    import requests
except Exception:
    requests = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# Load .env from script directory if present
SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_PATH = SCRIPT_DIR / ".env"
if load_dotenv is not None and ENV_PATH.exists():
    load_dotenv(ENV_PATH)

OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"

# Logging
logger = logging.getLogger("step9_author_signal_detector")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# Regexes / detectors
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
LONG_DIGITS_RE = re.compile(r"\b\d{6,}\b")
SHA1_RE = re.compile(r"\b[0-9a-fA-F]{7,40}\b")  # commit-ish
PATH_USER_RE = re.compile(r"(?:/home/|/Users/|C:\\\\Users\\\\)([A-Za-z0-9_.-]+)")
GIT_AUTHOR_LINE_RE = re.compile(r"author[:=]\\s*([A-Za-z0-9_\\-\\.@]+)", re.I)
IDENTIFIER_NAMES = ["customerid", "email", "ssn", "phone", "userid", "user_id", "customer_id"]

# Helper functions
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out = []
    if not path.exists():
        logger.debug(f"Not found: {path}")
        return out
    with open(path, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f"Loaded {len(out)} records from {path}")
    return out

def safe_truncate(s: str, n: int = 200) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[:n] + "..."

# deterministic scanner for a single payload record
def scan_payload_for_signals(payload: Dict[str, Any], snippet_rec: Optional[Dict[str, Any]], metadata_rec: Optional[Dict[str, Any]],
                             commits: Optional[List[Dict[str,Any]]], prs: Optional[List[Dict[str,Any]]]) -> Dict[str, Any]:
    node_id = payload.get("node_id")
    text = payload.get("payload_text","") + "\n"
    signals = {"emails": [], "paths_with_users": [], "long_numbers": [], "sha_like": [], "identifier_names": [], "provenance": {}}

    # scan payload text
    signals["emails"].extend(EMAIL_RE.findall(text))
    signals["long_numbers"].extend(LONG_DIGITS_RE.findall(text))
    signals["sha_like"].extend([m.group(0) for m in SHA1_RE.finditer(text) if len(m.group(0))>=7])
    # path-like scans for usernames
    for m in PATH_USER_RE.finditer(text):
        signals["paths_with_users"].append(m.group(1))
    # identifier names
    lower = text.lower()
    for ident in IDENTIFIER_NAMES:
        if ident in lower:
            signals["identifier_names"].append(ident)

    # snippet scans
    if snippet_rec and snippet_rec.get("snippet"):
        s = snippet_rec.get("snippet","")
        signals["emails"].extend(EMAIL_RE.findall(s))
        signals["long_numbers"].extend(LONG_DIGITS_RE.findall(s))
        signals["sha_like"].extend([m.group(0) for m in SHA1_RE.finditer(s) if len(m.group(0))>=7])
        for m in PATH_USER_RE.finditer(s):
            signals["paths_with_users"].append(m.group(1))
        for ident in IDENTIFIER_NAMES:
            if ident in s.lower():
                signals["identifier_names"].append(ident)

    # metadata/provenance-based signals
    if metadata_rec:
        canonical = metadata_rec.get("canonical") or {}
        if canonical.get("introduced_by_commit"):
            signals["provenance"]["introduced_by_commit"] = canonical.get("introduced_by_commit")
            # if commit SHA present, add to sha_like
            signals["sha_like"].append(canonical.get("introduced_by_commit"))
        if canonical.get("introduced_by_pr"):
            signals["provenance"]["introduced_by_pr"] = canonical.get("introduced_by_pr")
    # also inspect header for explicit commit or pr text
    header = payload.get("components",{}).get("header","")
    if header:
        # look for commit-like strings
        signals["sha_like"].extend([m.group(0) for m in SHA1_RE.finditer(header) if len(m.group(0))>=7])
        # look for obvious 'introduced_by_pr' pattern
        mpr = re.search(r"introduced_by_pr[=:\s]*([0-9]+)", header, re.I)
        if mpr:
            signals["provenance"]["introduced_by_pr_header"] = mpr.group(1)

    # commit/PR metadata enrichment: if we loaded commits/prs arrays, try to find authors
    if commits:
        # build a map of sha -> author.name/email
        sha2author = {}
        for c in commits:
            sha = c.get("sha") or c.get("commit","") and c.get("commit").get("sha") if isinstance(c,dict) else None
            # flexible: check both top-level and nested commit object
            if not sha:
                sha = c.get("sha")
            if not sha:
                continue
            author = None
            if isinstance(c.get("author"), dict):
                author = c.get("author",{}).get("login") or c.get("commit",{}).get("author",{}).get("name")
            if not author:
                # fallback into commit.author.name
                author = c.get("commit",{}).get("author",{}).get("name")
            if author:
                sha2author[sha[:7]] = author
        # check if any sha-like signals match map keys
        for s in list(set(signals["sha_like"])):
            short = s[:7]
            if short in sha2author:
                signals["provenance"]["commit_author_hint_"+short] = sha2author[short]

    if prs:
        # build pr-number -> author mapping
        pr2author = {}
        for pr in prs:
            num = str(pr.get("number") or pr.get("id") or "")
            if not num:
                continue
            author = pr.get("user",{}).get("login") or pr.get("author",{}).get("login") if isinstance(pr.get("user"), dict) else None
            if not author:
                author = pr.get("author") or pr.get("owner")
            if author:
                pr2author[num] = author
        if "provenance" in signals and signals["provenance"].get("introduced_by_pr"):
            prnum = str(signals["provenance"].get("introduced_by_pr"))
            if prnum in pr2author:
                signals["provenance"]["pr_author_hint_"+prnum] = pr2author[prnum]

    # deduplicate lists and limit lengths
    for k in ["emails","paths_with_users","long_numbers","sha_like","identifier_names"]:
        signals[k] = list(dict.fromkeys(signals[k]))[:10]

    # compute a simple risk score (heuristic)
    score = 0
    # each email => +3, each username in path => +2, sha-like presence => +1, identifier names => +2, provenance present => +3
    score += 3 * len(signals["emails"])
    score += 2 * len(signals["paths_with_users"])
    score += 1 * len(signals["sha_like"])
    score += 2 * len(signals["identifier_names"])
    if signals.get("provenance"):
        score += 3
    # normalize to 0..10
    risk = min(10, int(score))
    return {"node_id": node_id, "signals": signals, "risk_score": risk, "risk_level": ("LOW","MEDIUM","HIGH")[0 if risk<3 else 1 if risk<6 else 2] }

# Construct a model prompt that **only** asks to describe the signals and remediation,
# and explicitly instructs the model: DO NOT GUESS OR NAME AN AUTHOR.
MODEL_PROMPT_TEMPLATE = """
You are a privacy auditor assistant. For the provided code payload and the extracted signals (list below),
do the following, and do NOT attempt to identify, guess, or name any individual author:

1) List the signals that are likely to help an adversary attribute this code to a person or team.
2) For each signal, explain briefly how it could be used for attribution (1-2 sentences).
3) Provide 3 concise remediation suggestions to reduce attribution risk (redaction, replace with placeholders, remove provenance, aggregate outputs).
4) Do NOT infer or state who the author is, and do NOT provide contact information.

Signals (as JSON): 
{signals_json}

Payload header excerpt (for context, truncated):
{header_excerpt}

Code snippet excerpt (truncated):
{snippet_excerpt}
"""

def call_openrouter(prompt: str, model: str, temperature: float=0.0, max_tokens: int=256) -> Dict[str, Any]:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not found in environment (.env loaded or system env)")

    if requests is None:
        raise RuntimeError("requests package not available; install with `pip install requests`")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"model": model, "messages": [{"role":"system","content":"You are a privacy auditor."}, {"role":"user","content": prompt}], "temperature": temperature, "max_tokens": max_tokens}
    resp = requests.post(OPENROUTER_ENDPOINT, headers=headers, json=body, timeout=120)
    if resp.status_code != 200:
        logger.error("OpenRouter error: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
    return resp.json()

# Main driver
def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--payloads", default="results/generation_payloads.jsonl")
    p.add_argument("--snippets", default="results/code_snippets.jsonl")
    p.add_argument("--metadata", default="results/target_metadata.jsonl")
    p.add_argument("--commits", default="../Testing_GitHub_Code/results/commits.json")
    p.add_argument("--prs", default="../Testing_GitHub_Code/results/pull_requests.json")
    p.add_argument("--out", default="results/author_signals.jsonl")
    p.add_argument("--out-model", default="results/author_signals_model.jsonl")
    p.add_argument("--backend", choices=["local","openrouter"], default="local")
    p.add_argument("--model", default="openai/gpt-4o")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    payloads = load_jsonl(Path(args.payloads))
    snippets = {r.get("node_id"): r for r in load_jsonl(Path(args.snippets))}
    metadata = {r.get("node_id"): r for r in load_jsonl(Path(args.metadata))}
    commits = []
    prs = []
    try:
        if Path(args.commits).exists():
            commits = json.loads(Path(args.commits).read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to load commits.json")
    try:
        if Path(args.prs).exists():
            prs = json.loads(Path(args.prs).read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to load pull_requests.json")

    out_path = Path(args.out)
    out_model_path = Path(args.out_model)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_model_path.parent.mkdir(parents=True, exist_ok=True)

    model_results = []

    written = 0
    with open(out_path, "w", encoding="utf-8") as outf, open(out_model_path, "w", encoding="utf-8") as outf_model:
        for rec in payloads:
            node_id = rec.get("node_id")
            snip = snippets.get(node_id)
            meta = metadata.get(node_id)
            scan = scan_payload_for_signals(rec, snip, meta, commits, prs)
            # include truncated context for storage
            header_excerpt = safe_truncate((rec.get("components") or {}).get("header",""), 400)
            snippet_excerpt = safe_truncate((snip or {}).get("snippet",""), 1000)
            scan_record = {"node_id": node_id, "scan": scan, "header_excerpt": header_excerpt, "snippet_excerpt": snippet_excerpt, "timestamp": int(time.time())}
            outf.write(json.dumps(scan_record, ensure_ascii=False) + "\n")
            written += 1
            logger.debug(f"Wrote signals for {node_id} (risk={scan['risk_score']})")

            # optionally call model to provide human-friendly explanation of signals
            if args.backend == "openrouter" and not args.dry_run:
                prompt = MODEL_PROMPT_TEMPLATE.format(signals_json=json.dumps(scan["signals"], ensure_ascii=False, indent=2),
                                                      header_excerpt=header_excerpt,
                                                      snippet_excerpt=snippet_excerpt)
                try:
                    resp = call_openrouter(prompt=prompt, model=args.model, temperature=0.0, max_tokens=300)
                    # parse textual content
                    choices = resp.get("choices") or []
                    text = ""
                    if choices:
                        ch = choices[0]
                        msg = ch.get("message") or {}
                        text = msg.get("content") or ch.get("text") or ""
                    model_out = {"node_id": node_id, "text": text, "meta": {"model": args.model, "backend": "openrouter"}, "raw": resp}
                    outf_model.write(json.dumps(model_out, ensure_ascii=False) + "\n")
                    logger.debug(f"Wrote model explanation for {node_id}")
                except Exception as e:
                    logger.exception(f"Model call failed for {node_id}")
                    # write an error note
                    model_out = {"node_id": node_id, "error": str(e)}
                    outf_model.write(json.dumps(model_out, ensure_ascii=False) + "\n")

            # if backend local, write a small template explanation
            elif args.backend == "local":
                # generate a brief explanation based on scan
                explanation = []
                explanation.append(f"Detected {len(scan['signals'].get('emails',[]))} email(s), {len(scan['signals'].get('paths_with_users',[]))} user-path entries, {len(scan['signals'].get('sha_like',[]))} sha-like tokens.")
                explanation.append("Potential attribution vectors: provenance (commits/PRs), usernames in file paths, embedded emails, unique identifiers like CustomerID.")
                explanation.append("Remediations: redact file paths and emails; strip provenance; replace real identifiers with placeholders; aggregate outputs instead of raw snippets.")
                model_out = {"node_id": node_id, "text": "\n".join(explanation), "meta": {"backend":"local_stub"}}
                outf_model.write(json.dumps(model_out, ensure_ascii=False) + "\n")

        logger.info(f"Wrote {written} scan records to: {out_path}")
        logger.info(f"Wrote model explanations to: {out_model_path}")

if __name__ == "__main__":
    main()
