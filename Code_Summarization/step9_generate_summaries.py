# step9_generate_summaries.py
# Version: OpenRouter + .env loader

"""
Generates summaries using:
 - backend 'openrouter' (requires OPENROUTER_API_KEY in .env)
 - backend 'local' (offline deterministic)

Reads:
    results/generation_payloads.jsonl

Writes:
    results/auto_summaries.jsonl
    results/summaries_validated.jsonl
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv   # <--- important

# -------------------------------------------------------
# LOAD .env AUTOMATICALLY
# -------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_PATH = SCRIPT_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    print(f"Loaded environment from {ENV_PATH}")
else:
    print(f"WARNING: .env not found at {ENV_PATH}, using system environment")

# -------------------------------------------------------
# Defaults
# -------------------------------------------------------
DEFAULT_PAYLOADS = Path('./results/generation_payloads.jsonl')
DEFAULT_OUT = Path('./results/auto_summaries.jsonl')
DEFAULT_VALIDATED = Path('./results/summaries_validated.jsonl')
DEFAULT_BACKEND = 'openrouter'   # now default
DEFAULT_MODEL = 'openai/gpt-4o'
DEFAULT_TEMPERATURE = 0.0
DEFAULT_MAX_TOKENS = 512

OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"

# -------------------------------------------------------
# Logging
# -------------------------------------------------------
logger = logging.getLogger('step9_generate_summaries')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items = []
    logger.debug(f"Loading JSONL from {path}")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    logger.debug(f"Loaded {len(items)} records from {path}")
    return items

def write_jsonl(path: Path, records: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info(f"Wrote {len(records)} records to {path}")

# ---------------- OpenRouter Chat Wrapper --------------------
def call_openrouter_chat(prompt: str, model: str, temperature: float, max_tokens: int):
    api_key = os.getenv("OPENROUTER_API_KEY", None)
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY not found in environment. "
            "Ensure .env file contains: OPENROUTER_API_KEY=xxxx"
        )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a concise, accurate code summarizer."},
            {"role": "user", "content": prompt}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    logger.debug(f"POST to OpenRouter: {model}")

    r = requests.post(OPENROUTER_ENDPOINT, headers=headers, json=body, timeout=200)
    if r.status_code != 200:
        logger.error(f"OpenRouter Error: {r.status_code} => {r.text}")
        r.raise_for_status()

    return r.json()

def parse_openrouter_response(resp: Dict[str, Any]):
    """Extract text from OpenRouter's OpenAI-compatible response."""
    try:
        choices = resp.get("choices", [])
        if not choices:
            return {"text": "", "meta": {}, "raw": resp}

        msg = choices[0].get("message", {})
        txt = msg.get("content", "")

        meta = {
            "model": resp.get("model"),
            "usage": resp.get("usage"),
        }

        return {"text": txt, "meta": meta, "raw": resp}
    except Exception:
        logger.exception("Failed to parse OpenRouter response")
        return {"text": "", "meta": {}, "raw": resp}

# ---------------- Local Stub (offline testing) -------------------
def local_stub(prompt: str):
    return {
        "text": (
            "ONE-LINER: Stub summary\n\n"
            "BULLETS:\n"
            "- Purpose bullet\n"
            "- IO bullet\n"
            "- Side effects bullet"
        ),
        "meta": {"backend": "local"},
        "raw": None,
    }

# ------------------- Simple parser ----------------------------
def parse_model_output(text: str):
    """Extract minimal structure."""
    out = {"one_liner": None, "bullets": []}

    if not text.strip():
        return out

    lines = text.splitlines()

    # first line as one-liner
    out["one_liner"] = lines[0].strip()

    # bullets
    for ln in lines[1:]:
        ln = ln.strip()
        if ln.startswith("-"):
            out["bullets"].append(ln[1:].strip())

    return out

# ------------------- (Light) validation step -------------------
def validate(structured, ast_summary, graph_context):
    notes = []
    status = "PASS"

    if not structured.get("one_liner"):
        notes.append("Missing one-liner")
        status = "WARN"

    if len(structured.get("bullets", [])) < 2:
        notes.append("Too few bullets")
        status = "WARN"

    return {"status": status, "notes": notes}

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--payloads", default=str(DEFAULT_PAYLOADS))
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--validated", default=str(DEFAULT_VALIDATED))
    p.add_argument("--backend", default=DEFAULT_BACKEND, choices=["openrouter", "local"])
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--temperature", default=DEFAULT_TEMPERATURE, type=float)
    p.add_argument("--max-tokens", default=DEFAULT_MAX_TOKENS, type=int)

    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)

    logger.debug("Starting step9_generate_summaries")
    payloads = load_jsonl(Path(args.payloads))

    out_records = []
    validated_records = []

    # Load AST + Graph for validation
    ast_map = {x["node_id"]: x for x in load_jsonl(Path("results/ast_summaries.jsonl"))}
    ctx_map = {x["node_id"]: x for x in load_jsonl(Path("results/target_graph_context.jsonl"))}

    for rec in payloads:
        node_id = rec["node_id"]
        logger.debug(f"Generating summary for {node_id}")

        prompt = rec["payload_text"]

        if args.backend == "openrouter":
            resp = call_openrouter_chat(
                prompt=prompt,
                model=args.model,
                temperature=args.temperature,
                max_tokens=args.max_tokens,
            )
            parsed = parse_openrouter_response(resp)
            text = parsed["text"]
            meta = parsed["meta"]
        else:
            stub = local_stub(prompt)
            text = stub["text"]
            meta = stub["meta"]

        structured = parse_model_output(text)
        validation = validate(structured, ast_map.get(node_id, {}), ctx_map.get(node_id, {}))

        out_records.append({
            "node_id": node_id,
            "one_liner": structured.get("one_liner"),
            "bullets": structured.get("bullets"),
            "raw_output": text,
            "meta": meta,
        })

        validated_records.append({
            "node_id": node_id,
            "validation": validation
        })

    write_jsonl(Path(args.out), out_records)
    write_jsonl(Path(args.validated), validated_records)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error in step9_generate_summaries")
        raise




'''
In this, we have used llm named "openai/gpt-4o" with backend "openrouter". It will 
'''