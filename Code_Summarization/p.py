# validate_payloads.py
import json, sys
from pathlib import Path

PAYLOADS = Path("results/generation_payloads.jsonl")
TARGETS = Path("results/target_metadata.jsonl")
NODE_V2 = Path("../Testing_GitHub_Code/results/node_v2.json")  # optional cross-check

def load_jsonl(p):
    out=[]
    with open(p,'r',encoding='utf-8') as fh:
        for ln in fh:
            ln=ln.strip()
            if ln:
                out.append(json.loads(ln))
    return out

def basic_checks(payload):
    errs=[]
    node_id = payload.get("node_id")
    if not node_id:
        errs.append("missing node_id")
    txt = payload.get("payload_text","")
    if not txt.strip():
        errs.append("empty payload_text")
    est = payload.get("est_tokens")
    if est is None:
        errs.append("missing est_tokens")
    else:
        if not isinstance(est,int) or est<=0:
            errs.append(f"bad est_tokens: {est}")
    comps = payload.get("components",{})
    # snippet check
    snippet = comps.get("code_snippet","")
    if not snippet or len(snippet.strip())<10:
        errs.append("snippet too small/empty")
    # header/provenance
    header = comps.get("header","")
    if "Node:" not in header or "Path:" not in header:
        errs.append("header missing Node/Path")
    # AST check
    ast = comps.get("ast_summary","")
    if not ast.strip():
        errs.append("ast_summary empty")
    # ensure instructions present
    instr = comps.get("instructions","")
    if "one-line" not in instr.lower() and "one-line" not in payload.get("payload_text","").lower():
        # not fatal, but warn
        errs.append("instructions might be missing expected guidance")
    return errs

def main():
    if not PAYLOADS.exists():
        print("No payloads found at", PAYLOADS); sys.exit(1)
    payloads = load_jsonl(PAYLOADS)
    targets = load_jsonl(TARGETS) if TARGETS.exists() else []
    print(f"Loaded {len(payloads)} payloads; targets file has {len(targets)} records")
    total_errs=0
    sample_failures=[]
    for i,p in enumerate(payloads):
        errs = basic_checks(p)
        if errs:
            total_errs += 1
            if len(sample_failures)<10:
                sample_failures.append({"index":i,"node_id":p.get("node_id"),"errors":errs})
    print("Payloads with problems:", total_errs)
    if sample_failures:
        print("\nSample failures (up to 10):")
        for s in sample_failures:
            print(json.dumps(s, indent=2))
    else:
        print("No basic problems detected.")
    # Optional: list payloads exceeding token budget (if present)
    over = [p for p in payloads if p.get("est_tokens",0) > 2800]
    print(f"Payloads >2800 tokens: {len(over)}")
    if over:
        print("Examples of large payloads (node_id, est_tokens):")
        for o in over[:5]:
            print(o.get("node_id"), o.get("est_tokens"))

if __name__ == '__main__':
    main()
