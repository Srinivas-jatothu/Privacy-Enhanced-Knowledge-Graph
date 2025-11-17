"""
step7_payload_assembly.py

Purpose:
  - Assemble KG-driven LLM payloads for each target into ordered, token-budgeted
    prompt texts suitable for single-shot generation (one-liner + 3-bullets + callers + provenance).

Inputs (defaults):
  - ./results/target_metadata.jsonl        (from step2)
  - ./results/target_graph_context.jsonl   (from step3)
  - ./results/ast_summaries.jsonl          (from step5)
  - ./results/code_snippets.jsonl          (from step6)

Output (defaults):
  - ./results/generation_payloads.jsonl
    Each record: { node_id, path, payload_text, est_tokens, components: {header, ast_summary, graph_summary, docs, code_snippet, instructions} }

Behavior & Logic:
  - For each target (node_id) assemble sections in order:
      HEADER (node id, path, provenance, signature)
      AST SUMMARY (signature, params, control hints)
      GRAPH SUMMARY (top callers/callees, same-file preferred neighbor)
      DOC EXCERPTS (short excerpts from docs neighbors if any)
      CODE SNIPPET (bounded from step6)
      INSTRUCTIONS (generation instructions: one-liner + 3 bullets + callers + provenance)
  - The script estimates token cost by using characters -> token heuristic (4 chars ~ 1 token) and enforces a token budget (default ~ 3000 tokens). If the assembled payload exceeds budget, it trims the code_snippet and docs sections first.
  - Detailed DEBUG logs are emitted for traceability.

Notes:
  - This script does NOT call any LLM. It only prepares and audits payloads.
  - Keep examples short; production prompts can be tailored to specific LLM models.

Usage:
  python step7_payload_assembly.py

CLI options:
  --targets, --context, --ast, --snippets, --out, --token-budget, --max-doc-chars

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
DEFAULT_TARGETS = Path('./results/target_metadata.jsonl')
DEFAULT_CONTEXT = Path('./results/target_graph_context.jsonl')
DEFAULT_AST = Path('./results/ast_summaries.jsonl')
DEFAULT_SNIPPETS = Path('./results/code_snippets.jsonl')
DEFAULT_OUTPUT = Path('./results/generation_payloads.jsonl')
DEFAULT_TOKEN_BUDGET = 3000  # approximate tokens per payload
DEFAULT_MAX_DOC_CHARS = 800  # chars to include from docs
DEFAULT_MAX_SNIPPET_CHARS = 2500

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger('step7_payload_assembly')
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


def estimate_tokens(text: str) -> int:
    """Rudimentary token estimate: 1 token ~= 4 chars (very approximate).
    This is conservative for English-like code+text mixed payloads.
    """
    if not text:
        return 0
    return max(1, int(len(text) / 4))


def truncate_by_chars(text: str, max_chars: int) -> str:
    if not text:
        return ''
    if len(text) <= max_chars:
        return text
    # attempt to trim to nearest line boundary
    head = text[:max_chars]
    if '\n' in head:
        # keep up to last newline to avoid cutting mid-line awkwardly
        i = head.rfind('\n')
        if i > max_chars * 0.6:
            return head[:i]
    return head


def assemble_header(meta: Dict[str, Any], ast_summary: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"Node: {meta.get('node_id')}")
    lines.append(f"Path: {meta.get('path')}")
    sig = ast_summary.get('signature') or ''
    if sig:
        lines.append(f"Signature: {sig}")
    # provenance if available inside meta.canonical
    canonical = meta.get('canonical') or {}
    prov = []
    if canonical.get('introduced_by_commit'):
        prov.append(f"introduced_by_commit={canonical.get('introduced_by_commit')}")
    if canonical.get('introduced_by_pr'):
        prov.append(f"introduced_by_pr={canonical.get('introduced_by_pr')}")
    if prov:
        lines.append('Provenance: ' + '; '.join(prov))
    return '\n'.join(lines)


def assemble_ast_section(ast_summary: Dict[str, Any]) -> str:
    lines = []
    sig = ast_summary.get('signature')
    params = ast_summary.get('params') or []
    if sig:
        lines.append(f"AST signature: {sig}")
    if params:
        lines.append('Parameters: ' + ', '.join(params))
    cf = ast_summary.get('control_flow') or []
    if cf:
        lines.append('Control flow hints: ' + ', '.join(cf))
    lits = ast_summary.get('literals') or []
    if lits:
        lines.append('Literals: ' + ', '.join(lits[:10]))
    return '\n'.join(lines)


def assemble_graph_section(context_rec: Dict[str, Any], node_v2_meta: Dict[str, Any]) -> str:
    # produce short callers/callees summary
    neighbors = context_rec.get('neighbors') or []
    callers = [n['node']['label'] for n in neighbors if n['category']=='callers'][:10]
    callees = [n['node']['label'] for n in neighbors if n['category']=='callees'][:10]
    same = [n['node']['label'] for n in neighbors if n['category']=='same_file'][:10]
    lines = []
    if callers:
        lines.append('Callers: ' + ', '.join(callers))
    if callees:
        lines.append('Callees: ' + ', '.join(callees))
    if same:
        lines.append('Same-file neighbors: ' + ', '.join(same))
    return '\n'.join(lines)


def assemble_docs(context_rec: Dict[str, Any], max_chars: int) -> str:
    docs = [n['node'] for n in (context_rec.get('neighbors') or []) if n['category']=='docs']
    out_texts = []
    for d in docs:
        # prefer module_doc or a short excerpt if present in the node
        md = d.get('module_doc') or d.get('doc') or d.get('label') or d.get('path')
        if not md:
            continue
        out_texts.append(str(md).strip())
        # stop if large
        if sum(len(x) for x in out_texts) > max_chars:
            break
    joined = '\n\n'.join(out_texts)
    return truncate_by_chars(joined, max_chars)


def assemble_code_snippet(snippet_rec: Dict[str, Any], max_chars: int) -> str:
    if not snippet_rec:
        return ''
    s = snippet_rec.get('snippet') or ''
    return truncate_by_chars(s, max_chars)


def assemble_instructions():
    return (
        "Generate a concise summary for the above function.\n"
        "- Provide a one-line purpose summary.\n"
        "- Provide 3 bullets: (1) Purpose, (2) Inputs/Outputs (types & meaning), (3) Side effects (I/O, DB, network).\n"
        "- List direct callers (as mentioned above).\n"
        "- Add a provenance note showing introduced_by_pr/commit if available.\n"
        "- If uncertain about behavior, state the uncertainty explicitly.\n"
        "Keep the summary factual and reference the provided code/span and KG provenance."
    )

# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog='step7_payload_assembly.py')
    p.add_argument('--targets', default=str(DEFAULT_TARGETS))
    p.add_argument('--context', default=str(DEFAULT_CONTEXT))
    p.add_argument('--ast', default=str(DEFAULT_AST))
    p.add_argument('--snippets', default=str(DEFAULT_SNIPPETS))
    p.add_argument('--out', default=str(DEFAULT_OUTPUT))
    p.add_argument('--token-budget', type=int, default=DEFAULT_TOKEN_BUDGET)
    p.add_argument('--max-doc-chars', type=int, default=DEFAULT_MAX_DOC_CHARS)
    p.add_argument('--max-snippet-chars', type=int, default=DEFAULT_MAX_SNIPPET_CHARS)
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug('Starting step7_payload_assembly')

    targets_path = Path(args.targets)
    context_path = Path(args.context)
    ast_path = Path(args.ast)
    snippets_path = Path(args.snippets)
    out_path = Path(args.out)
    token_budget = int(args.token_budget)
    max_doc_chars = int(args.max_doc_chars)
    max_snippet_chars = int(args.max_snippet_chars)

    # load inputs
    if not targets_path.exists():
        logger.error(f'Targets not found: {targets_path}')
        return
    if not context_path.exists():
        logger.error(f'Context not found: {context_path}')
        return
    if not ast_path.exists():
        logger.error(f'AST summaries not found: {ast_path}')
        return
    if not snippets_path.exists():
        logger.error(f'Code snippets not found: {snippets_path}')
        return

    targets = load_jsonl(targets_path)
    contexts = {r['node_id']: r for r in load_jsonl(context_path)}
    asts = {r['node_id']: r for r in load_jsonl(ast_path)}
    snippets = {r['node_id']: r for r in load_jsonl(snippets_path)}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, 'w', encoding='utf-8') as outf:
        for t in targets:
            node_id = t.get('node_id')
            path = t.get('path')
            logger.debug(f'Assembling payload for {node_id} ({path})')

            ast_summary = asts.get(node_id) or {}
            context_rec = contexts.get(node_id) or {}
            snippet_rec = snippets.get(node_id) or {}

            header = assemble_header(t, ast_summary)
            ast_sec = assemble_ast_section(ast_summary)
            graph_sec = assemble_graph_section(context_rec, t.get('canonical') or {})
            docs_sec = assemble_docs(context_rec, max_doc_chars)
            code_sec = assemble_code_snippet(snippet_rec, max_snippet_chars)
            instructions = assemble_instructions()

            # assemble full payload
            sections = []
            sections.append('### HEADER')
            sections.append(header)
            if ast_sec:
                sections.append('\n### AST SUMMARY')
                sections.append(ast_sec)
            if graph_sec:
                sections.append('\n### GRAPH CONTEXT')
                sections.append(graph_sec)
            if docs_sec:
                sections.append('\n### DOC EXCERPTS')
                sections.append(docs_sec)
            if code_sec:
                sections.append('\n### CODE SNIPPET')
                sections.append(code_sec)
            sections.append('\n### INSTRUCTIONS')
            sections.append(instructions)

            payload_text = '\n\n'.join(sections)
            est_tokens = estimate_tokens(payload_text)

            # if over budget, trim code & docs first
            if est_tokens > token_budget:
                logger.debug(f'Payload tokens ({est_tokens}) exceeds budget ({token_budget}). Trimming docs & snippets.')
                # try trimming docs
                docs_sec = truncate_by_chars(docs_sec, int(max_doc_chars / 2))
                code_sec = truncate_by_chars(code_sec, int(max_snippet_chars / 2))
                sections = []
                sections.append('### HEADER')
                sections.append(header)
                if ast_sec:
                    sections.append('\n### AST SUMMARY')
                    sections.append(ast_sec)
                if graph_sec:
                    sections.append('\n### GRAPH CONTEXT')
                    sections.append(graph_sec)
                if docs_sec:
                    sections.append('\n### DOC EXCERPTS')
                    sections.append(docs_sec)
                if code_sec:
                    sections.append('\n### CODE SNIPPET')
                    sections.append(code_sec)
                sections.append('\n### INSTRUCTIONS')
                sections.append(instructions)
                payload_text = '\n\n'.join(sections)
                est_tokens = estimate_tokens(payload_text)

            payload = {
                'node_id': node_id,
                'path': path,
                'payload_text': payload_text,
                'est_tokens': est_tokens,
                'components': {
                    'header': header,
                    'ast_summary': ast_sec,
                    'graph_summary': graph_sec,
                    'docs': docs_sec,
                    'code_snippet': code_sec,
                    'instructions': instructions
                }
            }

            outf.write(json.dumps(payload, ensure_ascii=False) + '\n')
            written += 1
            logger.debug(f'Wrote payload for {node_id}: est_tokens={est_tokens}')

    logger.info(f'Wrote {written} generation payloads to: {out_path}')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step7_payload_assembly')
        raise
