"""
step6_extract_code_snippets.py

Purpose:
  - Extract bounded code snippets for each target function (or file node) to be used
    in generation payloads. Prefer source stored in code_entities_full.json; fallback
    to reading the repository file directly.

  - Produce results/code_snippets.jsonl with fields:
      {
        node_id,
        path,
        start_line, end_line,
        snippet,           # trimmed to token/line budget
        snippet_type,      # 'full_function' | 'trimmed' | 'top_lines' | 'fallback_file'
        source_hint,       # where the snippet came from (code_entities | repo_file)
        reason
      }

Notes:
  - This script does not perform sanitization; it returns raw code. In a production
    run you should add redaction or tokenization as required by privacy policy.
  - The script is careful to keep snippets under a configurable line budget.

Usage:
  python step6_extract_code_snippets.py

Defaults & CLI args:
  --map         : results/target_ast_map.jsonl
  --code-entities: ../Testing_GitHub_Code/results/code_entities_full.json
  --repo-root   : ../Ecommerce-Data-MLOps (fallback to read raw files)
  --out         : results/code_snippets.jsonl
  --max-lines   : 80  (max lines to include in snippet)
  --context-lines: 4  (extra context lines before/after function body when trimmed)

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
DEFAULT_MAP = Path('./results/target_ast_map.jsonl')
DEFAULT_CODE_ENTITIES = Path('../Testing_GitHub_Code/results/code_entities_full.json')
DEFAULT_REPO_ROOT = Path('../Ecommerce-Data-MLOps')
DEFAULT_OUTPUT = Path('./results/code_snippets.jsonl')
DEFAULT_MAX_LINES = 80
DEFAULT_CONTEXT_LINES = 4

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger('step6_extract_code_snippets')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# -----------------------------
# Helpers
# -----------------------------

def load_json(path: Path) -> Any:
    logger.debug(f'Loading JSON from: {path}')
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    logger.debug(f'Loading JSONL from: {path}')
    out = []
    with open(path, 'r', encoding='utf-8') as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            out.append(json.loads(ln))
    logger.debug(f'Loaded {len(out)} records')
    return out


def build_code_entities_index(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    index = {}
    if not path.exists():
        return index
    try:
        raw = load_json(path)
        records = []
        if isinstance(raw, list):
            records = raw
        elif isinstance(raw, dict):
            records = raw.get('items') or raw.get('entities') or list(raw.values())
        for entry in records:
            rel = entry.get('relpath') or entry.get('path') or entry.get('file') or entry.get('filepath')
            if not rel:
                continue
            funcs = entry.get('functions') or []
            for f in funcs:
                index.setdefault(rel, []).append(f)
    except Exception:
        logger.exception('Failed building code entities index')
    logger.debug(f'Indexed code entities for {len(index)} files')
    return index


def read_repo_file(repo_root: Path, relpath: str) -> Optional[List[str]]:
    p = (repo_root / relpath).resolve()
    if not p.exists():
        # try without leading ./
        p2 = (repo_root / relpath.lstrip('./\\')).resolve()
        if p2.exists():
            p = p2
        else:
            return None
    try:
        with open(p, 'r', encoding='utf-8') as fh:
            return fh.read().splitlines()
    except Exception:
        logger.exception(f'Failed to read file from repo: {p}')
        return None


def extract_snippet_from_source(source: str, start: Optional[int], end: Optional[int], max_lines: int, context: int) -> Dict[str, Any]:
    lines = source.splitlines()
    total = len(lines)
    if start and end and 1 <= start <= end <= total:
        # indices in code_entities are 1-based
        s = max(1, start - context)
        e = min(total, end + context)
        selected = lines[s-1:e]
        snippet_type = 'full_function' if (e - s + 1) <= max_lines else 'trimmed'
        if snippet_type == 'trimmed':
            # prefer top N lines of function body while keeping signature + context
            head = lines[max(0, start-1): min(total, start-1 + max_lines - 2)]
            selected = lines[max(0, start-context-1): max(0, start-1)] + head
        return {'snippet': '\n'.join(selected), 'snippet_type': snippet_type, 'source_hint': 'code_entities', 'reason': 'span_extract'}
    else:
        # fallback: return top max_lines
        selected = lines[:max_lines]
        return {'snippet': '\n'.join(selected), 'snippet_type': 'top_lines', 'source_hint': 'code_entities', 'reason': 'no_span_fallback'}


def trim_snippet(snippet: str, max_lines: int) -> str:
    lines = snippet.splitlines()
    if len(lines) <= max_lines:
        return snippet
    # keep signature (first 1-3 lines) + last lines
    head = lines[:3]
    tail = lines[: max_lines - len(head)]
    return '\n'.join(head + tail)

# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog='step6_extract_code_snippets.py')
    p.add_argument('--map', default=str(DEFAULT_MAP))
    p.add_argument('--code-entities', default=str(DEFAULT_CODE_ENTITIES))
    p.add_argument('--repo-root', default=str(DEFAULT_REPO_ROOT))
    p.add_argument('--out', default=str(DEFAULT_OUTPUT))
    p.add_argument('--max-lines', type=int, default=DEFAULT_MAX_LINES)
    p.add_argument('--context-lines', type=int, default=DEFAULT_CONTEXT_LINES)
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug('Starting step6_extract_code_snippets')

    map_path = Path(args.map)
    code_entities_path = Path(args.code_entities)
    repo_root = Path(args.repo_root)
    out_path = Path(args.out)
    max_lines = int(args.max_lines)
    context_lines = int(args.context_lines)

    if not map_path.exists():
        logger.error(f'Target AST map not found: {map_path}')
        return

    records = load_jsonl(map_path)
    code_index = build_code_entities_index(code_entities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, 'w', encoding='utf-8') as outf:
        for r in records:
            node_id = r.get('node_id')
            path = r.get('path')
            start = r.get('ast_start_line')
            end = r.get('ast_end_line')
            chosen = r.get('chosen_neighbor') or r.get('chosen') or None

            snippet_rec = {
                'node_id': node_id,
                'path': path,
                'start_line': start,
                'end_line': end,
                'snippet': None,
                'snippet_type': None,
                'source_hint': None,
                'reason': None
            }

            # 1) try code_entities source if available
            found = None
            if chosen and chosen.get('id', '').startswith('code_entities'):
                # chosen created by step4
                rel = path
                funcs = code_index.get(rel, [])
                for f in funcs:
                    if (f.get('name') or '').lower() == (chosen.get('label') or '').lower():
                        found = f
                        break
            # 2) try direct path match in code_index
            if not found:
                funcs = code_index.get(path) or []
                for f in funcs:
                    if (f.get('name') or '').lower() == ((chosen.get('label') if chosen else '') or '').lower():
                        found = f
                        break
            # 3) if found in code_entities, use its source field
            if found and found.get('source'):
                res = extract_snippet_from_source(found.get('source'), found.get('lineno') or found.get('start_line'), found.get('end_lineno') or found.get('end_line'), max_lines, context_lines)
                snippet_rec.update(res)
            else:
                # 4) fallback to reading file from repo
                lines = read_repo_file(repo_root, path)
                if lines:
                    total = len(lines)
                    if start and end and 1 <= start <= end <= total:
                        s = max(1, int(start) - context_lines)
                        e = min(total, int(end) + context_lines)
                        selected = lines[s-1:e]
                        snippet = '\n'.join(selected)
                        if (e - s + 1) > max_lines:
                            snippet = '\n'.join(lines[start-1: min(total, start-1 + max_lines - 2)])
                            snippet_type = 'trimmed'
                        else:
                            snippet_type = 'full_function'
                        snippet_rec.update({'snippet': snippet, 'snippet_type': snippet_type, 'source_hint': 'repo_file', 'reason': 'repo_extract'})
                    else:
                        # top lines of file
                        snippet = '\n'.join(lines[:max_lines])
                        snippet_rec.update({'snippet': snippet, 'snippet_type': 'top_lines', 'source_hint': 'repo_file', 'reason': 'repo_top_lines'})
                else:
                    # final fallback: empty snippet
                    snippet_rec.update({'snippet': '', 'snippet_type': 'none', 'source_hint': None, 'reason': 'no_source_found'})

            # trim snippet if too long (defensive)
            if snippet_rec.get('snippet'):
                snippet_rec['snippet'] = trim_snippet(snippet_rec['snippet'], max_lines)

            outf.write(json.dumps(snippet_rec, ensure_ascii=False) + '\n')
            written += 1
            logger.debug(f"Wrote snippet for {node_id}: type={snippet_rec.get('snippet_type')} reason={snippet_rec.get('reason')}")

    logger.info(f'Wrote {written} code snippets to: {out_path}')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step6_extract_code_snippets')
        raise
