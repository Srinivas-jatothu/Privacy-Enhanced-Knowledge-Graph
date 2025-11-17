"""
step11_persist_into_kg.py

Purpose:
  - Attach generated summaries to canonical nodes in node_v2.json.
  - Produce an audit CSV that records: node_id, one_liner, confidence, validation_status, generated_at, introduced_by_commit, introduced_by_pr, summary_source (local/openrouter), and snippet_ref.

Inputs (defaults):
  - results/auto_summaries.jsonl
  - results/summaries_validated.jsonl
  - ../Testing_GitHub_Code/results/node_v2.json

Outputs:
  - results/node_v2_with_summaries.json  (copy of node_v2.json with added `summaries` field on nodes)
  - results/node_v2_summaries_audit.csv  (audit CSV for easy review)
  - backup of original node_v2.json at ../Testing_GitHub_Code/results/node_v2.json.bak.TIMESTAMP

Notes:
  - This script is conservative: it will only attach summaries for node_ids that exist in node_v2.json. If a summary's node_id is missing, it will write a warning to the audit CSV and skip attachment.
  - Each summary object attached to a node will include provenance fields, model metadata and validation status.
  - The script logs actions in debug-level output for traceability.

Usage:
  python step11_persist_into_kg.py

"""

import argparse
import csv
import json
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# -----------------------------
# Config / defaults
# -----------------------------
DEFAULT_AUTO = Path('./results/auto_summaries.jsonl')
DEFAULT_VALID = Path('./results/summaries_validated.jsonl')
DEFAULT_NODE_V2 = Path('../Testing_GitHub_Code/results/node_v2.json')
OUT_NODE_V2 = Path('./results/node_v2_with_summaries.json')
OUT_AUDIT = Path('./results/node_v2_summaries_audit.csv')
BACKUP_SUFFIX = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger('step11_persist_into_kg')
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
    if not path.exists():
        logger.error(f'File not found: {path}');
        return out
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
    if not path.exists():
        logger.error(f'File not found: {path}');
        return None
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    logger.info(f'Wrote JSON to: {path}')

# -----------------------------
# Main
# -----------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(prog='step11_persist_into_kg.py')
    p.add_argument('--auto', default=str(DEFAULT_AUTO))
    p.add_argument('--validated', default=str(DEFAULT_VALID))
    p.add_argument('--node-v2', default=str(DEFAULT_NODE_V2))
    p.add_argument('--out-node', default=str(OUT_NODE_V2))
    p.add_argument('--out-audit', default=str(OUT_AUDIT))
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug('Starting step11_persist_into_kg')

    auto_path = Path(args.auto)
    valid_path = Path(args.validated)
    node_v2_path = Path(args.node_v2)
    out_node_path = Path(args.out_node)
    out_audit_path = Path(args.out_audit)

    auto_records = load_jsonl(auto_path)
    valid_records = load_jsonl(valid_path)

    # build validated map: node_id -> validation struct
    valid_map = {r.get('node_id'): r.get('validation') for r in valid_records}

    node_v2 = load_json(node_v2_path)
    if node_v2 is None:
        logger.error('node_v2.json could not be loaded; aborting')
        return

    # create a backup of the canonical node_v2.json
    try:
        backup_path = node_v2_path.with_name(node_v2_path.name + f'.bak.{BACKUP_SUFFIX}')
        shutil.copy2(node_v2_path, backup_path)
        logger.info(f'Backed up original node_v2.json to: {backup_path}')
    except Exception:
        logger.exception('Failed to backup node_v2.json; continuing cautiously')

    # index nodes by id for quick lookup
    node_index = {}
    if isinstance(node_v2, list):
        for n in node_v2:
            node_index[n.get('id')] = n
    elif isinstance(node_v2, dict) and node_v2.get('nodes'):
        for n in node_v2.get('nodes'):
            node_index[n.get('id')] = n
    else:
        logger.error('node_v2.json format not recognized (expect list or {nodes: [...]}); aborting')
        return

    audit_rows = []
    attached = 0
    skipped = 0

    for rec in auto_records:
        node_id = rec.get('node_id')
        one_liner = rec.get('one_liner')
        bullets = rec.get('bullets')
        callers = rec.get('callers')
        provenance_note = rec.get('provenance_note')
        confidence = rec.get('confidence')
        model_meta = rec.get('model_metadata') or {}
        raw_output = rec.get('raw_model_output')
        gen_time = rec.get('generation_time_s')

        validation = valid_map.get(node_id) or {'status': 'UNKNOWN', 'notes': []}

        node = node_index.get(node_id)
        if not node:
            logger.warning(f'Node {node_id} not found in node_v2.json; skipping attachment')
            skipped += 1
            audit_rows.append({
                'node_id': node_id,
                'status': 'SKIPPED_NODE_MISSING',
                'one_liner': one_liner or '',
                'confidence': confidence or '',
                'validation_status': validation.get('status') if isinstance(validation, dict) else validation,
                'introduced_by_commit': '',
                'introduced_by_pr': '',
                'notes': 'node not found in canonical KG'
            })
            continue

        # Build summary object to attach
        summary_obj = {
            'one_liner': one_liner,
            'bullets': bullets,
            'callers': callers,
            'provenance_note': provenance_note,
            'confidence': confidence,
            'validation': validation,
            'model_metadata': model_meta,
            'raw_output_truncated': (raw_output[:200] + '...') if raw_output and len(raw_output)>200 else raw_output,
            'generated_at': datetime.utcnow().isoformat() + 'Z'
        }

        # Attach under a stable key: node['summaries'] is a list of versions
        if node.get('summaries') is None:
            node['summaries'] = []
        node['summaries'].append(summary_obj)
        attached += 1

        # audit row
        audit_rows.append({
            'node_id': node_id,
            'status': 'ATTACHED',
            'one_liner': one_liner or '',
            'confidence': confidence or '',
            'validation_status': validation.get('status') if isinstance(validation, dict) else validation,
            'introduced_by_commit': (node.get('introduced_by_commit') or node.get('metadata',{}).get('introduced_by_commit')) or '',
            'introduced_by_pr': (node.get('introduced_by_pr') or node.get('metadata',{}).get('introduced_by_pr')) or '',
            'notes': ''
        })

    # write out updated node_v2 (preserve original shape: if original was list, produce list; if dict, keep dict tag)
    if isinstance(node_v2, list):
        out_data = [node_index[n.get('id')] for n in node_v2]
    else:
        # replace nodes in node_v2['nodes']
        new_nodes = []
        for n in node_v2.get('nodes'):
            nid = n.get('id')
            if nid in node_index:
                new_nodes.append(node_index[nid])
            else:
                new_nodes.append(n)
        node_v2['nodes'] = new_nodes
        out_data = node_v2

    write_json(out_node_path, out_data)

    # write audit CSV
    out_audit_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_audit_path, 'w', newline='', encoding='utf-8') as csvf:
        fieldnames = ['node_id','status','one_liner','confidence','validation_status','introduced_by_commit','introduced_by_pr','notes']
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()
        for r in audit_rows:
            writer.writerow(r)

    logger.info(f'Attached {attached} summaries; skipped {skipped} summaries (missing nodes).')
    logger.info(f'Wrote updated KG to: {out_node_path} and audit CSV to: {out_audit_path}')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled error in step11_persist_into_kg')
        raise
