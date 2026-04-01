"""
============================================================
step1_kg_audit.py
============================================================
PURPOSE:
    Audit the Knowledge Graph (node_v2.json) and classify
    every field of every node into one of 4 leakage categories:
        1. Literal Leakage    - URLs, emails, tokens, secrets
        2. Identifier Leakage - function names, class names, module names
        3. Provenance Leakage - author names, commit hashes, PR numbers, dates
        4. Structural Leakage - file paths, folder names, module paths
        5. Safe               - no sensitive information

    This audit is the FOUNDATION of the entire Method 2 pipeline.
    Every subsequent sanitization step is based on this audit.

USAGE:
    python step1_kg_audit.py

INPUT:
    - node_v2.json  : Main KG node file
                      Path: ../../Testing_GitHub_Code/results/node_v2.json

OUTPUT:
    - results/audit_report.csv        : Full field-level audit of every node
    - results/audit_summary.csv       : Summary count per leakage category
    - results/audit_node_types.json   : All unique node types and their fields
    - results/audit_leakage_map.json  : Leakage classification map for use in later steps
============================================================
"""

import json
import os
import csv
import re
from collections import defaultdict
from loguru import logger

# ============================================================
# CONFIG - paths
# ============================================================

KG_FILE = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Testing_GitHub_Code\results\node_v2.json"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# LEAKAGE CLASSIFICATION RULES
# Based on manual inspection + UNKNOWN field resolution
# ============================================================

FIELD_LEAKAGE_MAP = {

    # --- PROVENANCE LEAKAGE ---
    "introduced_by_commit":     "PROVENANCE",
    "modified_by_commits":      "PROVENANCE",
    "sha":                      "PROVENANCE",
    "author":                   "PROVENANCE",
    "date":                     "PROVENANCE",
    "prs":                      "PROVENANCE",
    "introduced_by_pr":         "PROVENANCE",
    "introduced_by_pr_title":   "PROVENANCE",
    "message":                  "PROVENANCE",
    "created_at":               "PROVENANCE",
    "merged_at":                "PROVENANCE",
    "title":                    "PROVENANCE",

    # --- IDENTIFIER LEAKAGE ---
    "label":                    "IDENTIFIER",
    "name":                     "IDENTIFIER",
    "qualified_name":           "IDENTIFIER",
    "docstring":                "IDENTIFIER",
    "signature":                "IDENTIFIER",
    "text_preview":             "IDENTIFIER",

    # --- STRUCTURAL LEAKAGE ---
    "file":                     "STRUCTURAL",
    "path":                     "STRUCTURAL",

    # --- LITERAL LEAKAGE ---
    "hash":                     "LITERAL",
    "html_url":                 "LITERAL",

    # --- SAFE ---
    "lineno":                   "SAFE",
    "end_lineno":               "SAFE",
    "mime":                     "SAFE",
    "size":                     "SAFE",
    "version":                  "SAFE",

    # --- SPECIAL ---
    "id":                       "IDENTIFIER",
    "type":                     "SAFE",
    "attrs":                    "NESTED",
}

ATTRS_FIELD_LEAKAGE_MAP = {
    # SAFE
    "type":             "SAFE",
    "lineno":           "SAFE",
    "end_lineno":       "SAFE",
    "state":            "SAFE",

    # IDENTIFIER
    "qualified_name":   "IDENTIFIER",

    # STRUCTURAL
    "module":           "STRUCTURAL",
    "file":             "STRUCTURAL",

    # LITERAL
    "file_hash":        "LITERAL",
    "html_url":         "LITERAL",

    # PROVENANCE
    "sha":              "PROVENANCE",
    "message_head":     "PROVENANCE",
    "author_name":      "PROVENANCE",
    "author_email":     "PROVENANCE",
    "author_date":      "PROVENANCE",
    "parents":          "PROVENANCE",
    "closed_at":        "PROVENANCE",
    "commits":          "PROVENANCE",
    "created_at":       "PROVENANCE",
    "merged_at":        "PROVENANCE",
    "title":            "PROVENANCE",
    "user":             "PROVENANCE",
    "merge_commit_sha": "PROVENANCE",
}

# ============================================================
# REGEX PATTERNS for value-level auto detection
# ============================================================

COMMIT_HASH_PATTERN  = re.compile(r'^[0-9a-f]{40}$')
URL_PATTERN          = re.compile(r'https?://\S+')
EMAIL_PATTERN        = re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]+')
FILE_PATH_PATTERN    = re.compile(r'[\w./-]+\.(py|json|yaml|yml|txt|md|csv|sh|cfg|ini|toml)')
MODULE_PATH_PATTERN  = re.compile(r'^[a-z_]+(\.[a-z_]+){1,}$')
DATETIME_PATTERN     = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')


def detect_value_leakage(value):
    """Detect leakage type from value pattern alone."""
    if value is None:
        return "SAFE"
    if isinstance(value, (int, float, bool)):
        return "SAFE"

    val_str = str(value).strip()

    if COMMIT_HASH_PATTERN.match(val_str):
        return "PROVENANCE"
    if DATETIME_PATTERN.search(val_str):
        return "PROVENANCE"
    if EMAIL_PATTERN.search(val_str):
        return "PROVENANCE"
    if URL_PATTERN.search(val_str):
        return "LITERAL"
    if FILE_PATH_PATTERN.search(val_str):
        return "STRUCTURAL"
    if MODULE_PATH_PATTERN.match(val_str):
        return "STRUCTURAL"

    return "UNKNOWN"


# ============================================================
# CORE AUDIT FUNCTION
# ============================================================

def audit_node(node):
    """Audit a single node and return field-level findings."""
    findings  = []
    node_id   = node.get("id",   "UNKNOWN")
    node_type = node.get("type", "UNKNOWN")

    for field, value in node.items():

        # Handle nested attrs separately
        if field == "attrs":
            if isinstance(value, dict):
                for attr_field, attr_value in value.items():
                    category       = ATTRS_FIELD_LEAKAGE_MAP.get(attr_field, "UNKNOWN")
                    value_category = detect_value_leakage(attr_value)
                    final_category = category if category != "UNKNOWN" else value_category

                    findings.append({
                        "node_id":          node_id,
                        "node_type":        node_type,
                        "field":            f"attrs.{attr_field}",
                        "value_sample":     str(attr_value)[:80] if attr_value else "NULL",
                        "field_category":   category,
                        "value_category":   value_category,
                        "final_category":   final_category,
                        "action":           get_action(final_category)
                    })
            continue

        # Handle list values
        if isinstance(value, list):
            sample = str(value[0])[:80] if value else "EMPTY_LIST"
        else:
            sample = str(value)[:80] if value is not None else "NULL"

        category       = FIELD_LEAKAGE_MAP.get(field, "UNKNOWN")
        value_category = detect_value_leakage(
            value if not isinstance(value, list) else (value[0] if value else None)
        )
        final_category = category if category != "UNKNOWN" else value_category

        findings.append({
            "node_id":          node_id,
            "node_type":        node_type,
            "field":            field,
            "value_sample":     sample,
            "field_category":   category,
            "value_category":   value_category,
            "final_category":   final_category,
            "action":           get_action(final_category)
        })

    return findings


def get_action(category):
    """Map leakage category to sanitization action."""
    actions = {
        "LITERAL":      "REPLACE with [LITERAL_TYPE]",
        "IDENTIFIER":   "REPLACE with role-based placeholder",
        "PROVENANCE":   "REPLACE with generalized value",
        "STRUCTURAL":   "REPLACE with abstracted path",
        "SAFE":         "KEEP as-is",
        "NESTED":       "INSPECT individually",
        "UNKNOWN":      "MANUAL REVIEW needed",
    }
    return actions.get(category, "MANUAL REVIEW needed")


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting KG Audit...")
    logger.info(f"Loading KG from: {KG_FILE}")

    with open(KG_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    nodes = data if isinstance(data, list) else list(data.values())
    logger.info(f"Total nodes loaded: {len(nodes)}")

    all_findings     = []
    node_type_fields = defaultdict(set)
    leakage_counts   = defaultdict(int)

    for node in nodes:
        findings = audit_node(node)
        all_findings.extend(findings)

        node_type = node.get("type", "UNKNOWN")
        for f in findings:
            node_type_fields[node_type].add(f["field"])
            leakage_counts[f["final_category"]] += 1

    logger.info(f"Total field findings: {len(all_findings)}")

    # --------------------------------------------------------
    # Save audit_report.csv
    # --------------------------------------------------------
    report_path = os.path.join(OUTPUT_DIR, "audit_report.csv")
    fieldnames  = ["node_id", "node_type", "field", "value_sample",
                   "field_category", "value_category", "final_category", "action"]

    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_findings)
    logger.success(f"Audit report saved: {report_path}")

    # --------------------------------------------------------
    # Save audit_summary.csv
    # --------------------------------------------------------
    summary_path = os.path.join(OUTPUT_DIR, "audit_summary.csv")
    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["leakage_category", "field_count", "percentage"])
        total = sum(leakage_counts.values())
        for category, count in sorted(leakage_counts.items(), key=lambda x: -x[1]):
            pct = round(count / total * 100, 2)
            writer.writerow([category, count, f"{pct}%"])
    logger.success(f"Audit summary saved: {summary_path}")

    # --------------------------------------------------------
    # Save audit_node_types.json
    # --------------------------------------------------------
    node_types_path = os.path.join(OUTPUT_DIR, "audit_node_types.json")
    node_type_fields_serializable = {
        k: sorted(list(v)) for k, v in node_type_fields.items()
    }
    with open(node_types_path, 'w', encoding='utf-8') as f:
        json.dump(node_type_fields_serializable, f, indent=2)
    logger.success(f"Node types map saved: {node_types_path}")

    # --------------------------------------------------------
    # Save audit_leakage_map.json
    # --------------------------------------------------------
    leakage_map = {
        "field_leakage_map":       FIELD_LEAKAGE_MAP,
        "attrs_field_leakage_map": ATTRS_FIELD_LEAKAGE_MAP
    }
    leakage_map_path = os.path.join(OUTPUT_DIR, "audit_leakage_map.json")
    with open(leakage_map_path, 'w', encoding='utf-8') as f:
        json.dump(leakage_map, f, indent=2)
    logger.success(f"Leakage map saved: {leakage_map_path}")

    # --------------------------------------------------------
    # Console summary
    # --------------------------------------------------------
    print("\n" + "="*60)
    print("KG AUDIT SUMMARY")
    print("="*60)
    print(f"Total nodes audited  : {len(nodes)}")
    print(f"Total field findings : {len(all_findings)}")
    print()
    print(f"{'Category':<20} {'Count':>8} {'Percentage':>12}")
    print("-"*42)
    total = sum(leakage_counts.values())
    for category, count in sorted(leakage_counts.items(), key=lambda x: -x[1]):
        pct = round(count / total * 100, 2)
        print(f"{category:<20} {count:>8} {pct:>11}%")
    print()
    print("Node Types found:")
    for node_type, fields in sorted(node_type_fields_serializable.items()):
        print(f"  {node_type:<20} -> {len(fields)} fields")
    print("="*60)
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")

    # --------------------------------------------------------
    # Report remaining UNKNOWNs
    # --------------------------------------------------------
    print("\nUNKNOWN fields remaining (need manual review):")
    unknowns_remaining = [f for f in all_findings if f["final_category"] == "UNKNOWN"]
    if unknowns_remaining:
        seen = set()
        for f in unknowns_remaining:
            key = f"{f['node_type']} -> {f['field']}"
            if key not in seen:
                seen.add(key)
                print(f"  {key:<60} | {f['value_sample'][:50]}")
    else:
        print("  None! All fields classified. Ready for sanitization.")


if __name__ == "__main__":
    main()