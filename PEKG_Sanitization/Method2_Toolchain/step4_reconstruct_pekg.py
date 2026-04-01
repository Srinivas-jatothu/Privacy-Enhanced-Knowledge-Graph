"""
============================================================
step4_reconstruct_pekg.py
============================================================
PURPOSE:
    Reconstruct and validate the final Privacy Enhanced
    Knowledge Graph (PEKG) from the sanitized intermediate
    files produced by steps 2 and 3.

    This step:
        1. Loads kg_after_identifier_sanitization.json
           and edges_sanitized.json
        2. Validates graph integrity:
               - All edge source/target IDs exist in node list
               - No dangling edges
               - Node count preserved
               - Edge count preserved
        3. Runs a final privacy scan to confirm no sensitive
           values remain (spot check against known patterns)
        4. Produces the final pekg_method2.json
        5. Produces a combined final report summarizing
           all sanitization steps

USAGE:
    python step4_reconstruct_pekg.py

INPUT:
    - results/kg_after_identifier_sanitization.json  (from step3)
    - results/edges_sanitized.json                   (from step3)
    - results/sensitive_item_registry.csv            (from step2)
    - results/identifier_registry.csv                (from step3)
    - Original node_v2.json                          (for comparison)
    - Original edges.json                            (for comparison)

OUTPUT:
    - results/pekg_method2.json
        Final Privacy Enhanced Knowledge Graph
    - results/pekg_method2_edges.json
        Final sanitized edges
    - results/final_privacy_scan.csv
        Residual sensitive values found (should be empty)
    - results/final_report_method2.txt
        Complete sanitization report for thesis
============================================================
"""

import json
import os
import csv
import re
from collections import defaultdict
from loguru import logger

# ============================================================
# CONFIG
# ============================================================

INPUT_KG        = os.path.join(os.path.dirname(__file__), "results", "kg_after_identifier_sanitization.json")
INPUT_EDGES     = os.path.join(os.path.dirname(__file__), "results", "edges_sanitized.json")
REGISTRY_STEP2  = os.path.join(os.path.dirname(__file__), "results", "sensitive_item_registry.csv")
REGISTRY_STEP3  = os.path.join(os.path.dirname(__file__), "results", "identifier_registry.csv")
ORIGINAL_KG     = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Testing_GitHub_Code\results\node_v2.json"
ORIGINAL_EDGES  = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Testing_GitHub_Code\results\edges.json"

OUTPUT_DIR      = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_KG           = os.path.join(OUTPUT_DIR, "pekg_method2.json")
OUTPUT_EDGES        = os.path.join(OUTPUT_DIR, "pekg_method2_edges.json")
PRIVACY_SCAN_FILE   = os.path.join(OUTPUT_DIR, "final_privacy_scan.csv")
FINAL_REPORT_FILE   = os.path.join(OUTPUT_DIR, "final_report_method2.txt")

# ============================================================
# PRIVACY SCAN PATTERNS
# These are patterns that should NOT appear in the final PEKG
# If found, they indicate sanitization gaps
# ============================================================

SENSITIVE_PATTERNS = [
    # Real email addresses
    (re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}(?<!placeholder)'), "EMAIL"),

    # GitHub URLs with real org/repo names
    (re.compile(r'https://github\.com/[A-Za-z]'), "GITHUB_URL"),

    # Full commit hashes (40 hex chars)
    (re.compile(r'\b[0-9a-f]{40}\b'), "COMMIT_HASH"),

    # Datetime timestamps
    (re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'), "TIMESTAMP"),

    # Known contributor names (from our registry)
    (re.compile(r'\b(Thomas|Ashkan|Ghanavati|Moheth|Bardia|Komal|kokomocha|baridamm)\b',
                re.IGNORECASE), "CONTRIBUTOR_NAME"),

    # Internal module paths (3+ dotted levels with known prefixes)
    (re.compile(r'\b(dags|gcpdeploy|src)\.[a-z_]+\.[a-z_]+'), "MODULE_PATH"),

    # Internal file paths
    (re.compile(r'dags/src/[a-z_]+\.py'), "FILE_PATH"),
]

# Fields that are SAFE to skip in privacy scan
SKIP_FIELDS = {"type", "lineno", "end_lineno", "size", "mime",
               "version", "id", "source", "target", "relation"}


def scan_value_for_sensitive(value, field_name):
    """
    Scan a string value for residual sensitive patterns.
    Returns list of (pattern_type, matched_text) tuples.
    """
    if not value or not isinstance(value, str):
        return []
    if field_name in SKIP_FIELDS:
        return []

    findings = []
    for pattern, pattern_type in SENSITIVE_PATTERNS:
        matches = pattern.findall(value)
        for match in matches:
            findings.append((pattern_type, str(match)[:80]))

    return findings


def scan_node_for_residuals(node):
    """
    Scan all fields of a node for residual sensitive values.
    Returns list of finding dicts.
    """
    findings = []
    node_id   = node.get("id", "UNKNOWN")
    node_type = node.get("type", "UNKNOWN")

    def check_field(field_name, value):
        if isinstance(value, list):
            for item in value:
                check_field(field_name, item)
        elif isinstance(value, dict):
            for k, v in value.items():
                check_field(f"{field_name}.{k}", v)
        elif isinstance(value, str):
            hits = scan_value_for_sensitive(value, field_name)
            for pattern_type, matched in hits:
                findings.append({
                    "node_id":      node_id,
                    "node_type":    node_type,
                    "field":        field_name,
                    "pattern_type": pattern_type,
                    "matched_text": matched,
                    "severity":     "HIGH" if pattern_type in
                                    ("EMAIL", "COMMIT_HASH", "CONTRIBUTOR_NAME")
                                    else "MEDIUM"
                })

    for field, value in node.items():
        check_field(field, value)

    return findings


# ============================================================
# GRAPH INTEGRITY VALIDATION
# ============================================================

def validate_graph(nodes, edges):
    """
    Validate that the sanitized graph is internally consistent.
    Returns (is_valid, issues_list)
    """
    issues = []
    node_ids = {node["id"] for node in nodes}

    # Check all edge endpoints exist
    missing_sources = 0
    missing_targets = 0
    for edge in edges:
        source = edge.get("source", "")
        target = edge.get("target", "")
        if source not in node_ids:
            missing_sources += 1
        if target not in node_ids:
            missing_targets += 1

    if missing_sources > 0:
        issues.append(f"WARNING: {missing_sources} edges have source IDs not in node list")
    if missing_targets > 0:
        issues.append(f"WARNING: {missing_targets} edges have target IDs not in node list")

    # Check for duplicate node IDs
    id_counts = defaultdict(int)
    for node in nodes:
        id_counts[node["id"]] += 1
    duplicates = {k: v for k, v in id_counts.items() if v > 1}
    if duplicates:
        issues.append(f"WARNING: {len(duplicates)} duplicate node IDs found")

    is_valid = len(issues) == 0
    return is_valid, issues


# ============================================================
# LOAD REGISTRY STATS
# ============================================================

def load_registry_stats(registry_file):
    """Load a registry CSV and return basic stats."""
    rows = []
    try:
        with open(registry_file, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        logger.warning(f"Could not load registry {registry_file}: {e}")
    return rows


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Final PEKG Reconstruction (Step 4)...")

    # --------------------------------------------------------
    # Load sanitized KG and edges
    # --------------------------------------------------------
    logger.info(f"Loading sanitized KG from: {INPUT_KG}")
    with open(INPUT_KG, 'r', encoding='utf-8') as f:
        data = json.load(f)

    is_dict = isinstance(data, dict)
    nodes   = list(data.values()) if is_dict else data
    logger.info(f"Sanitized nodes loaded: {len(nodes)}")

    logger.info(f"Loading sanitized edges from: {INPUT_EDGES}")
    with open(INPUT_EDGES, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    logger.info(f"Sanitized edges loaded: {len(edges)}")

    # --------------------------------------------------------
    # Load original KG for comparison
    # --------------------------------------------------------
    logger.info("Loading original KG for comparison...")
    with open(ORIGINAL_KG, 'r', encoding='utf-8') as f:
        original_data = json.load(f)
    with open(ORIGINAL_EDGES, 'r', encoding='utf-8') as f:
        original_edges = json.load(f)

    original_nodes = list(original_data.values()) if isinstance(original_data, dict) else original_data

    # --------------------------------------------------------
    # STEP A: Validate graph integrity
    # --------------------------------------------------------
    logger.info("Validating graph integrity...")
    is_valid, issues = validate_graph(nodes, edges)

    if is_valid:
        logger.success("Graph integrity: VALID - all edges have valid endpoints")
    else:
        for issue in issues:
            logger.warning(issue)

    # --------------------------------------------------------
    # STEP B: Final privacy scan
    # --------------------------------------------------------
    logger.info("Running final privacy scan...")
    all_residuals = []

    for node in nodes:
        residuals = scan_node_for_residuals(node)
        all_residuals.extend(residuals)

    if all_residuals:
        logger.warning(f"Privacy scan found {len(all_residuals)} residual sensitive values!")
    else:
        logger.success("Privacy scan: CLEAN - no residual sensitive values found")

    # Save privacy scan results
    scan_fieldnames = ["node_id", "node_type", "field",
                       "pattern_type", "matched_text", "severity"]
    with open(PRIVACY_SCAN_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=scan_fieldnames)
        writer.writeheader()
        writer.writerows(all_residuals)
    logger.success(f"Privacy scan saved: {PRIVACY_SCAN_FILE} ({len(all_residuals)} findings)")

    # --------------------------------------------------------
    # STEP C: Save final PEKG
    # --------------------------------------------------------
    logger.info("Saving final PEKG...")

    if is_dict:
        output_data = {node["id"]: node for node in nodes}
    else:
        output_data = nodes

    with open(OUTPUT_KG, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    logger.success(f"Final PEKG saved: {OUTPUT_KG}")

    with open(OUTPUT_EDGES, 'w', encoding='utf-8') as f:
        json.dump(edges, f, indent=2, ensure_ascii=False)
    logger.success(f"Final PEKG edges saved: {OUTPUT_EDGES}")

    # --------------------------------------------------------
    # STEP D: Load registry stats for final report
    # --------------------------------------------------------
    step2_rows = load_registry_stats(REGISTRY_STEP2)
    step3_rows = load_registry_stats(REGISTRY_STEP3)

    step2_by_type   = defaultdict(int)
    step2_by_tool   = defaultdict(int)
    for row in step2_rows:
        step2_by_type[row.get("leakage_type", "?")] += 1
        step2_by_tool[row.get("tool", "?")]         += 1

    step3_by_field  = defaultdict(int)
    for row in step3_rows:
        step3_by_field[row.get("field", "?")] += 1

    residual_by_type = defaultdict(int)
    for r in all_residuals:
        residual_by_type[r["pattern_type"]] += 1

    # --------------------------------------------------------
    # STEP E: Node type distribution comparison
    # --------------------------------------------------------
    orig_type_counts = defaultdict(int)
    san_type_counts  = defaultdict(int)
    for node in original_nodes:
        orig_type_counts[node.get("type", "?")] += 1
    for node in nodes:
        san_type_counts[node.get("type", "?")] += 1

    # --------------------------------------------------------
    # STEP F: Build final report
    # --------------------------------------------------------
    report_lines = [
        "=" * 70,
        "FINAL REPORT: METHOD 2 - TOOL-CHAIN SANITIZATION PIPELINE",
        "Privacy Enhanced Knowledge Graph (PEKG)",
        "=" * 70,
        "",
        "OVERVIEW",
        "-" * 40,
        f"  Original KG nodes     : {len(original_nodes)}",
        f"  Sanitized KG nodes    : {len(nodes)}",
        f"  Original KG edges     : {len(original_edges)}",
        f"  Sanitized KG edges    : {len(edges)}",
        f"  Node count preserved  : {'YES' if len(nodes) == len(original_nodes) else 'NO'}",
        f"  Edge count preserved  : {'YES' if len(edges) == len(original_edges) else 'NO'}",
        f"  Graph integrity       : {'VALID' if is_valid else 'ISSUES FOUND'}",
        "",
        "SANITIZATION PIPELINE SUMMARY",
        "-" * 40,
        "",
        "  Step 1 - KG Audit:",
        f"    Fields classified    : 16426",
        f"    PROVENANCE fields    : 6678  (40.66%)",
        f"    IDENTIFIER fields    : 3704  (22.55%)",
        f"    SAFE fields          : 3644  (22.18%)",
        f"    STRUCTURAL fields    : 1564  (9.52%)",
        f"    LITERAL fields       :  836  (5.09%)",
        f"    UNKNOWN fields       :    0  (0.00%) - fully resolved",
        "",
        "  Step 2 - Literal & PII Sanitization:",
        f"    Total replacements   : {len(step2_rows)}",
        f"    False positives blocked: 79",
        f"    Unique contributors  : 10",
    ]

    for tool, count in sorted(step2_by_tool.items(), key=lambda x: -x[1]):
        report_lines.append(f"    Tool: {tool:<25} {count:>6} replacements")

    report_lines += [
        "",
        "  Step 3 - Identifier & Structural Sanitization:",
        f"    Total replacements   : {len(step3_rows)}",
        f"    Edge endpoints updated: 2130",
    ]

    for field, count in sorted(step3_by_field.items(), key=lambda x: -x[1])[:8]:
        report_lines.append(f"    Field: {field:<35} {count:>6} replacements")

    report_lines += [
        "",
        f"  TOTAL REPLACEMENTS ACROSS ALL STEPS: "
        f"{len(step2_rows) + len(step3_rows)}",
        "",
        "PRIVACY SCAN RESULTS",
        "-" * 40,
        f"  Residual sensitive values found: {len(all_residuals)}",
    ]

    if all_residuals:
        report_lines.append("  Residuals by type:")
        for rtype, count in sorted(residual_by_type.items(), key=lambda x: -x[1]):
            report_lines.append(f"    {rtype:<30} {count:>4}")
    else:
        report_lines.append("  Result: CLEAN - no residual sensitive values detected")

    report_lines += [
        "",
        "NODE TYPE DISTRIBUTION (Original vs Sanitized)",
        "-" * 40,
        f"  {'Node Type':<20} {'Original':>10} {'Sanitized':>10} {'Match':>8}",
        f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8}",
    ]
    all_types = sorted(set(list(orig_type_counts.keys()) + list(san_type_counts.keys())))
    for ntype in all_types:
        orig  = orig_type_counts[ntype]
        san   = san_type_counts[ntype]
        match = "✓" if orig == san else "✗"
        report_lines.append(f"  {ntype:<20} {orig:>10} {san:>10} {match:>8}")

    report_lines += [
        "",
        "METHOD 2 DOCUMENTED WEAKNESSES (for thesis)",
        "-" * 40,
        "  1. IDENTIFIER LEAKAGE (partial):",
        "     Counter-based replacements (func_N) lose semantic role.",
        "     LLM cannot distinguish data-cleaning from ML functions.",
        "     → Addressed in Method 3 with role-aware abstraction.",
        "",
        "  2. FALSE POSITIVES IN PII DETECTION:",
        "     Presidio misclassified 79 technical terms as PII.",
        "     Required a domain-specific software terms blocklist.",
        "     → Highlights gap in general NLP tools for SE context.",
        "",
        "  3. COMMIT MESSAGE PARTIAL SANITIZATION:",
        "     Free-text commit messages partially sanitized.",
        "     Domain-specific project terms may remain.",
        "     → Method 3 uses semantic understanding instead.",
        "",
        "  4. STRUCTURAL CONTEXT LOSS:",
        "     File type hints preserved (dag_file, test_file)",
        "     but full structural relationships anonymized.",
        "",
        "OUTPUT FILES",
        "-" * 40,
        f"  Final PEKG          : pekg_method2.json",
        f"  Final Edges         : pekg_method2_edges.json",
        f"  Privacy Scan        : final_privacy_scan.csv",
        f"  Step2 Registry      : sensitive_item_registry.csv",
        f"  Step3 Registry      : identifier_registry.csv",
        "=" * 70,
    ]

    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    with open(FINAL_REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report_text)
    logger.success(f"Final report saved: {FINAL_REPORT_FILE}")

    # --------------------------------------------------------
    # Final console summary
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4 COMPLETE")
    print("=" * 60)
    print(f"Final PEKG     : {OUTPUT_KG}")
    print(f"Final Edges    : {OUTPUT_EDGES}")
    print(f"Privacy scan   : {len(all_residuals)} residual findings")
    print(f"Graph valid    : {'YES' if is_valid else 'NO - check issues above'}")
    total = len(step2_rows) + len(step3_rows)
    print(f"Total replaced : {total} sensitive items across all steps")
    print("=" * 60)
    print("\nMethod 2 sanitization pipeline COMPLETE.")
    print("Ready for Step 5: Run summarization on pekg_method2.json")


if __name__ == "__main__":
    main()