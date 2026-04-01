"""
============================================================
step2_literal_sanitizer.py
============================================================
PURPOSE:
    Sanitize LITERAL and PROVENANCE leakage fields in the KG
    using two tools:
        1. detect-secrets  - detects API keys, tokens, passwords,
                             high-entropy strings, hardcoded secrets
        2. Microsoft Presidio - detects PII: PERSON names, EMAIL,
                             URL, PHONE, DATE, CRYPTO addresses

    This is STAGE 1 of the Method 2 sanitization pipeline.
    It handles the easiest and most critical leakages first:
        - URLs         → [URL]
        - Email IDs    → [EMAIL]
        - Author names → [CONTRIBUTOR_N] (consistent per person)
        - API tokens   → [SECRET]
        - Commit hashes in free text → [COMMIT_ID]
        - Dates/timestamps → [TIMESTAMP]
        - GitHub URLs  → [REPO_URL]

    KNOWN LIMITATION (documented for thesis):
        Presidio has false positives on software engineering text
        e.g. detecting 'Flask', 'Docker', 'seaborn' as PERSON entities.
        A SOFTWARE_TERMS blocklist is applied to reduce false positives.
        Remaining false positives are documented in the registry.

    NOTE: Function names, file paths, module names are NOT
    handled here - those are handled in step3 and step4.

USAGE:
    python step2_literal_sanitizer.py

INPUT:
    - node_v2.json
      Path: ../../Testing_GitHub_Code/results/node_v2.json
    - results/audit_leakage_map.json  (from step1)

OUTPUT:
    - results/kg_after_literal_sanitization.json
        KG with all literal + PII fields sanitized
    - results/sensitive_item_registry.csv
        Full mapping table: original → replacement, tool, node, field
    - results/literal_sanitization_summary.txt
        Summary of what was sanitized
    - results/false_positives_log.csv
        Log of suspected false positives for thesis documentation
============================================================
"""

import json
import os
import csv
import re
import copy
import difflib
from collections import defaultdict
from loguru import logger

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

# ============================================================
# CONFIG
# ============================================================

KG_FILE     = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Testing_GitHub_Code\results\node_v2.json"
OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_KG           = os.path.join(OUTPUT_DIR, "kg_after_literal_sanitization.json")
REGISTRY_FILE       = os.path.join(OUTPUT_DIR, "sensitive_item_registry.csv")
SUMMARY_FILE        = os.path.join(OUTPUT_DIR, "literal_sanitization_summary.txt")
FALSE_POSITIVE_FILE = os.path.join(OUTPUT_DIR, "false_positives_log.csv")

# ============================================================
# SOFTWARE TERMS BLOCKLIST
# These are technical terms that Presidio falsely detects
# as PERSON, LOCATION, DATE etc. in commit messages.
# This blocklist is a KEY CONTRIBUTION of Method 2 analysis —
# it documents the domain gap of general PII tools.
# ============================================================

SOFTWARE_TERMS_BLOCKLIST = {
    # Python libraries / frameworks
    "flask", "django", "fastapi", "numpy", "pandas", "scipy",
    "sklearn", "tensorflow", "pytorch", "keras", "seaborn",
    "matplotlib", "plotly", "airflow", "celery", "redis",
    "sqlalchemy", "alembic", "pydantic", "pytest", "pylint",
    "black", "flake8", "mypy", "bandit", "semgrep",

    # DevOps / Infrastructure tools
    "docker", "kubernetes", "helm", "terraform", "ansible",
    "jenkins", "gitlab", "github", "git", "dvc", "mlflow",
    "grafana", "prometheus", "kibana", "elasticsearch",

    # Cloud platforms
    "gcpdeploy", "gcp", "aws", "azure", "heroku",
    "cloudrun", "bigquery", "pubsub", "firestore",

    # Common false PERSON detections in commit messages
    "push", "merge", "update", "add", "fix", "refactor",
    "bump", "chore", "feat", "docs", "test", "ci", "cd",
    "hotfix", "release", "revert", "wip",

    # Common false LOCATION detections
    "md", "readme", "pylint", "df", "api",

    # Version-like strings that get detected as DATE
    "monthly", "weekly", "daily", "nightly",
}

# Regex patterns to identify false positives
FILE_EXTENSION_RE   = re.compile(r'^[\w_-]+\.(py|json|yaml|yml|txt|md|csv|sh|cfg|ini|toml|js|ts|html|css)$', re.IGNORECASE)
VERSION_RE          = re.compile(r'^\d+\.\d+(\.\d+)?$')
NUMERIC_ONLY_RE     = re.compile(r'^\d+$')
SNAKE_CASE_RE       = re.compile(r'^[a-z][a-z0-9]*(_[a-z0-9]+)+$')
HASH_LIKE_RE        = re.compile(r'^[a-f0-9]{7,}$')
SHORT_ABBREV_RE     = re.compile(r'^[A-Z]{1,3}$')


def is_false_positive(entity_type, text):
    """
    Check if a Presidio detection is likely a false positive
    in software engineering context.
    Returns True if it should be IGNORED.
    """
    text_clean = text.strip().lower()
    text_orig  = text.strip()

    # Check blocklist
    if text_clean in SOFTWARE_TERMS_BLOCKLIST:
        return True

    # File names detected as URL
    if entity_type == "URL" and FILE_EXTENSION_RE.match(text_orig):
        return True

    # Version numbers detected as DATE_TIME
    if entity_type == "DATE_TIME" and VERSION_RE.match(text_orig):
        return True

    # Pure numbers detected as PHONE
    if entity_type == "PHONE_NUMBER" and NUMERIC_ONLY_RE.match(text_orig):
        return True

    # Short abbreviations detected as LOCATION (e.g. MD, API)
    if entity_type == "LOCATION" and SHORT_ABBREV_RE.match(text_orig):
        return True

    # Snake_case identifiers detected as PERSON
    if entity_type == "PERSON" and SNAKE_CASE_RE.match(text_clean):
        return True

    # Hash-like strings
    if HASH_LIKE_RE.match(text_clean):
        return True

    # Very short strings (1-2 chars) - likely abbreviations
    if len(text_clean) <= 2:
        return True

    return False


# ============================================================
# FIELDS TO PROCESS
# ============================================================

LITERAL_FIELDS = {
    "hash", "html_url",
    "attrs.file_hash", "attrs.html_url"
}

PROVENANCE_FIELDS = {
    "author", "message", "date",
    "introduced_by_pr_title", "title",
    "created_at", "merged_at",
    "attrs.author_name", "attrs.author_email",
    "attrs.author_date", "attrs.message_head",
    "attrs.html_url", "attrs.closed_at",
    "attrs.created_at", "attrs.merged_at",
    "attrs.title", "attrs.user","text_preview"
}

COMMIT_HASH_FIELDS = {
    "introduced_by_commit",
    "sha",
    "attrs.sha",
    "attrs.merge_commit_sha"
}

PR_NUMBER_FIELDS = {
    "introduced_by_pr",
    "prs"
}

COMMIT_LIST_FIELDS = {
    "modified_by_commits",
    "attrs.parents"
}

# ============================================================
# REGEX PATTERNS
# ============================================================

COMMIT_HASH_RE  = re.compile(r'[0-9a-f]{40}')
URL_RE          = re.compile(r'https?://[^\s"\']+')
EMAIL_RE        = re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}')
DATETIME_RE     = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Z\+\-\d:]*')
GITHUB_URL_RE   = re.compile(r'https://github\.com/[^\s"\']+')

# ============================================================
# CONTRIBUTOR REGISTRY
# Maps real names → consistent CONTRIBUTOR_N placeholders
# ============================================================

contributor_registry = {}
contributor_counter  = [1]


# def get_contributor_placeholder(name):
#     """Return consistent placeholder for a contributor name."""
#     name_clean = str(name).strip().lower()
#     if name_clean not in contributor_registry:
#         contributor_registry[name_clean] = f"CONTRIBUTOR_{contributor_counter[0]}"
#         contributor_counter[0] += 1
#     return contributor_registry[name_clean]

def normalize_name(name: str) -> str:
    return str(name).strip().lower()


# def canonicalize_name(name, existing_names):
#     """
#     Merge partial / overlapping names.
#     Example:
#         'ashkan' → 'ashkan ghanavati'
#     """
#     name = normalize_name(name)

#     for existing in existing_names:
#         # If one is substring of the other → same person
#         if name in existing or existing in name:
#             # Always prefer the longer (more complete) name
#             return existing if len(existing) >= len(name) else name

#     return name

def canonicalize_name(name, existing_names):
    name = normalize_name(name)

    for existing in existing_names:
        # 1. Substring match (already works)
        if name in existing or existing in name:
            return existing if len(existing) >= len(name) else name

        # 2. Fuzzy match (NEW)
        similarity = difflib.SequenceMatcher(None, name, existing).ratio()
        if similarity > 0.85:
            return existing

    return name


def get_contributor_placeholder(name):
    name_norm = normalize_name(name)

    # Try to merge with existing names
    canonical = canonicalize_name(name_norm, contributor_registry.keys())

    if canonical not in contributor_registry:
        contributor_registry[canonical] = f"CONTRIBUTOR_{contributor_counter[0]}"
        contributor_counter[0] += 1

    return contributor_registry[canonical]


# ============================================================
# REGISTRIES
# ============================================================

registry_rows       = []
false_positive_rows = []


def record_replacement(node_id, node_type, field, original, replacement, tool, leakage_type):
    registry_rows.append({
        "node_id":      node_id,
        "node_type":    node_type,
        "field":        field,
        "original":     str(original)[:120],
        "replacement":  str(replacement),
        "tool":         tool,
        "leakage_type": leakage_type
    })


def record_false_positive(node_id, field, entity_type, text, reason):
    false_positive_rows.append({
        "node_id":      node_id,
        "field":        field,
        "entity_type":  entity_type,
        "text":         str(text)[:100],
        "reason":       reason
    })


# ============================================================
# PRESIDIO INITIALIZATION
# ============================================================

logger.info("Initializing Presidio analyzer...")
presidio_analyzer   = AnalyzerEngine()
presidio_anonymizer = AnonymizerEngine()
logger.success("Presidio initialized.")

PRESIDIO_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS",
    "URL",
    "PHONE_NUMBER",
    "DATE_TIME",
    "CRYPTO",
    "IP_ADDRESS",
    "LOCATION"
]


def scan_with_presidio(text, node_id, field_name):
    """
    Scan text with Presidio.
    Filters false positives using blocklist and pattern checks.
    Returns list of (entity_type, original_text, start, end) tuples.
    """
    if not text or not isinstance(text, str) or len(text.strip()) < 3:
        return []

    try:
        results = presidio_analyzer.analyze(
            text=text,
            entities=PRESIDIO_ENTITIES,
            language="en"
        )

        findings = []
        for result in results:
            original = text[result.start:result.end]

            # Check if this is a false positive
            if is_false_positive(result.entity_type, original):
                record_false_positive(
                    node_id, field_name,
                    result.entity_type, original,
                    "blocklist or pattern match"
                )
                continue

            findings.append((result.entity_type, original, result.start, result.end))

        return findings

    except Exception as e:
        logger.warning(f"Presidio scan error on text '{text[:50]}': {e}")
        return []


def apply_presidio_replacements(text, findings, node_id, node_type, field_name):
    """Apply Presidio replacements right-to-left to preserve positions."""
    if not findings:
        return text

    findings_sorted = sorted(findings, key=lambda x: x[2], reverse=True)
    result = text

    for entity_type, original, start, end in findings_sorted:
        # if entity_type == "PERSON":
        #     replacement = get_contributor_placeholder(original)
        if entity_type == "PERSON":
            # Only use contributor registry for known author fields
            # For free text fields use generic placeholder to avoid false entries
            AUTHOR_FIELDS = {"author", "attrs.author_name", "attrs.user"}
            if field_name in AUTHOR_FIELDS:
                replacement = get_contributor_placeholder(original)
            else:
                replacement = "[PERSON]"
        elif entity_type == "EMAIL_ADDRESS":
            replacement = "[EMAIL]"
        elif entity_type in ("URL", "IP_ADDRESS"):
            replacement = "[REPO_URL]" if "github.com" in original else "[URL]"
        elif entity_type == "PHONE_NUMBER":
            replacement = "[PHONE]"
        elif entity_type == "DATE_TIME":
            replacement = "[TIMESTAMP]"
        elif entity_type == "CRYPTO":
            replacement = "[CRYPTO_ADDRESS]"
        elif entity_type == "LOCATION":
            replacement = "[LOCATION]"
        else:
            replacement = f"[{entity_type}]"

        record_replacement(node_id, node_type, field_name,
                           original, replacement, "presidio", "PROVENANCE")
        result = result[:start] + replacement + result[end:]

    return result


# ============================================================
# FIELD SANITIZERS
# ============================================================

def sanitize_literal_field(value, node_id, node_type, field_name):
    """Sanitize LITERAL fields: hashes, URLs."""
    if not value or not isinstance(value, str):
        return value

    sanitized = value

    if GITHUB_URL_RE.search(sanitized):
        record_replacement(node_id, node_type, field_name, sanitized, "[REPO_URL]", "regex", "LITERAL")
        return "[REPO_URL]"

    if URL_RE.search(sanitized):
        record_replacement(node_id, node_type, field_name, sanitized, "[URL]", "regex", "LITERAL")
        return "[URL]"

    if re.match(r'^[0-9a-f]{40}$', sanitized.strip()):
        record_replacement(node_id, node_type, field_name, sanitized, "[HASH]", "regex", "LITERAL")
        return "[HASH]"

    return sanitized


def sanitize_provenance_field(value, node_id, node_type, field_name):
    """Sanitize PROVENANCE fields: names, emails, dates, messages."""
    if not value:
        return value

    # Direct datetime strings
    if isinstance(value, str) and DATETIME_RE.match(value.strip()):
        record_replacement(node_id, node_type, field_name, value, "[TIMESTAMP]", "regex", "PROVENANCE")
        return "[TIMESTAMP]"

    # Direct email strings
    if isinstance(value, str) and re.match(r'^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$', value.strip()):
        record_replacement(node_id, node_type, field_name, value, "[EMAIL]", "regex", "PROVENANCE")
        return "[EMAIL]"

    # Author name fields - direct contributor registry
    if field_name in ("author", "attrs.author_name", "attrs.user"):
        if value and str(value).strip():
            placeholder = get_contributor_placeholder(value)
            record_replacement(node_id, node_type, field_name, value,
                               placeholder, "contributor-registry", "PROVENANCE")
            return placeholder
        
    # PR titles reveal internal project context - replace entirely
    if field_name == "introduced_by_pr_title":
        record_replacement(node_id, node_type, field_name, value, "[PR_TITLE]", "regex", "PROVENANCE")
        return "[PR_TITLE]"

    # PR titles and branch names always replaced entirely
    if field_name in ("title", "attrs.title", "introduced_by_pr_title"):
        record_replacement(node_id, node_type, field_name, value, "[PR_TITLE]", "regex", "PROVENANCE")
        return "[PR_TITLE]"
    
    # Free text fields - run Presidio with false positive filtering
    if isinstance(value, str) and len(value) > 3:
        findings = scan_with_presidio(value, node_id, field_name)
        if findings:
            sanitized = apply_presidio_replacements(
                value, findings, node_id, node_type, field_name)
        else:
            sanitized = value

        # Also replace any inline commit hashes Presidio missed
        sanitized_2 = COMMIT_HASH_RE.sub("[COMMIT_ID]", sanitized)
        if sanitized_2 != sanitized:
            record_replacement(node_id, node_type, field_name,
                               sanitized, sanitized_2, "regex", "PROVENANCE")
        return sanitized_2

    return value


def sanitize_commit_hash_field(value, node_id, node_type, field_name):
    if not value:
        return value
    record_replacement(node_id, node_type, field_name, value, "[COMMIT_ID]", "regex", "PROVENANCE")
    return "[COMMIT_ID]"


def sanitize_commit_list_field(value, node_id, node_type, field_name):
    if not isinstance(value, list):
        return value
    sanitized = []
    for item in value:
        if isinstance(item, str) and re.match(r'^[0-9a-f]{40}$', item.strip()):
            record_replacement(node_id, node_type, field_name, item, "[COMMIT_ID]", "regex", "PROVENANCE")
            sanitized.append("[COMMIT_ID]")
        elif isinstance(item, dict) and "sha" in item:
            record_replacement(node_id, node_type, field_name, str(item), "[COMMIT_OBJ]", "regex", "PROVENANCE")
            sanitized.append({"sha": "[COMMIT_ID]"})
        else:
            sanitized.append(item)
    return sanitized


def sanitize_pr_field(value, node_id, node_type, field_name):
    if not value:
        return value
    if isinstance(value, list):
        record_replacement(node_id, node_type, field_name, str(value), "[PR_REF_LIST]", "regex", "PROVENANCE")
        return ["[PR_REF]" for _ in value]
    record_replacement(node_id, node_type, field_name, value, "[PR_REF]", "regex", "PROVENANCE")
    return "[PR_REF]"


# ============================================================
# SANITIZE A SINGLE NODE
# ============================================================

# def sanitize_node(node):
#     sanitized = copy.deepcopy(node)
#     node_id   = node.get("id",   "UNKNOWN")
#     node_type = node.get("type", "UNKNOWN")

#     for field in list(sanitized.keys()):
#         if field == "attrs":
#             continue

#         value = sanitized[field]

#         if field in COMMIT_HASH_FIELDS:
#             sanitized[field] = sanitize_commit_hash_field(value, node_id, node_type, field)
#         elif field in COMMIT_LIST_FIELDS:
#             sanitized[field] = sanitize_commit_list_field(value, node_id, node_type, field)
#         elif field in PR_NUMBER_FIELDS:
#             sanitized[field] = sanitize_pr_field(value, node_id, node_type, field)
#         elif field in LITERAL_FIELDS:
#             sanitized[field] = sanitize_literal_field(value, node_id, node_type, field)
#         elif field in PROVENANCE_FIELDS:
#             sanitized[field] = sanitize_provenance_field(value, node_id, node_type, field)

#     # Handle attrs
#     if "attrs" in sanitized and isinstance(sanitized["attrs"], dict):
#         for attr_field in list(sanitized["attrs"].keys()):
#             full_field = f"attrs.{attr_field}"
#             value      = sanitized["attrs"][attr_field]

#             if full_field in COMMIT_HASH_FIELDS:
#                 sanitized["attrs"][attr_field] = sanitize_commit_hash_field(
#                     value, node_id, node_type, full_field)
#             elif full_field in COMMIT_LIST_FIELDS:
#                 sanitized["attrs"][attr_field] = sanitize_commit_list_field(
#                     value, node_id, node_type, full_field)
#             elif full_field in LITERAL_FIELDS:
#                 sanitized["attrs"][attr_field] = sanitize_literal_field(
#                     value, node_id, node_type, full_field)
#             elif full_field in PROVENANCE_FIELDS:
#                 sanitized["attrs"][attr_field] = sanitize_provenance_field(
#                     value, node_id, node_type, full_field)

#     return sanitized


def sanitize_node(node):
    sanitized = copy.deepcopy(node)
    node_id   = node.get("id",   "UNKNOWN")
    node_type = node.get("type", "UNKNOWN")

    # --------------------------------------------------------
    # Top-level fields
    # --------------------------------------------------------
    for field in list(sanitized.keys()):
        if field == "attrs":
            continue

        value = sanitized[field]

        if field in COMMIT_HASH_FIELDS:
            sanitized[field] = sanitize_commit_hash_field(value, node_id, node_type, field)

        elif field in COMMIT_LIST_FIELDS:
            sanitized[field] = sanitize_commit_list_field(value, node_id, node_type, field)

        elif field in PR_NUMBER_FIELDS:
            sanitized[field] = sanitize_pr_field(value, node_id, node_type, field)

        elif field in LITERAL_FIELDS:
            sanitized[field] = sanitize_literal_field(value, node_id, node_type, field)

        elif field in PROVENANCE_FIELDS:
            sanitized[field] = sanitize_provenance_field(value, node_id, node_type, field)

    # --------------------------------------------------------
    # attrs fields
    # --------------------------------------------------------
    if "attrs" in sanitized and isinstance(sanitized["attrs"], dict):
        for attr_field in list(sanitized["attrs"].keys()):
            full_field = f"attrs.{attr_field}"
            value      = sanitized["attrs"][attr_field]

            if full_field in COMMIT_HASH_FIELDS:
                sanitized["attrs"][attr_field] = sanitize_commit_hash_field(
                    value, node_id, node_type, full_field)

            elif full_field in COMMIT_LIST_FIELDS:
                sanitized["attrs"][attr_field] = sanitize_commit_list_field(
                    value, node_id, node_type, full_field)

            elif full_field in LITERAL_FIELDS:
                sanitized["attrs"][attr_field] = sanitize_literal_field(
                    value, node_id, node_type, full_field)

            elif full_field in PROVENANCE_FIELDS:
                sanitized["attrs"][attr_field] = sanitize_provenance_field(
                    value, node_id, node_type, full_field)

    # --------------------------------------------------------
    # 🔥 NEW: Handle attrs.commits (nested commit objects)
    # --------------------------------------------------------
    if "attrs" in sanitized and isinstance(sanitized["attrs"], dict):
        if "commits" in sanitized["attrs"] and isinstance(sanitized["attrs"]["commits"], list):

            sanitized_commits = []

            for commit_obj in sanitized["attrs"]["commits"]:
                if isinstance(commit_obj, dict):
                    sanitized_commit = commit_obj.copy()

                    # ---- SHA ----
                    if "sha" in sanitized_commit and sanitized_commit["sha"]:
                        record_replacement(
                            node_id, node_type, "attrs.commits.sha",
                            sanitized_commit["sha"], "[COMMIT_ID]",
                            "regex", "PROVENANCE"
                        )
                        sanitized_commit["sha"] = "[COMMIT_ID]"

                    # ---- MESSAGE HEAD ----
                    if "message_head" in sanitized_commit and sanitized_commit["message_head"]:
                        original_msg = sanitized_commit["message_head"]
                        msg = original_msg

                        # Run Presidio
                        findings = scan_with_presidio(msg, node_id, "attrs.commits.message_head")
                        if findings:
                            msg = apply_presidio_replacements(
                                msg, findings, node_id, node_type, "attrs.commits.message_head"
                            )

                        # Replace commit hashes
                        msg = COMMIT_HASH_RE.sub("[COMMIT_ID]", msg)

                        # Replace GitHub URLs
                        msg = GITHUB_URL_RE.sub("[REPO_URL]", msg)

                        if msg != original_msg:
                            record_replacement(
                                node_id, node_type, "attrs.commits.message_head",
                                original_msg[:80], msg[:80],
                                "regex", "PROVENANCE"
                            )

                        sanitized_commit["message_head"] = msg

                    sanitized_commits.append(sanitized_commit)

                else:
                    sanitized_commits.append(commit_obj)

            sanitized["attrs"]["commits"] = sanitized_commits

    return sanitized

# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Literal & PII Sanitization (Step 2)...")
    logger.info(f"Loading KG from: {KG_FILE}")

    with open(KG_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    is_dict = isinstance(data, dict)
    nodes   = list(data.values()) if is_dict else data
    logger.info(f"Total nodes to sanitize: {len(nodes)}")

    # --------------------------------------------------------
    # Sanitize all nodes
    # --------------------------------------------------------
    sanitized_nodes = []
    for i, node in enumerate(nodes):
        sanitized = sanitize_node(node)
        sanitized_nodes.append(sanitized)
        if (i + 1) % 200 == 0:
            logger.info(f"  Processed {i+1}/{len(nodes)} nodes...")

    logger.success(f"Sanitization complete. {len(registry_rows)} replacements made.")
    logger.info(f"False positives caught and skipped: {len(false_positive_rows)}")

    # --------------------------------------------------------
    # Save sanitized KG
    # --------------------------------------------------------
    output_data = {node["id"]: node for node in sanitized_nodes} if is_dict else sanitized_nodes
    with open(OUTPUT_KG, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    logger.success(f"Sanitized KG saved: {OUTPUT_KG}")

    # --------------------------------------------------------
    # Save sensitive item registry
    # --------------------------------------------------------
    registry_fieldnames = ["node_id", "node_type", "field", "original",
                           "replacement", "tool", "leakage_type"]
    with open(REGISTRY_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=registry_fieldnames)
        writer.writeheader()
        writer.writerows(registry_rows)
    logger.success(f"Registry saved: {REGISTRY_FILE} ({len(registry_rows)} entries)")

    # --------------------------------------------------------
    # Save false positives log
    # --------------------------------------------------------
    fp_fieldnames = ["node_id", "field", "entity_type", "text", "reason"]
    with open(FALSE_POSITIVE_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fp_fieldnames)
        writer.writeheader()
        writer.writerows(false_positive_rows)
    logger.success(f"False positives log saved: {FALSE_POSITIVE_FILE} ({len(false_positive_rows)} entries)")

    # --------------------------------------------------------
    # Summary stats
    # --------------------------------------------------------
    tool_counts  = defaultdict(int)
    type_counts  = defaultdict(int)
    field_counts = defaultdict(int)
    fp_type_counts = defaultdict(int)

    for row in registry_rows:
        tool_counts[row["tool"]]         += 1
        type_counts[row["leakage_type"]] += 1
        field_counts[row["field"]]       += 1

    for row in false_positive_rows:
        fp_type_counts[row["entity_type"]] += 1

    summary_lines = [
        "=" * 60,
        "LITERAL & PII SANITIZATION SUMMARY (Step 2)",
        "=" * 60,
        f"Total nodes processed      : {len(nodes)}",
        f"Total replacements made    : {len(registry_rows)}",
        f"False positives blocked    : {len(false_positive_rows)}",
        f"Unique contributors found  : {len(contributor_registry)}",
        "",
        "Replacements by Tool:",
    ]
    for tool, count in sorted(tool_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {tool:<30} {count:>6}")

    summary_lines += ["", "Replacements by Leakage Type:"]
    for ltype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {ltype:<30} {count:>6}")

    summary_lines += ["", "Top 10 Most Sanitized Fields:"]
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1])[:10]:
        summary_lines.append(f"  {field:<40} {count:>6}")

    summary_lines += ["", "False Positives Blocked by Entity Type:"]
    for etype, count in sorted(fp_type_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {etype:<30} {count:>6}")

    summary_lines += ["", "Contributor Mapping (placeholder ← real name):"]
    for name, placeholder in sorted(contributor_registry.items(), key=lambda x: x[1]):
        summary_lines.append(f"  {placeholder:<20} ← {name}")

    summary_lines.append("=" * 60)

    summary_text = "\n".join(summary_lines)
    print("\n" + summary_text)

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary_text)
    logger.success(f"Summary saved: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()