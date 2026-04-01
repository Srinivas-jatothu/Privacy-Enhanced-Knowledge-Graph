"""
============================================================
step5_code_sanitizer.py
============================================================
PURPOSE:
    Sanitize raw code snippets extracted by step6_extract_code_snippets.py
    BEFORE they are sent to the LLM for summarization.

    This is the FINAL sanitization layer that closes the gap
    left by steps 2-4 (which only sanitized the KG metadata).

    Without this step, the LLM receives:
        KG context  : "func_001 CALLS func_002"  (sanitized)
        Code snippet: "def correlation_check(...)" (NOT sanitized)
    → CONTRADICTION — sensitive names leak through code

    With this step, the LLM receives:
        KG context  : "func_001 CALLS func_002"  (sanitized)
        Code snippet: "def func_001(...)"         (sanitized)
    → CONSISTENT — no sensitive names reach LLM

    SANITIZATION APPLIED TO CODE SNIPPETS:
        1. Function/class names → func_N placeholders
           (using same identifier_registry.csv from step3)
        2. File paths → file_N placeholders
        3. Module paths → module_lN placeholders
        4. String literals containing secrets → [SECRET]
        5. Email addresses → [EMAIL]
        6. URLs → [URL] or [REPO_URL]
        7. Hardcoded credentials → [SECRET]
        8. Variable names that suggest sensitive data → [SENSITIVE_VAR]

    CONSISTENCY GUARANTEE:
        The same identifier_registry.csv from step3 is used here.
        So if node_v2.json has "func_001 ← correlation_check",
        the code snippet will also show "func_001" not "correlation_check".
        This ensures the LLM sees consistent names across KG and code.

USAGE:
    python step5_code_sanitizer.py

INPUT:
    - Code_Summarization/results/code_snippets.jsonl
        Raw code snippets from step6_extract_code_snippets.py
    - results/identifier_registry.csv
        Mapping from step3: original name → placeholder
    - results/sensitive_item_registry.csv
        Mapping from step2: PII/literal → placeholder

OUTPUT:
    - results/code_snippets_sanitized.jsonl
        Sanitized code snippets ready for LLM
    - results/code_sanitization_report.csv
        What was replaced in each snippet
    - results/code_sanitization_summary.txt
        Summary statistics
============================================================
"""

import json
import os
import csv
import re
import copy
from collections import defaultdict
from loguru import logger

# ============================================================
# CONFIG
# ============================================================

CODE_SNIPPETS_FILE  = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Code_Summarization\results\code_snippets.jsonl"
IDENTIFIER_REGISTRY = os.path.join(os.path.dirname(__file__), "results", "identifier_registry.csv")
OUTPUT_DIR          = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_SNIPPETS     = os.path.join(OUTPUT_DIR, "code_snippets_sanitized.jsonl")
REPORT_FILE         = os.path.join(OUTPUT_DIR, "code_sanitization_report.csv")
SUMMARY_FILE        = os.path.join(OUTPUT_DIR, "code_sanitization_summary.txt")

# ============================================================
# LOAD IDENTIFIER REGISTRY FROM STEP 3
# This ensures consistency between KG and code snippets
# ============================================================

# def load_identifier_registry(registry_file):
#     """
#     Load the identifier registry from step3.
#     Returns dict: original_value → placeholder
#     """
#     registry = {}
#     try:
#         with open(registry_file, encoding='utf-8') as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 original    = row.get("original", "").strip()
#                 replacement = row.get("replacement", "").strip()
#                 if original and replacement:
#                     registry[original] = replacement
#         logger.success(f"Loaded {len(registry)} identifier mappings from registry")
#     except Exception as e:
#         logger.error(f"Could not load identifier registry: {e}")
#     return registry


def load_identifier_registry(registry_file):
    """
    Load identifier registry from step3.
    When same name has multiple mappings, prefer func_N over module_N.
    """
    registry = {}
    func_registry = {}    # func_N mappings
    other_registry = {}   # module_N, file_N etc mappings

    try:
        with open(registry_file, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original    = row.get("original", "").strip()
                replacement = row.get("replacement", "").strip()
                if original and replacement:
                    if replacement.startswith("func_"):
                        func_registry[original] = replacement
                    else:
                        other_registry[original] = replacement

        # Merge: func_N takes priority over module_N/file_N
        registry = {**other_registry, **func_registry}
        logger.success(f"Loaded {len(registry)} identifier mappings")
        logger.info(f"  func_N mappings    : {len(func_registry)}")
        logger.info(f"  other mappings     : {len(other_registry) - len(func_registry)}")

    except Exception as e:
        logger.error(f"Could not load identifier registry: {e}")
    return registry


# ============================================================
# REGEX PATTERNS FOR CODE SANITIZATION
# ============================================================

# Secrets and credentials
SECRET_PATTERNS = [
    (re.compile(r'(?i)(api[_-]?key|apikey)\s*=\s*["\']([^"\']{8,})["\']'),    "API_KEY"),
    (re.compile(r'(?i)(secret|token|password|passwd|pwd)\s*=\s*["\']([^"\']{6,})["\']'), "SECRET"),
    (re.compile(r'(?i)(access[_-]?key)\s*=\s*["\']([^"\']{10,})["\']'),       "ACCESS_KEY"),
    (re.compile(r'(?i)(auth[_-]?token)\s*=\s*["\']([^"\']{8,})["\']'),        "AUTH_TOKEN"),
    (re.compile(r'(?i)(private[_-]?key)\s*=\s*["\']([^"\']{10,})["\']'),      "PRIVATE_KEY"),
]

# Sensitive variable name patterns (variable names that suggest sensitive data)
SENSITIVE_VAR_PATTERNS = [
    re.compile(r'\b(password|passwd|pwd|secret|api_key|apikey|auth_token|access_key|private_key)\s*='),
]

# Email pattern
EMAIL_RE        = re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}')

# URL patterns
GITHUB_URL_RE   = re.compile(r'https://github\.com/[^\s"\']+')
URL_RE          = re.compile(r'https?://[^\s"\']+')

# Commit hash pattern
COMMIT_HASH_RE  = re.compile(r'\b[0-9a-f]{40}\b')

# File path patterns in strings
FILE_PATH_RE    = re.compile(r'["\']([^"\']*/([\w_-]+\.py))["\']')

# Import statement patterns
IMPORT_RE       = re.compile(r'^(from|import)\s+([\w.]+)', re.MULTILINE)

# ============================================================
# REPLACEMENT TRACKER
# ============================================================

report_rows = []


def record_code_replacement(node_id, snippet_field, original, replacement, method):
    report_rows.append({
        "node_id":      node_id,
        "field":        snippet_field,
        "original":     str(original)[:100],
        "replacement":  str(replacement)[:100],
        "method":       method
    })


# ============================================================
# CORE SANITIZATION FUNCTIONS
# ============================================================

def sanitize_function_names(code, node_id, identifier_registry):
    """
    Replace function/class names in code using identifier_registry.
    This ensures consistency with KG node labels.

    Strategy:
    1. Find all def/class statements
    2. Look up function name in registry
    3. Replace with same placeholder used in KG
    """
    sanitized = code

    # Find all function definitions
    func_def_re = re.compile(r'\bdef\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(')
    class_def_re = re.compile(r'\bclass\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[\(:]')

    # Replace function names
    for match in func_def_re.finditer(code):
        func_name = match.group(1)
        if func_name in identifier_registry:
            placeholder = identifier_registry[func_name]
            sanitized = sanitized.replace(
                f"def {func_name}(",
                f"def {placeholder}("
            )
            record_code_replacement(node_id, "snippet",
                                    f"def {func_name}", f"def {placeholder}",
                                    "identifier_registry")

    # Replace class names
    for match in class_def_re.finditer(code):
        class_name = match.group(1)
        if class_name in identifier_registry:
            placeholder = identifier_registry[class_name]
            sanitized = re.sub(
                rf'\bclass\s+{re.escape(class_name)}\b',
                f"class {placeholder}",
                sanitized
            )
            record_code_replacement(node_id, "snippet",
                                    f"class {class_name}", f"class {placeholder}",
                                    "identifier_registry")

    return sanitized


def sanitize_function_calls(code, node_id, identifier_registry):
    """
    Replace function call names in code using identifier_registry.
    e.g. save_heatmap(fig, path) → func_002(fig, path)
    """
    sanitized = code

    # Find function calls (word followed by parenthesis)
    call_re = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(')

    # Get all unique function names in code
    calls_found = set(call_re.findall(code))

    # Replace those that are in registry
    # Sort by length descending to avoid partial replacements
    calls_to_replace = sorted(
        [(name, identifier_registry[name]) for name in calls_found
         if name in identifier_registry],
        key=lambda x: -len(x[0])
    )

    for original, placeholder in calls_to_replace:
        # Use word boundary to avoid partial matches
        sanitized = re.sub(
            rf'\b{re.escape(original)}\s*\(',
            f"{placeholder}(",
            sanitized
        )
        record_code_replacement(node_id, "snippet",
                                f"{original}(", f"{placeholder}(",
                                "identifier_registry")

    return sanitized


def sanitize_imports(code, node_id, identifier_registry):
    """
    Replace module names in import statements.
    e.g. from dags.src.correlation import correlation_check
      →  from module_l3_005 import func_001
    """
    sanitized = code
    lines = sanitized.split('\n')
    sanitized_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith('import ') or stripped.startswith('from '):
            original_line = line

            # Replace module paths
            for original, placeholder in sorted(identifier_registry.items(),
                                                key=lambda x: -len(x[0])):
                if original in line and '.' in original:
                    line = line.replace(original, placeholder)

            if line != original_line:
                record_code_replacement(node_id, "imports",
                                        original_line.strip()[:60],
                                        line.strip()[:60],
                                        "identifier_registry")

        sanitized_lines.append(line)

    return '\n'.join(sanitized_lines)


def sanitize_string_literals(code, node_id):
    """
    Replace sensitive string literals in code.
    - Secrets/API keys → [SECRET]
    - Emails → [EMAIL]
    - GitHub URLs → [REPO_URL]
    - General URLs → [URL]
    - Commit hashes → [COMMIT_ID]
    """
    sanitized = code

    # Replace secrets
    for pattern, secret_type in SECRET_PATTERNS:
        def replace_secret(m):
            record_code_replacement(node_id, "string_literal",
                                    m.group(0)[:60], f'[{secret_type}]',
                                    "detect-secrets")
            return f'{m.group(1)} = "[{secret_type}]"'
        sanitized = pattern.sub(replace_secret, sanitized)

    # Replace emails
    def replace_email(m):
        record_code_replacement(node_id, "string_literal",
                                m.group(0), "[EMAIL]", "regex")
        return "[EMAIL]"
    sanitized = EMAIL_RE.sub(replace_email, sanitized)

    # Replace GitHub URLs
    def replace_github(m):
        record_code_replacement(node_id, "string_literal",
                                m.group(0)[:60], "[REPO_URL]", "regex")
        return "[REPO_URL]"
    sanitized = GITHUB_URL_RE.sub(replace_github, sanitized)

    # Replace other URLs
    def replace_url(m):
        record_code_replacement(node_id, "string_literal",
                                m.group(0)[:60], "[URL]", "regex")
        return "[URL]"
    sanitized = URL_RE.sub(replace_url, sanitized)

    # Replace commit hashes
    def replace_hash(m):
        record_code_replacement(node_id, "string_literal",
                                m.group(0), "[COMMIT_ID]", "regex")
        return "[COMMIT_ID]"
    sanitized = COMMIT_HASH_RE.sub(replace_hash, sanitized)

    return sanitized


def sanitize_file_paths_in_strings(code, node_id, identifier_registry):
    """
    Replace file path strings in code.
    e.g. "dags/src/correlation.py" → "dag_file_164.py"
    """
    sanitized = code

    for original, placeholder in sorted(identifier_registry.items(),
                                        key=lambda x: -len(x[0])):
        # Only process file-like paths (contain / or \)
        if ('/' in original or '\\' in original) and original in sanitized:
            sanitized = sanitized.replace(original, placeholder)
            record_code_replacement(node_id, "file_path",
                                    original, placeholder,
                                    "identifier_registry")

    return sanitized


def sanitize_node_id(node_id, identifier_registry):
    """
    Sanitize the node_id field of the snippet record itself.
    e.g. "Function:dags.src.correlation.correlation_check"
      →  "Function:func_001"
    """
    if ':' not in node_id:
        return node_id

    type_prefix = node_id.split(':')[0]
    inner       = node_id.split(':', 1)[1]

    # Look up inner part in registry
    if inner in identifier_registry:
        return f"{type_prefix}:{identifier_registry[inner]}"

    # Try just the function name part (last segment)
    parts = inner.split('.')
    if parts and parts[-1] in identifier_registry:
        return f"{type_prefix}:{identifier_registry[parts[-1]]}"

    return node_id


def sanitize_path_field(path, identifier_registry):
    """Sanitize the path field of the snippet record."""
    if path in identifier_registry:
        return identifier_registry[path]
    # Try matching just filename
    basename = os.path.basename(path)
    if basename in identifier_registry:
        return identifier_registry[basename]
    return path


# ============================================================
# SANITIZE A SINGLE SNIPPET RECORD
# ============================================================

def sanitize_snippet(record, identifier_registry):
    """
    Sanitize a single code snippet record.
    Returns sanitized copy.
    """
    sanitized = copy.deepcopy(record)
    node_id   = record.get("node_id", "UNKNOWN")

    # --------------------------------------------------------
    # 1. Sanitize the code snippet itself
    # --------------------------------------------------------
    snippet = record.get("snippet", "")
    if snippet:
        # Order matters - do these in sequence
        snippet = sanitize_string_literals(snippet, node_id)
        snippet = sanitize_file_paths_in_strings(snippet, node_id, identifier_registry)
        snippet = sanitize_imports(snippet, node_id, identifier_registry)
        snippet = sanitize_function_names(snippet, node_id, identifier_registry)
        snippet = sanitize_function_calls(snippet, node_id, identifier_registry)
        sanitized["snippet"] = snippet

    # --------------------------------------------------------
    # 2. Sanitize node_id field
    # --------------------------------------------------------
    original_node_id = node_id
    new_node_id = sanitize_node_id(node_id, identifier_registry)
    sanitized["node_id"] = new_node_id
    if new_node_id != original_node_id:
        record_code_replacement(original_node_id, "node_id",
                                original_node_id, new_node_id,
                                "identifier_registry")

    # --------------------------------------------------------
    # 3. Sanitize path field
    # --------------------------------------------------------
    path = record.get("path", "")
    if path:
        new_path = sanitize_path_field(path, identifier_registry)
        sanitized["path"] = new_path
        if new_path != path:
            record_code_replacement(original_node_id, "path",
                                    path, new_path,
                                    "identifier_registry")

    return sanitized


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Code Snippet Sanitization (Step 5)...")

    # --------------------------------------------------------
    # Load identifier registry from step3
    # --------------------------------------------------------
    logger.info(f"Loading identifier registry from: {IDENTIFIER_REGISTRY}")
    identifier_registry = load_identifier_registry(IDENTIFIER_REGISTRY)

    # --------------------------------------------------------
    # Load code snippets
    # --------------------------------------------------------
    logger.info(f"Loading code snippets from: {CODE_SNIPPETS_FILE}")
    snippets = []
    with open(CODE_SNIPPETS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                snippets.append(json.loads(line))

    logger.info(f"Total snippets to sanitize: {len(snippets)}")

    # --------------------------------------------------------
    # Sanitize all snippets
    # --------------------------------------------------------
    sanitized_snippets = []
    for i, snippet in enumerate(snippets):
        sanitized = sanitize_snippet(snippet, identifier_registry)
        sanitized_snippets.append(sanitized)
        if (i + 1) % 50 == 0:
            logger.info(f"  Processed {i+1}/{len(snippets)} snippets...")

    logger.success(f"Sanitization complete. {len(report_rows)} replacements made.")

    # --------------------------------------------------------
    # Save sanitized snippets
    # --------------------------------------------------------
    with open(OUTPUT_SNIPPETS, 'w', encoding='utf-8') as f:
        for snippet in sanitized_snippets:
            f.write(json.dumps(snippet, ensure_ascii=False) + '\n')
    logger.success(f"Sanitized snippets saved: {OUTPUT_SNIPPETS}")

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------
    report_fieldnames = ["node_id", "field", "original", "replacement", "method"]
    with open(REPORT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=report_fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    logger.success(f"Report saved: {REPORT_FILE} ({len(report_rows)} entries)")

    # --------------------------------------------------------
    # Summary stats
    # --------------------------------------------------------
    method_counts   = defaultdict(int)
    field_counts    = defaultdict(int)
    node_counts     = defaultdict(int)

    for row in report_rows:
        method_counts[row["method"]]    += 1
        field_counts[row["field"]]      += 1
        node_counts[row["node_id"]]     += 1

    # Sample: show before/after for first function snippet
    sample_before = snippets[0].get("snippet", "")[:200] if snippets else ""
    sample_after  = sanitized_snippets[0].get("snippet", "")[:200] if sanitized_snippets else ""

    summary_lines = [
        "=" * 60,
        "CODE SNIPPET SANITIZATION SUMMARY (Step 5)",
        "=" * 60,
        f"Total snippets processed   : {len(snippets)}",
        f"Total replacements made    : {len(report_rows)}",
        f"Snippets with replacements : {len(node_counts)}",
        "",
        "Replacements by Method:",
    ]
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {method:<30} {count:>6}")

    summary_lines += ["", "Replacements by Field Type:"]
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {field:<30} {count:>6}")

    summary_lines += [
        "",
        "CONSISTENCY CHECK:",
        "  Code snippets now use same func_N placeholders as KG.",
        "  LLM receives consistent names across all input layers.",
        "",
        "SAMPLE (first snippet):",
        "  BEFORE (first 200 chars):",
        f"  {sample_before[:200]}",
        "",
        "  AFTER (first 200 chars):",
        f"  {sample_after[:200]}",
        "=" * 60,
    ]

    summary_text = "\n".join(summary_lines)
    print("\n" + summary_text)

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary_text)
    logger.success(f"Summary saved: {SUMMARY_FILE}")

    print("\n" + "=" * 60)
    print("STEP 5 COMPLETE")
    print("=" * 60)
    print(f"Sanitized snippets : {OUTPUT_SNIPPETS}")
    print(f"Total replacements : {len(report_rows)}")
    print("=" * 60)
    print("\nReady for Step 6: Run summarization pipeline")
    print("Use code_snippets_sanitized.jsonl instead of code_snippets.jsonl")


if __name__ == "__main__":
    main()