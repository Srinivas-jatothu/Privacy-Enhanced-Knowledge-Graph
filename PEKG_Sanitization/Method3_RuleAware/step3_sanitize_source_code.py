"""
============================================================
step3_sanitize_source_code.py
============================================================
PURPOSE:
    Apply all identifier replacements from step2 to the
    actual Python files in Ecommerce-Data-MLOps-Sanitized/.

    For every .py file:
        1. Replace function definitions (def func_name →)
        2. Replace function calls (func_name(...))
        3. Replace class definitions
        4. Replace import statements
        5. Replace parameter names
        6. Replace string literals (secrets, URLs, emails)
        7. Replace comments containing sensitive info
        8. Replace docstring content

    IMPORTANT: Uses regex-based text replacement (not AST
    rewriting) because AST rewriting loses comments and
    formatting. Regex preserves the original code style.

    CONSISTENCY GUARANTEE:
        Same function name → same placeholder in ALL files.
        If handle_anomalous_codes → data_cleaning_func_001:
          ✅ def data_cleaning_func_001(...) in its file
          ✅ data_cleaning_func_001(...) in all callers
          ✅ from module import data_cleaning_func_001

USAGE:
    python step3_sanitize_source_code.py

INPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (copy from step1)
    - results/identifier_map.json      (from step2)

OUTPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (files modified in place)
    - results/sanitization_report.csv  (what changed in each file)
    - results/step3_summary.txt        (overall summary)
============================================================
"""

import os
import re
import json
import csv
from collections import defaultdict
from loguru import logger
from config import (
    SANITIZED_REPO,
    RESULTS_DIR,
    IDENTIFIER_MAP_FILE,
    SANITIZATION_REPORT,
    SCAN_EXTENSIONS,
    SKIP_FOLDERS,
    SKIP_FILES,
    KNOWN_CONTRIBUTORS,
)

# ============================================================
# OUTPUT
# ============================================================

STEP3_SUMMARY = os.path.join(RESULTS_DIR, "step3_summary.txt")

# ============================================================
# LOAD IDENTIFIER MAP FROM STEP 2
# ============================================================

def load_identifier_map(map_file):
    """Load the identifier map built in step 2."""
    with open(map_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


# ============================================================
# SECRET/PII PATTERNS
# These handle string literals inside code
# ============================================================

SECRET_PATTERNS = [
    # API keys / tokens
    (re.compile(r'(?i)(api[_-]?key|apikey)\s*=\s*["\']([^"\']{8,})["\']'),
     lambda m: f'{m.group(1)} = "[SECRET]"'),

    # Passwords
    (re.compile(r'(?i)(password|passwd|pwd|secret|token)\s*=\s*["\']([^"\']{4,})["\']'),
     lambda m: f'{m.group(1)} = "[SECRET]"'),

    # Access keys
    (re.compile(r'(?i)(access[_-]?key|auth[_-]?token)\s*=\s*["\']([^"\']{8,})["\']'),
     lambda m: f'{m.group(1)} = "[SECRET]"'),
]

# Email pattern
EMAIL_RE = re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}')

# GitHub URL pattern
GITHUB_URL_RE = re.compile(r'https://github\.com/[^\s"\')\]]+')

# General URL pattern
URL_RE = re.compile(r'https?://[^\s"\')\]]+')

# Commit hash pattern (40 hex chars)
COMMIT_HASH_RE = re.compile(r'\b[0-9a-f]{40}\b')


# ============================================================
# REPORT TRACKING
# ============================================================

report_rows = []
file_stats  = defaultdict(lambda: defaultdict(int))


def record(file_path, change_type, original, replacement):
    report_rows.append({
        "file":         file_path,
        "change_type":  change_type,
        "original":     str(original)[:80],
        "replacement":  str(replacement)[:80],
    })
    file_stats[file_path][change_type] += 1


# ============================================================
# CORE SANITIZATION FUNCTIONS
# ============================================================

def replace_function_definitions(content, func_map, rel_path):
    """
    Replace function names in def statements.
    e.g. def handle_anomalous_codes( → def data_cleaning_func_001(
    """
    # Sort by length descending to avoid partial replacements
    sorted_funcs = sorted(func_map.items(), key=lambda x: -len(x[0]))

    for original, placeholder in sorted_funcs:
        pattern = re.compile(
            rf'\bdef\s+{re.escape(original)}\s*\('
        )
        new_content = pattern.sub(f'def {placeholder}(', content)
        if new_content != content:
            record(rel_path, "function_def", original, placeholder)
            content = new_content

    return content


def replace_function_calls(content, func_map, rel_path):
    """
    Replace function call names throughout code.
    e.g. save_heatmap(fig, path) → data_saver_func_001(fig, path)
    Careful not to replace inside def statements (already done).
    """
    sorted_funcs = sorted(func_map.items(), key=lambda x: -len(x[0]))

    for original, placeholder in sorted_funcs:
        # Match function calls: word followed by (
        # NOT preceded by 'def ' (already handled)
        pattern = re.compile(
            rf'(?<!def\s)\b{re.escape(original)}\s*\('
        )
        new_content = pattern.sub(f'{placeholder}(', content)
        if new_content != content:
            record(rel_path, "function_call", original, placeholder)
            content = new_content

    return content

def replace_standalone_identifiers(content, func_map, rel_path):
    """
    Replace function names that appear as standalone identifiers
    e.g. in import statements: 'from x import cancellation_details'
    """
    sorted_funcs = sorted(func_map.items(), key=lambda x: -len(x[0]))
    for original, placeholder in sorted_funcs:
        pattern = re.compile(rf'\b{re.escape(original)}\b')
        new_content = pattern.sub(placeholder, content)
        if new_content != content:
            record(rel_path, "standalone_identifier", original, placeholder)
            content = new_content
    return content


def replace_class_definitions(content, class_map, rel_path):
    """Replace class names in class statements."""
    sorted_classes = sorted(class_map.items(), key=lambda x: -len(x[0]))

    for original, placeholder in sorted_classes:
        pattern = re.compile(
            rf'\bclass\s+{re.escape(original)}\b'
        )
        new_content = pattern.sub(f'class {placeholder}', content)
        if new_content != content:
            record(rel_path, "class_def", original, placeholder)
            content = new_content

    return content


def replace_imports(content, module_map, func_map, rel_path):
    """
    Replace module paths and function names in import statements.
    e.g. from dags.src.anomaly_code_handler import handle_anomalous_codes
      →  from module_l3_001 import data_cleaning_func_001
    """
    lines = content.split('\n')
    new_lines = []

    for line in lines:
        original_line = line
        stripped = line.strip()

        if stripped.startswith('import ') or \
           stripped.startswith('from '):

            # Replace module paths (longer ones first)
            sorted_modules = sorted(
                module_map.items(), key=lambda x: -len(x[0])
            )
            for mod_orig, mod_placeholder in sorted_modules:
                if mod_orig in line:
                    line = line.replace(mod_orig, mod_placeholder)

            # Replace imported function names
            sorted_funcs = sorted(
                func_map.items(), key=lambda x: -len(x[0])
            )
            for func_orig, func_placeholder in sorted_funcs:
                # Match as whole word in import context
                pattern = re.compile(
                    rf'\b{re.escape(func_orig)}\b'
                )
                line = pattern.sub(func_placeholder, line)

            if line != original_line:
                record(rel_path, "import",
                       original_line.strip()[:60],
                       line.strip()[:60])

        new_lines.append(line)

    return '\n'.join(new_lines)


def replace_parameters(content, param_map, rel_path):
    """
    Replace parameter names in function signatures and body.
    Only replaces as whole words to avoid partial matches.
    """
    # Sort by length descending
    sorted_params = sorted(
        param_map.items(), key=lambda x: -len(x[0])
    )

    for original, placeholder in sorted_params:
        # Skip very short param names (risk of false matches)
        if len(original) <= 2:
            continue

        pattern = re.compile(rf'\b{re.escape(original)}\b')
        new_content = pattern.sub(placeholder, content)
        if new_content != content:
            record(rel_path, "parameter", original, placeholder)
            content = new_content

    return content


def replace_secrets_and_literals(content, rel_path):
    """
    Replace sensitive string literals:
    - API keys, passwords, tokens → [SECRET]
    - Emails → [EMAIL]
    - GitHub URLs → [REPO_URL]
    - General URLs → [URL]
    - Commit hashes → [COMMIT_ID]
    """
    # Secrets
    for pattern, replacer in SECRET_PATTERNS:
        new_content = pattern.sub(replacer, content)
        if new_content != content:
            record(rel_path, "secret", "sensitive_literal", "[SECRET]")
            content = new_content

    # Emails
    new_content = EMAIL_RE.sub('[EMAIL]', content)
    if new_content != content:
        record(rel_path, "email", "email_address", "[EMAIL]")
        content = new_content

    # GitHub URLs
    new_content = GITHUB_URL_RE.sub('[REPO_URL]', content)
    if new_content != content:
        record(rel_path, "url", "github_url", "[REPO_URL]")
        content = new_content

    # General URLs
    new_content = URL_RE.sub('[URL]', content)
    if new_content != content:
        record(rel_path, "url", "url", "[URL]")
        content = new_content

    # Commit hashes
    new_content = COMMIT_HASH_RE.sub('[COMMIT_ID]', content)
    if new_content != content:
        record(rel_path, "commit_hash", "commit_hash", "[COMMIT_ID]")
        content = new_content

    return content


def replace_comments_and_docstrings(content, rel_path):
    """
    Replace contributor names in comments and docstrings.
    """
    new_content = content
    for name in KNOWN_CONTRIBUTORS:
        pattern = re.compile(
            rf'\b{re.escape(name)}\b', re.IGNORECASE
        )
        result = pattern.sub('[CONTRIBUTOR]', new_content)
        if result != new_content:
            record(rel_path, "contributor_name", name, "[CONTRIBUTOR]")
            new_content = result

    return new_content


# ============================================================
# SANITIZE A SINGLE FILE
# ============================================================

def sanitize_file(file_path, rel_path, identifier_map):
    """
    Sanitize a single Python file.
    Applies all replacements in correct order.
    Returns (sanitized_content, change_count)
    """
    func_map    = identifier_map.get("functions",  {})
    class_map   = identifier_map.get("classes",    {})
    param_map   = identifier_map.get("parameters", {})
    module_map  = identifier_map.get("modules",    {})

    try:
        with open(file_path, 'r', encoding='utf-8',
                  errors='ignore') as f:
            content = f.read()
    except Exception as e:
        logger.warning(f"Could not read {rel_path}: {e}")
        return None, 0

    original_content = content
    before_count = len(report_rows)

    # Apply replacements in order
    # 1. Imports first (handles module paths)
    content = replace_imports(content, module_map, func_map, rel_path)

    # 2. Function definitions
    content = replace_function_definitions(content, func_map, rel_path)

    # 3. Class definitions
    content = replace_class_definitions(content, class_map, rel_path)

    # 4. Function calls (after defs to avoid double replacement)
    content = replace_function_calls(content, func_map, rel_path)

    content = replace_standalone_identifiers(content, func_map, rel_path)

    # 5. Parameters
    content = replace_parameters(content, param_map, rel_path)

    # 6. String literals (secrets, URLs, emails)
    content = replace_secrets_and_literals(content, rel_path)

    # 7. Comments and docstrings (contributor names)
    content = replace_comments_and_docstrings(content, rel_path)

    change_count = len(report_rows) - before_count
    return content, change_count


# ============================================================
# VALIDATE FILE STILL PARSES
# ============================================================

def validate_python_syntax(content, rel_path):
    """Check if sanitized content is still valid Python."""
    import ast
    try:
        ast.parse(content)
        return True
    except SyntaxError as e:
        logger.warning(f"Syntax error after sanitization in "
                      f"{rel_path}: {e}")
        return False


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Source Code Sanitization (Step 3)...")
    logger.info(f"Repo: {SANITIZED_REPO}")

    # --------------------------------------------------------
    # Load identifier map from step 2
    # --------------------------------------------------------
    logger.info(f"Loading identifier map from: {IDENTIFIER_MAP_FILE}")
    identifier_map = load_identifier_map(IDENTIFIER_MAP_FILE)
    logger.success(
        f"Loaded: {len(identifier_map['functions'])} functions, "
        f"{len(identifier_map['modules'])} modules, "
        f"{len(identifier_map['parameters'])} params"
    )

    # --------------------------------------------------------
    # Process all Python files
    # --------------------------------------------------------
    files_processed = 0
    files_changed   = 0
    files_failed    = 0
    syntax_errors   = 0
    total_changes   = 0

    for root, dirs, files in os.walk(SANITIZED_REPO):
        dirs[:] = [d for d in dirs if d not in SKIP_FOLDERS]

        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext not in SCAN_EXTENSIONS:
                continue
            if filename in SKIP_FILES:
                continue

            file_path = os.path.join(root, filename)
            rel_path  = os.path.relpath(file_path, SANITIZED_REPO)

            # Sanitize
            sanitized_content, change_count = sanitize_file(
                file_path, rel_path, identifier_map
            )

            if sanitized_content is None:
                files_failed += 1
                continue

            files_processed += 1

            if change_count > 0:
                # Validate syntax before writing
                is_valid = validate_python_syntax(
                    sanitized_content, rel_path
                )

                if not is_valid:
                    syntax_errors += 1
                    logger.warning(
                        f"Skipping {rel_path} - syntax error "
                        f"after sanitization"
                    )
                    continue

                # Write sanitized content back
                try:
                    with open(file_path, 'w',
                              encoding='utf-8') as f:
                        f.write(sanitized_content)
                    files_changed += 1
                    total_changes += change_count
                    logger.info(
                        f"  Sanitized: {rel_path} "
                        f"({change_count} changes)"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not write {rel_path}: {e}"
                    )
                    files_failed += 1
            else:
                logger.info(f"  No changes: {rel_path}")

    logger.success(
        f"Done. {files_changed}/{files_processed} files changed."
    )

    # --------------------------------------------------------
    # Save sanitization report
    # --------------------------------------------------------
    report_fieldnames = ["file", "change_type",
                         "original", "replacement"]
    with open(SANITIZATION_REPORT, 'w', newline='',
              encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=report_fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    logger.success(
        f"Report saved: {SANITIZATION_REPORT} "
        f"({len(report_rows)} entries)"
    )

    # --------------------------------------------------------
    # Summary stats
    # --------------------------------------------------------
    change_type_counts = defaultdict(int)
    for row in report_rows:
        change_type_counts[row["change_type"]] += 1

    print("\n" + "=" * 60)
    print("SOURCE CODE SANITIZATION SUMMARY (Step 3)")
    print("=" * 60)
    print(f"Files processed    : {files_processed}")
    print(f"Files changed      : {files_changed}")
    print(f"Files unchanged    : {files_processed - files_changed}")
    print(f"Files failed       : {files_failed}")
    print(f"Syntax errors      : {syntax_errors}")
    print(f"Total replacements : {len(report_rows)}")
    print()
    print("Replacements by Type:")
    for ctype, count in sorted(
        change_type_counts.items(), key=lambda x: -x[1]
    ):
        print(f"  {ctype:<25} {count:>6}")
    print("=" * 60)
    print("\nNext step: python step4_validate_sanitized_repo.py")

    # --------------------------------------------------------
    # Save summary
    # --------------------------------------------------------
    summary_lines = [
        "SOURCE CODE SANITIZATION SUMMARY - Method 3",
        "=" * 60,
        f"Files processed    : {files_processed}",
        f"Files changed      : {files_changed}",
        f"Files failed       : {files_failed}",
        f"Syntax errors      : {syntax_errors}",
        f"Total replacements : {len(report_rows)}",
        "",
        "Replacements by Type:",
    ]
    for ctype, count in sorted(
        change_type_counts.items(), key=lambda x: -x[1]
    ):
        summary_lines.append(f"  {ctype:<25} {count:>6}")

    with open(STEP3_SUMMARY, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    logger.success(f"Summary saved: {STEP3_SUMMARY}")


if __name__ == "__main__":
    main()