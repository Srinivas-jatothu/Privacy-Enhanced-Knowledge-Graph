"""
============================================================
step2_build_identifier_map.py
============================================================
PURPOSE:
    Scan ALL Python files in the sanitized repo copy and
    build a role-aware identifier mapping table.

    For every function, class, parameter, and file:
        1. Detect semantic role using keyword analysis
        2. Assign a role-aware placeholder
        3. Record mapping: original → placeholder

    This is the KEY CONTRIBUTION of Method 3.

    ROLE-AWARE vs METHOD 2 (blind):
        Method 2: handle_anomalous_codes → func_001
        Method 3: handle_anomalous_codes → data_cleaning_func_001
                  save_heatmap          → data_saver_func_001
                  correlation_check     → analysis_func_001

    BLOCKLISTS:
        STDLIB_MODULES - standard/third-party libs never replaced
        SKIP_PARAMS    - common param names skipped to avoid
                         false replacements in code body

USAGE:
    python step2_build_identifier_map.py

INPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (copy from step1)

OUTPUT:
    - results/identifier_map.json
    - results/identifier_map.csv
    - results/identifier_map_summary.txt
============================================================
"""

import ast
import os
import json
import csv
import re
from collections import defaultdict
from loguru import logger
from config import (
    SANITIZED_REPO,
    RESULTS_DIR,
    IDENTIFIER_MAP_FILE,
    IDENTIFIER_MAP_CSV,
    ROLE_KEYWORDS,
    FILE_ROLE_KEYWORDS,
    SCAN_EXTENSIONS,
    SKIP_FOLDERS,
    SKIP_FILES,
    FUNC_PLACEHOLDER_FORMAT,
    FILE_PLACEHOLDER_FORMAT,
    MODULE_PLACEHOLDER_FORMAT,
    PARAM_PLACEHOLDER_FORMAT,
    CLASS_PLACEHOLDER_FORMAT,
    STDLIB_MODULES,
    SKIP_PARAMS,
)

SUMMARY_FILE = os.path.join(RESULTS_DIR, "identifier_map_summary.txt")

# ============================================================
# COUNTERS
# ============================================================

func_counters   = defaultdict(int)
class_counters  = defaultdict(int)
file_counters   = defaultdict(int)
param_counter   = [1]
module_counters = defaultdict(int)

# ============================================================
# REGISTRIES
# ============================================================

func_map    = {}
class_map   = {}
param_map   = {}
file_map    = {}
module_map  = {}


# ============================================================
# ROLE DETECTION
# ============================================================

def detect_role(name, keyword_dict):
    """Detect semantic role by keyword matching."""
    if not name:
        return "utility"

    words = re.sub(r'([A-Z])', r'_\1', name).lower()
    words = re.split(r'[_\s]+', words)
    words = [w for w in words if w]

    role_scores = defaultdict(int)
    for word in words:
        for role, keywords in keyword_dict.items():
            if word in keywords:
                role_scores[role] += 1

    if not role_scores:
        return "utility"

    return max(role_scores, key=role_scores.get)


# ============================================================
# PLACEHOLDER GENERATORS
# ============================================================

def get_func_placeholder(func_name):
    """Get role-aware placeholder for a function."""
    if func_name in func_map:
        return func_map[func_name]

    role = detect_role(func_name, ROLE_KEYWORDS)
    func_counters[role] += 1
    placeholder = FUNC_PLACEHOLDER_FORMAT.format(
        role=role, n=func_counters[role]
    )
    func_map[func_name] = placeholder
    return placeholder


def get_class_placeholder(class_name):
    """Get role-aware placeholder for a class."""
    if class_name in class_map:
        return class_map[class_name]

    role = detect_role(class_name, ROLE_KEYWORDS)
    class_counters[role] += 1
    placeholder = CLASS_PLACEHOLDER_FORMAT.format(
        role=role, n=class_counters[role]
    )
    class_map[class_name] = placeholder
    return placeholder


def get_param_placeholder(param_name):
    """
    Get placeholder for a parameter name.
    Skips standard params and common variable names.
    """
    # Skip standard Python params
    if param_name in SKIP_PARAMS:
        return None  # None = skip this param

    if param_name in param_map:
        return param_map[param_name]

    placeholder = PARAM_PLACEHOLDER_FORMAT.format(n=param_counter[0])
    param_counter[0] += 1
    param_map[param_name] = placeholder
    return placeholder


def get_file_placeholder(file_path):
    """Get role-aware placeholder for a file path."""
    if file_path in file_map:
        return file_map[file_path]

    filename    = os.path.basename(file_path)
    name_no_ext = os.path.splitext(filename)[0]
    ext         = os.path.splitext(filename)[1]

    role = detect_role(name_no_ext, FILE_ROLE_KEYWORDS)
    file_counters[role] += 1
    placeholder = FILE_PLACEHOLDER_FORMAT.format(
        role=role, n=file_counters[role]
    ) + ext

    file_map[file_path] = placeholder
    return placeholder


def get_module_placeholder(module_path):
    """
    Get placeholder for a module path.
    Skips stdlib and third-party modules.
    """
    # Skip stdlib and third-party modules
    # Check both full path and root module
    root_module = module_path.split('.')[0]
    if module_path in STDLIB_MODULES or root_module in STDLIB_MODULES:
        return None  # None = skip this module

    if module_path in module_map:
        return module_map[module_path]

    depth = len(module_path.split('.'))
    module_counters[depth] += 1
    placeholder = MODULE_PLACEHOLDER_FORMAT.format(
        depth=depth, n=module_counters[depth]
    )
    module_map[module_path] = placeholder
    return placeholder


# ============================================================
# AST SCANNER
# ============================================================

def scan_python_file(file_path, rel_path):
    """Scan a Python file using AST and extract identifiers."""
    records = []

    try:
        with open(file_path, 'r', encoding='utf-8',
                  errors='ignore') as f:
            source = f.read()
        tree = ast.parse(source, filename=file_path)
    except SyntaxError as e:
        logger.warning(f"Syntax error in {rel_path}: {e}")
        return records
    except Exception as e:
        logger.warning(f"Could not parse {rel_path}: {e}")
        return records

    for node in ast.walk(tree):

        # Function definitions
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name   = node.name
            placeholder = get_func_placeholder(func_name)

            records.append({
                "type":        "function",
                "original":    func_name,
                "placeholder": placeholder,
                "file":        rel_path,
                "line":        node.lineno,
                "role":        detect_role(func_name, ROLE_KEYWORDS)
            })

            # Parameters - only non-skipped ones
            for arg in node.args.args:
                param_name = arg.arg
                param_placeholder = get_param_placeholder(param_name)
                if param_placeholder is None:
                    continue  # skip this param
                records.append({
                    "type":        "parameter",
                    "original":    param_name,
                    "placeholder": param_placeholder,
                    "file":        rel_path,
                    "line":        node.lineno,
                    "role":        "parameter"
                })

        # Class definitions
        elif isinstance(node, ast.ClassDef):
            class_name  = node.name
            placeholder = get_class_placeholder(class_name)
            records.append({
                "type":        "class",
                "original":    class_name,
                "placeholder": placeholder,
                "file":        rel_path,
                "line":        node.lineno,
                "role":        detect_role(class_name, ROLE_KEYWORDS)
            })

        # Import statements - skip stdlib
        elif isinstance(node, ast.Import):
            for alias in node.names:
                module      = alias.name
                placeholder = get_module_placeholder(module)
                if placeholder is None:
                    continue  # skip stdlib
                records.append({
                    "type":        "import",
                    "original":    module,
                    "placeholder": placeholder,
                    "file":        rel_path,
                    "line":        node.lineno,
                    "role":        "module"
                })

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                module      = node.module
                placeholder = get_module_placeholder(module)
                if placeholder is None:
                    continue  # skip stdlib
                records.append({
                    "type":        "import_from",
                    "original":    module,
                    "placeholder": placeholder,
                    "file":        rel_path,
                    "line":        node.lineno,
                    "role":        "module"
                })

    return records


# ============================================================
# SCAN ALL FILES
# ============================================================

def scan_all_files(repo_path):
    """Walk entire repo and scan all Python files."""
    all_records   = []
    files_scanned = 0
    files_skipped = 0

    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in SKIP_FOLDERS]

        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext not in SCAN_EXTENSIONS:
                continue
            if filename in SKIP_FILES:
                files_skipped += 1
                continue

            file_path = os.path.join(root, filename)
            rel_path  = os.path.relpath(file_path, repo_path)

            get_file_placeholder(rel_path)

            records = scan_python_file(file_path, rel_path)
            all_records.extend(records)
            files_scanned += 1

            logger.info(f"  Scanned: {rel_path} "
                       f"({len(records)} identifiers)")

    logger.success(f"Scanned {files_scanned} files, "
                  f"skipped {files_skipped}")
    return all_records


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Identifier Map Build (Step 2)...")
    logger.info(f"Scanning repo: {SANITIZED_REPO}")
    logger.info(f"STDLIB blocklist: {len(STDLIB_MODULES)} modules")
    logger.info(f"Param blocklist : {len(SKIP_PARAMS)} params")

    all_records = scan_all_files(SANITIZED_REPO)
    logger.success(f"Total identifiers found: {len(all_records)}")

    complete_map = {
        "functions":  func_map,
        "classes":    class_map,
        "parameters": param_map,
        "files":      file_map,
        "modules":    module_map,
        "records":    all_records
    }

    # Save JSON
    with open(IDENTIFIER_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(complete_map, f, indent=2, ensure_ascii=False)
    logger.success(f"Map saved: {IDENTIFIER_MAP_FILE}")

    # Save CSV
    csv_fieldnames = ["type", "original", "placeholder",
                      "file", "line", "role"]
    with open(IDENTIFIER_MAP_CSV, 'w', newline='',
              encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
        writer.writeheader()
        writer.writerows(all_records)
    logger.success(f"CSV saved: {IDENTIFIER_MAP_CSV}")

    # Summary
    type_counts = defaultdict(int)
    for rec in all_records:
        type_counts[rec["type"]] += 1

    print("\n" + "=" * 60)
    print("IDENTIFIER MAP SUMMARY (Step 2)")
    print("=" * 60)
    print(f"Total identifiers found : {len(all_records)}")
    print(f"Unique functions        : {len(func_map)}")
    print(f"Unique classes          : {len(class_map)}")
    print(f"Unique parameters       : {len(param_map)}")
    print(f"Unique files            : {len(file_map)}")
    print(f"Unique modules (internal): {len(module_map)}")
    print()
    print("Identifiers by Type:")
    for itype, count in sorted(type_counts.items(),
                                key=lambda x: -x[1]):
        print(f"  {itype:<20} {count:>6}")
    print()
    print("Functions by Role (KEY CONTRIBUTION):")
    for role, count in sorted(func_counters.items(),
                               key=lambda x: -x[1]):
        print(f"  {role:<25} {count:>4} functions")
    print()
    print("Sample Function Mappings:")
    for i, (orig, placeholder) in enumerate(func_map.items()):
        print(f"  {placeholder:<35} ← {orig}")
        if i >= 14:
            break
    print()
    print("Modules replaced (internal only):")
    for orig, placeholder in list(module_map.items())[:10]:
        print(f"  {placeholder:<25} ← {orig}")
    print("=" * 60)
    print("\nNext step: python step3_sanitize_source_code.py")

    # Save summary
    summary_lines = [
        "IDENTIFIER MAP SUMMARY - Method 3",
        "=" * 60,
        f"Total identifiers    : {len(all_records)}",
        f"Unique functions     : {len(func_map)}",
        f"Unique classes       : {len(class_map)}",
        f"Unique parameters    : {len(param_map)}",
        f"Unique files         : {len(file_map)}",
        f"Unique modules       : {len(module_map)}",
        "",
        "Functions by Role:",
    ]
    for role, count in sorted(func_counters.items(),
                               key=lambda x: -x[1]):
        summary_lines.append(f"  {role:<25} {count:>4}")

    summary_lines += ["", "All Function Mappings:"]
    for orig, placeholder in sorted(func_map.items()):
        summary_lines.append(f"  {placeholder:<35} ← {orig}")

    summary_lines += ["", "Internal Module Mappings:"]
    for orig, placeholder in sorted(module_map.items()):
        summary_lines.append(f"  {placeholder:<25} ← {orig}")

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))
    logger.success(f"Summary saved: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()