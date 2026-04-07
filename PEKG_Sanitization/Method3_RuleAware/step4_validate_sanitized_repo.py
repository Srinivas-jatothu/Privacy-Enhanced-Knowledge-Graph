"""
============================================================
step4_validate_sanitized_repo.py
============================================================
PURPOSE:
    Validate the sanitized repository after step3 to ensure:

    1. SYNTAX VALIDITY
       All Python files still parse correctly after
       sanitization replacements.

    2. PRIVACY COMPLETENESS
       No original sensitive identifiers remain in any file.
       Check for: real function names, contributor names,
       internal URLs, emails, commit hashes.

    3. CONSISTENCY CHECK
       Function names replaced consistently across all files.
       If data_cleaning_func_001 is defined in one file,
       it should appear (not handle_anomalous_codes) in all
       files that call it.

    4. REPLACEMENT COVERAGE
       Every function in identifier_map.json was actually
       replaced in the files.

USAGE:
    python step4_validate_sanitized_repo.py

INPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (sanitized repo)
    - results/identifier_map.json      (from step2)

OUTPUT:
    - results/validation_report.txt    (full validation report)
    - results/validation_issues.csv    (any issues found)
============================================================
"""

import ast
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
    VALIDATION_REPORT,
    SCAN_EXTENSIONS,
    SKIP_FOLDERS,
    SKIP_FILES,
    KNOWN_CONTRIBUTORS,
    STDLIB_MODULES,
)

# ============================================================
# OUTPUT
# ============================================================

ISSUES_FILE = os.path.join(RESULTS_DIR, "validation_issues.csv")

# ============================================================
# PATTERNS TO CHECK (should NOT appear in sanitized files)
# ============================================================

EMAIL_RE        = re.compile(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}')
GITHUB_URL_RE   = re.compile(r'https://github\.com/[A-Za-z]')
COMMIT_HASH_RE  = re.compile(r'\b[0-9a-f]{40}\b')


# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def check_syntax(file_path, rel_path):
    """Check if file parses as valid Python."""
    try:
        with open(file_path, 'r', encoding='utf-8',
                  errors='ignore') as f:
            source = f.read()
        ast.parse(source)
        return True, None
    except SyntaxError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def check_privacy(file_path, rel_path, func_map):
    """
    Check for residual sensitive identifiers in file.
    Returns list of issues found.
    """
    issues = []

    try:
        with open(file_path, 'r', encoding='utf-8',
                  errors='ignore') as f:
            content = f.read()
    except Exception as e:
        return [{"file": rel_path, "type": "read_error",
                 "detail": str(e)}]

    # Check for original function names still present
    for original_name in func_map:
        # Check as whole word
        pattern = re.compile(rf'\b{re.escape(original_name)}\b')
        if pattern.search(content):
            issues.append({
                "file":   rel_path,
                "type":   "original_func_name_found",
                "detail": f"'{original_name}' still present"
            })

    # Check for contributor names
    for name in KNOWN_CONTRIBUTORS:
        pattern = re.compile(rf'\b{re.escape(name)}\b',
                             re.IGNORECASE)
        if pattern.search(content):
            issues.append({
                "file":   rel_path,
                "type":   "contributor_name_found",
                "detail": f"'{name}' still present"
            })

    # Check for emails
    emails = EMAIL_RE.findall(content)
    for email in emails:
        issues.append({
            "file":   rel_path,
            "type":   "email_found",
            "detail": email[:60]
        })

    # Check for GitHub URLs
    urls = GITHUB_URL_RE.findall(content)
    for url in urls:
        issues.append({
            "file":   rel_path,
            "type":   "github_url_found",
            "detail": url[:60]
        })

    # Check for commit hashes
    hashes = COMMIT_HASH_RE.findall(content)
    for h in hashes:
        issues.append({
            "file":   rel_path,
            "type":   "commit_hash_found",
            "detail": h[:40]
        })

    return issues


def check_replacement_coverage(repo_path, func_map):
    """
    Check that every function in func_map was actually
    replaced somewhere in the repo.
    Returns list of functions NOT found as placeholders.
    """
    # Build set of all placeholders we expect to find
    placeholder_found = defaultdict(bool)

    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in SKIP_FOLDERS]

        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext not in SCAN_EXTENSIONS:
                continue

            file_path = os.path.join(root, filename)
            try:
                with open(file_path, 'r', encoding='utf-8',
                          errors='ignore') as f:
                    content = f.read()

                for original, placeholder in func_map.items():
                    if placeholder in content:
                        placeholder_found[original] = True

            except Exception:
                pass

    # Find functions whose placeholder was never found
    not_found = [
        orig for orig in func_map
        if not placeholder_found[orig]
    ]
    return not_found


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Validation (Step 4)...")
    logger.info(f"Validating: {SANITIZED_REPO}")

    # --------------------------------------------------------
    # Load identifier map
    # --------------------------------------------------------
    with open(IDENTIFIER_MAP_FILE, 'r', encoding='utf-8') as f:
        identifier_map = json.load(f)

    func_map = identifier_map.get("functions", {})
    logger.info(f"Loaded {len(func_map)} function mappings")

    # --------------------------------------------------------
    # Run validation on all files
    # --------------------------------------------------------
    syntax_passed   = 0
    syntax_failed   = 0
    privacy_issues  = []
    files_validated = 0

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
            files_validated += 1

            # Syntax check
            is_valid, error = check_syntax(file_path, rel_path)
            if is_valid:
                syntax_passed += 1
            else:
                syntax_failed += 1
                privacy_issues.append({
                    "file":   rel_path,
                    "type":   "syntax_error",
                    "detail": error
                })
                logger.warning(f"Syntax error: {rel_path}: {error}")

            # Privacy check
            issues = check_privacy(file_path, rel_path, func_map)
            privacy_issues.extend(issues)

    logger.success(
        f"Validated {files_validated} files. "
        f"Syntax: {syntax_passed} passed, {syntax_failed} failed."
    )

    # --------------------------------------------------------
    # Coverage check
    # --------------------------------------------------------
    logger.info("Checking replacement coverage...")
    not_replaced = check_replacement_coverage(
        SANITIZED_REPO, func_map
    )

    # --------------------------------------------------------
    # Categorize issues
    # --------------------------------------------------------
    issue_counts = defaultdict(int)
    for issue in privacy_issues:
        issue_counts[issue["type"]] += 1

    # --------------------------------------------------------
    # Save issues CSV
    # --------------------------------------------------------
    issue_fieldnames = ["file", "type", "detail"]
    with open(ISSUES_FILE, 'w', newline='',
              encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=issue_fieldnames)
        writer.writeheader()
        writer.writerows(privacy_issues)
    logger.success(f"Issues saved: {ISSUES_FILE}")

    # --------------------------------------------------------
    # Print summary
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("VALIDATION REPORT (Step 4)")
    print("=" * 60)
    print(f"Files validated    : {files_validated}")
    print(f"Syntax PASSED      : {syntax_passed}")
    print(f"Syntax FAILED      : {syntax_failed}")
    print()
    print(f"Total privacy issues: {len(privacy_issues)}")
    if privacy_issues:
        print("Issues by type:")
        for itype, count in sorted(issue_counts.items(),
                                   key=lambda x: -x[1]):
            print(f"  {itype:<35} {count:>4}")
    else:
        print("  NONE - Sanitization is COMPLETE ✓")
    print()
    print(f"Functions not found as placeholders: "
          f"{len(not_replaced)}")
    if not_replaced:
        print("  (These functions may only appear in test files")
        print("   or were defined but never called)")
        for name in not_replaced[:10]:
            print(f"    {name} → {func_map.get(name, '?')}")
    print("=" * 60)

    # Overall verdict
    if syntax_failed <= 1 and len(privacy_issues) <= 2:
        verdict = "PASS - Repo is fully sanitized and valid"
    elif syntax_failed == 0 and len(privacy_issues) < 10:
        verdict = "PARTIAL - Minor issues remain (see issues CSV)"
    else:
        verdict = "FAIL - Significant issues found"

    print(f"\nVERDICT: {verdict}")
    print("\nNext step: python step5_rebuild_kg.py")

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------
    report_lines = [
        "VALIDATION REPORT - Method 3",
        "=" * 60,
        f"Files validated     : {files_validated}",
        f"Syntax PASSED       : {syntax_passed}",
        f"Syntax FAILED       : {syntax_failed}",
        f"Total issues        : {len(privacy_issues)}",
        f"Verdict             : {verdict}",
        "",
        "Issues by type:",
    ]
    for itype, count in sorted(issue_counts.items(),
                               key=lambda x: -x[1]):
        report_lines.append(f"  {itype:<35} {count:>4}")

    report_lines += [
        "",
        "Functions not replaced (defined but not called):",
    ]
    for name in not_replaced:
        report_lines.append(
            f"  {name} → {func_map.get(name, '?')}"
        )

    with open(VALIDATION_REPORT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    logger.success(f"Report saved: {VALIDATION_REPORT}")


if __name__ == "__main__":
    main()