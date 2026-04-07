"""
============================================================
step1_copy_repo.py
============================================================
PURPOSE:
    Create a clean copy of the original Ecommerce-Data-MLOps
    repository that will be sanitized in subsequent steps.

    The original repo is NEVER modified.
    All sanitization happens on the copy.

USAGE:
    python step1_copy_repo.py

INPUT:
    - Ecommerce-Data-MLOps/  (original repo)
      Path defined in config.py → ORIGINAL_REPO

OUTPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (clean copy)
      Path defined in config.py → SANITIZED_REPO
    - results/repo_copy_report.txt
      Summary of what was copied
============================================================
"""

import os
import shutil
from loguru import logger
from config import (
    ORIGINAL_REPO,
    SANITIZED_REPO,
    RESULTS_DIR,
    SKIP_FOLDERS,
    SCAN_EXTENSIONS
)

# ============================================================
# OUTPUT
# ============================================================

COPY_REPORT = os.path.join(RESULTS_DIR, "repo_copy_report.txt")


# ============================================================
# COPY FUNCTION
# ============================================================

def copy_repo(src, dst):
    """
    Copy source repo to destination.
    Skips venv, __pycache__, .git, .dvc etc.
    Returns stats dict.
    """
    stats = {
        "files_copied":   0,
        "files_skipped":  0,
        "dirs_created":   0,
        "py_files":       0,
        "total_size_kb":  0,
        "skipped_folders": [],
        "copied_py_files": []
    }

    for root, dirs, files in os.walk(src):

        # Skip unwanted folders in-place
        dirs[:] = [
            d for d in dirs
            if d not in SKIP_FOLDERS
        ]

        # Compute relative path
        rel_path = os.path.relpath(root, src)
        dst_dir  = os.path.join(dst, rel_path)

        # Create destination directory
        os.makedirs(dst_dir, exist_ok=True)
        stats["dirs_created"] += 1

        for file in files:
            src_file = os.path.join(root, file)
            dst_file = os.path.join(dst_dir, file)

            # Track Python files separately
            _, ext = os.path.splitext(file)

            try:
                shutil.copy2(src_file, dst_file)
                stats["files_copied"] += 1
                stats["total_size_kb"] += os.path.getsize(src_file) / 1024

                if ext in SCAN_EXTENSIONS:
                    stats["py_files"] += 1
                    stats["copied_py_files"].append(
                        os.path.join(rel_path, file)
                    )

            except Exception as e:
                logger.warning(f"Could not copy {src_file}: {e}")
                stats["files_skipped"] += 1

    return stats


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Repo Copy (Step 1)...")
    logger.info(f"Source : {ORIGINAL_REPO}")
    logger.info(f"Dest   : {SANITIZED_REPO}")

    # --------------------------------------------------------
    # Check source exists
    # --------------------------------------------------------
    if not os.path.exists(ORIGINAL_REPO):
        logger.error(f"Original repo not found: {ORIGINAL_REPO}")
        return

    # --------------------------------------------------------
    # Remove existing sanitized copy if exists
    # --------------------------------------------------------
    if os.path.exists(SANITIZED_REPO):
        logger.warning(f"Sanitized repo already exists. Removing...")
        shutil.rmtree(SANITIZED_REPO)
        logger.info("Removed existing sanitized repo.")

    # --------------------------------------------------------
    # Copy repo
    # --------------------------------------------------------
    logger.info("Copying repo...")
    stats = copy_repo(ORIGINAL_REPO, SANITIZED_REPO)
    logger.success(f"Repo copied successfully!")

    # --------------------------------------------------------
    # Print summary
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("REPO COPY SUMMARY (Step 1)")
    print("=" * 60)
    print(f"Source            : {ORIGINAL_REPO}")
    print(f"Destination       : {SANITIZED_REPO}")
    print(f"Directories created: {stats['dirs_created']}")
    print(f"Total files copied : {stats['files_copied']}")
    print(f"Python files       : {stats['py_files']}")
    print(f"Files skipped      : {stats['files_skipped']}")
    print(f"Total size         : {stats['total_size_kb']:.1f} KB")
    print()
    print("Python files copied:")
    for f in sorted(stats["copied_py_files"]):
        print(f"  {f}")
    print("=" * 60)
    print(f"\nSanitized repo ready at:")
    print(f"  {SANITIZED_REPO}")
    print("\nNext step: python step2_build_identifier_map.py")

    # --------------------------------------------------------
    # Save report
    # --------------------------------------------------------
    report_lines = [
        "REPO COPY REPORT - Method 3",
        "=" * 60,
        f"Source      : {ORIGINAL_REPO}",
        f"Destination : {SANITIZED_REPO}",
        f"Dirs created: {stats['dirs_created']}",
        f"Files copied: {stats['files_copied']}",
        f"Python files: {stats['py_files']}",
        f"Size (KB)   : {stats['total_size_kb']:.1f}",
        "",
        "Python files:",
    ] + [f"  {f}" for f in sorted(stats["copied_py_files"])]

    with open(COPY_REPORT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    logger.success(f"Report saved: {COPY_REPORT}")


if __name__ == "__main__":
    main()