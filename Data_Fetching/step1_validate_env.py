# step1_validate_env.py
import os
import sys
import shutil
import logging
import importlib

# Load .env if python-dotenv is installed (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
    dotenv_loaded = True
except Exception:
    dotenv_loaded = False

# Configure logging for clear debug/info output
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("env-check")

def check_python_version(min_major=3, min_minor=8):
    pyv = sys.version_info
    ok = (pyv.major > min_major) or (pyv.major == min_major and pyv.minor >= min_minor)
    log.info("Python version: %s.%s.%s", pyv.major, pyv.minor, pyv.micro)
    if not ok:
        log.error("Python >= %s.%s required.", min_major, min_minor)
    return ok

def check_command(cmd):
    path = shutil.which(cmd)
    if path:
        log.info("Found command '%s' at: %s", cmd, path)
        return True
    else:
        log.warning("Command '%s' NOT found in PATH.", cmd)
        return False

def check_package(pkg_name, optional=False):
    try:
        importlib.import_module(pkg_name)
        log.info("Python package '%s' is installed.", pkg_name)
        return True
    except Exception:
        if optional:
            log.warning("Optional package '%s' is NOT installed.", pkg_name)
        else:
            log.error("Required package '%s' is NOT installed.", pkg_name)
        return False

def main():
    log.info("=== Step 1: environment & config validation ===")
    log.debug("dotenv loaded: %s", dotenv_loaded)

    # 1) Python
    ok_py = check_python_version(3, 8)

    # 2) External commands
    git_ok = check_command("git")

    # 3) Python deps (requests, tqdm required; GitPython optional)
    req_pkgs = [("requests", False), ("tqdm", False), ("python_dotenv", True)]
    # note: GitPython package is named 'git' for import
    req_pkgs.append(("git", True))  # GitPython optional

    pkg_results = {}
    for pkg, optional in req_pkgs:
        # map 'python_dotenv' -> 'dotenv' import name
        imp_name = "dotenv" if pkg == "python_dotenv" else pkg
        ok = check_package(imp_name, optional=optional)
        pkg_results[pkg] = ok

    # 4) Config values (.env or env)
    github_token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("REPO")
    outdir = os.environ.get("OUTDIR") or "out"
    clone_path = os.environ.get("CLONE_PATH")

    log.info("Config values (from env/.env):")
    log.info("  GITHUB_TOKEN: %s", "SET" if github_token else "MISSING")
    log.info("  REPO: %s", repo or "MISSING (expected owner/repo)")
    log.info("  OUTDIR: %s", outdir)
    log.info("  CLONE_PATH: %s", clone_path or "not set")

    # 5) If clone_path provided, check it exists and looks like a git repo
    clone_ok = True
    if clone_path:
        if os.path.isdir(clone_path):
            if os.path.isdir(os.path.join(clone_path, ".git")):
                log.info("CLONE_PATH exists and contains .git (looks like a clone).")
            else:
                log.warning("CLONE_PATH exists but does NOT contain .git. It may not be a git clone.")
            clone_ok = True
        else:
            log.error("CLONE_PATH '%s' does not exist.", clone_path)
            clone_ok = False

    # 6) Summary & recommendations
    all_good = ok_py and git_ok and pkg_results["requests"] and pkg_results["tqdm"]
    if not all_good:
        log.error("Environment checks FAILED. See above messages.")
        log.info("Recommended install commands (copy/paste):")
        log.info("  python3 -m pip install --upgrade pip")
        log.info("  python3 -m pip install requests tqdm python-dotenv GitPython")
    else:
        log.info("All required checks passed.")

    # Give actionable next steps depending on missing items
    if not github_token:
        log.warning("GITHUB_TOKEN is missing. Create a Personal Access Token and add to .env or export as environment variable.")
        log.info("Example .env entry: GITHUB_TOKEN=ghp_xxxYOURTOKENxxx")
    if not repo:
        log.warning("REPO is missing. Set REPO=owner/repo in .env or pass via CLI when running the exporter.")
        log.info("Example .env entry: REPO=Thomas-George-T/Ecommerce-Data-MLOps")

    # Exit code: 0 if minimal requirements satisfied, else 2
    sys.exit(0 if all_good and github_token and repo and clone_ok else 2)

if __name__ == "__main__":
    main()
