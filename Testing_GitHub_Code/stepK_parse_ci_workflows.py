#!/usr/bin/env python3
"""
stepK_parse_ci_workflows.py

Purpose:
  - Discover CI/CD workflow files in the repository (.github/workflows/*.yml, .gitlab-ci.yml, Jenkinsfiles, etc.)
  - Parse workflows (YAML) and extract structured info:
      - workflows -> jobs -> steps
      - job-level and step-level 'uses', 'run' commands, 'with', 'env'
      - referenced actions (docker image, actions/*), external scripts, artifacts, and environment keys
  - Detect potential secrets/credentials in workflow envs or 'with' blocks and redact values.
  - Emit:
      - results/ci_workflows.json         (structured workflow objects)
      - results/ci_env_secrets_report.json (list of detected secret keys and location)
  - Log progress and provide safe defaults.

Notes:
  - Uses PyYAML (safe_load). If not installed: `pip install pyyaml`.
  - Does not execute any CI code; static parsing only.
  - Redaction is configurable via SENSITIVE_KEY_PATTERNS below.
  - Designed to be idempotent and produce KG-ready JSON.

Usage:
  python stepK_parse_ci_workflows.py --repo-dir /path/to/repo --out-dir ./results

Author: Professional KG pipeline
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import pathlib
import re
import sys
import glob
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except Exception as e:
    raise ImportError("PyYAML is required. Install with: pip install pyyaml") from e

# ----------------------------
# Config
# ----------------------------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), "results")
LOG_FILENAME = "stepK_parse_ci_workflows.log"

# Patterns used to flag potentially sensitive keys (case-insensitive)
SENSITIVE_KEY_PATTERNS = [
    r"(?i)secret", r"(?i)token", r"(?i)key", r"(?i)password", r"(?i)passwd",
    r"(?i)api[_-]?key", r"(?i)credential", r"(?i)auth", r"(?i)aws_access", r"(?i)aws_secret"
]

# Redaction placeholder
REDACTED = "<REDACTED>"

# Files to discover and parse
# WORKFLOW_GLOBS = [
#     ".github/workflows/*.yml",
#     ".github/workflows/*.yaml",
#     ".gitlab-ci.yml",
#     "Jenkinsfile",  # Jenkinsfile is often Groovy; we'll capture presence and raw text
#     ".github/workflows/**.yml",
#     ".github/workflows/**.yaml"
# ]

WORKFLOW_GLOBS = [
    ".github/workflows/*.yml",
    ".github/workflows/*.yaml",
    ".gitlab-ci.yml",
    "Jenkinsfile",  # Jenkinsfile is often Groovy; we'll capture presence and raw text
    ".github/workflows/**/*.yml",
    ".github/workflows/**/*.yaml"
]

# ----------------------------
# Logging helper
# ----------------------------
def setup_logging(out_dir: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(out_dir, exist_ok=True)
    logger = logging.getLogger("ci-parser")
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(os.path.join(out_dir, LOG_FILENAME))
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger

# ----------------------------
# Utilities
# ----------------------------
def safe_relpath(repo_dir: str, path: str) -> str:
    try:
        return str(pathlib.Path(path).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return os.path.basename(path)

# def find_workflow_files(repo_dir: str, logger: logging.Logger) -> List[str]:
#     repo = pathlib.Path(repo_dir)
#     found = []
#     for pattern in WORKFLOW_GLOBS:
#         for p in repo.glob(pattern):
#             if p.is_file():
#                 found.append(str(p.resolve()))
#     # Also include Jenkinsfile at repo root
#     jf = repo / "Jenkinsfile"
#     if jf.exists() and str(jf.resolve()) not in found:
#         found.append(str(jf.resolve()))
#     # Include .gitlab-ci.yml if present
#     gl = repo / ".gitlab-ci.yml"
#     if gl.exists() and str(gl.resolve()) not in found:
#         found.append(str(gl.resolve()))
#     logger.info("Discovered %d CI/CD manifest files", len(found))
#     return sorted(found)




def find_workflow_files(repo_dir: str, logger: logging.Logger) -> List[str]:
    """
    Robust discovery of CI/CD manifest files using glob with recursive support.
    Handles patterns like '**/*.yml' and simple file names.
    Returns sorted list of absolute file paths.
    """
    repo_dir = os.path.abspath(repo_dir)
    found = set()

    for pattern in WORKFLOW_GLOBS:
        # If pattern is just a filename like 'Jenkinsfile' or '.gitlab-ci.yml',
        # search repo root AND subdirectories for that filename.
        if os.path.basename(pattern) in ("Jenkinsfile", ".gitlab-ci.yml"):
            # search for exact filename anywhere under repo
            glob_pattern = os.path.join(repo_dir, "**", os.path.basename(pattern))
            matches = glob.glob(glob_pattern, recursive=True)
        else:
            # otherwise join repo dir with the pattern and allow recursive matching
            glob_pattern = os.path.join(repo_dir, pattern)
            matches = glob.glob(glob_pattern, recursive=True)

        for m in matches:
            if os.path.isfile(m):
                found.add(os.path.abspath(m))

    # Deduplicate + sort
    result = sorted(found)
    logger.info("Discovered %d CI/CD manifest files", len(result))
    return result

def is_sensitive_key(key: str) -> bool:
    for pat in SENSITIVE_KEY_PATTERNS:
        if re.search(pat, key):
            return True
    return False

def redact_value_if_sensitive(key: str, value: Any) -> Tuple[Any, bool]:
    """
    If key looks sensitive, redact the value.
    Returns (possibly_redacted_value, was_redacted_bool)
    """
    if key is None:
        return value, False
    if is_sensitive_key(key):
        return REDACTED, True
    # Also if value itself contains obvious tokens (e.g., startswith 'ghp_' or long hex)
    if isinstance(value, str):
        if re.match(r"ghp_[A-Za-z0-9]+", value) or re.match(r"[A-Fa-f0-9]{40,}", value):
            return REDACTED, True
    return value, False

def safe_load_yaml(text: str) -> Optional[Any]:
    try:
        return yaml.safe_load(text)
    except Exception:
        # try with loader that preserves more (fallback) - but keep safe
        try:
            return yaml.load(text, Loader=yaml.SafeLoader)
        except Exception:
            return None

# ----------------------------
# Parsers
# ----------------------------
def parse_github_workflow(content: str, relpath: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Parse a GitHub Actions workflow YAML into structured form:
      { name, on, jobs: {job_id: {name, runs-on, steps: [{name, uses, run, with, env}]}} }
    """
    obj = safe_load_yaml(content)
    if obj is None:
        logger.warning("YAML parse failed for %s", relpath)
        return {"file": relpath, "parse_error": True, "raw": content[:2048]}

    result = {"file": relpath, "type": "github_workflow", "name": obj.get("name"), "on": obj.get("on"), "jobs": {}}
    jobs = obj.get("jobs") or {}
    for job_id, job_def in jobs.items():
        job_entry: Dict[str, Any] = {}
        job_entry["name"] = job_def.get("name") or job_id
        job_entry["runs_on"] = job_def.get("runs-on") or job_def.get("runs_on")
        job_entry["env"] = {}
        job_entry["steps"] = []
        # job-level env
        job_env = job_def.get("env", {}) or {}
        for k, v in list(job_env.items()):
            rv, redacted = redact_value_if_sensitive(k, v)
            job_entry["env"][k] = rv
        # steps
        steps = job_def.get("steps") or []
        for step in steps:
            s = {}
            s["name"] = step.get("name")
            s["uses"] = step.get("uses")
            s["run"] = None
            if "run" in step:
                # capture first 500 chars of script
                s["run"] = step.get("run")[:2000] if isinstance(step.get("run"), str) else step.get("run")
            s["with"] = {}
            for k, v in (step.get("with") or {}).items():
                rv, redacted = redact_value_if_sensitive(k, v)
                s["with"][k] = rv
            s["env"] = {}
            for k, v in (step.get("env") or {}).items():
                rv, redacted = redact_value_if_sensitive(k, v)
                s["env"][k] = rv
            job_entry["steps"].append(s)
        result["jobs"][job_id] = job_entry
    return result

def parse_gitlab_ci(content: str, relpath: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Parse .gitlab-ci.yml into jobs mapping:
      job_name: {stage, script, variables, only, except}
    """
    obj = safe_load_yaml(content)
    if obj is None:
        logger.warning("YAML parse failed for %s", relpath)
        return {"file": relpath, "parse_error": True, "raw": content[:2048]}

    result = {"file": relpath, "type": "gitlab_ci", "jobs": {}}
    # heuristic: top-level keys that are dicts and not 'stages' etc. are jobs
    for k, v in (obj or {}).items():
        if k in ("stages", "variables", "image", "services", "include", "workflow"):
            continue
        if isinstance(v, dict):
            job = {}
            job["script"] = v.get("script")
            job["stage"] = v.get("stage")
            job["variables"] = {}
            for var_key, var_val in (v.get("variables") or {}).items():
                rv, redacted = redact_value_if_sensitive(var_key, var_val)
                job["variables"][var_key] = rv
            result["jobs"][k] = job
    return result

def parse_jenkinsfile(content: str, relpath: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Jenkinsfile (Groovy) is difficult to parse statically without a Groovy parser.
    We will provide:
      - raw presence
      - heuristics: extract environment block lines, credentialId usage, sh steps
    """
    lines = content.splitlines()
    job = {"file": relpath, "type": "jenkinsfile", "env": {}, "stages": []}
    in_env = False
    env_lines = []
    credential_ids = []
    scripts = []
    for ln in lines:
        s = ln.strip()
        if s.startswith("environment"):
            in_env = True
            continue
        if in_env:
            if s.startswith("}") or s == "":
                in_env = False
            else:
                env_lines.append(s)
        # find credentials usage e.g., credentials('MY_CRED')
        m = re.search(r"credentials\(['\"]([^'\"]+)['\"]\)", ln)
        if m:
            credential_ids.append(m.group(1))
        # find sh """ ... """ or sh('...')
        if s.startswith("sh "):
            scripts.append(s)
    # parse env_lines of form NAME = 'value'
    for el in env_lines:
        m = re.match(r"([A-Za-z0-9_]+)\s*=\s*['\"]?(.*?)['\"]?$", el)
        if m:
            k = m.group(1); v = m.group(2)
            rv, redacted = redact_value_if_sensitive(k, v)
            job["env"][k] = rv
    job["credential_ids"] = credential_ids
    job["scripts_preview"] = scripts[:20]
    return job

# ----------------------------
# Orchestration
# ----------------------------
def parse_file(path: str, repo_dir: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    rel = safe_relpath(repo_dir, path)
    text = None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except Exception as e:
        logger.warning("Failed to read %s: %s", rel, e)
        return None

    fname = os.path.basename(path).lower()
    if fname.endswith(".yml") or fname.endswith(".yaml"):
        # heuristics: if path under .github/workflows -> GitHub Actions
        if ".github" in path and "workflows" in path:
            return parse_github_workflow(text, rel, logger)
        elif fname == ".gitlab-ci.yml" or ".gitlab" in path:
            return parse_gitlab_ci(text, rel, logger)
        else:
            # fallback: try GitHub workflow parse
            return parse_github_workflow(text, rel, logger)
    elif fname == "jenkinsfile" or "/jenkinsfile" in path.lower():
        return parse_jenkinsfile(text, rel, logger)
    elif fname == ".gitlab-ci.yml":
        return parse_gitlab_ci(text, rel, logger)
    else:
        # unknown type: return raw preview
        logger.info("Unknown CI file type for %s; storing raw preview", rel)
        return {"file": rel, "type": "unknown_ci", "raw": text[:4000]}

def detect_secrets_in_object(obj: Any, relpath: str) -> List[Dict[str, Any]]:
    """
    Walk a parsed workflow object and detect keys that match sensitive patterns,
    returning a list of detections with location hints.
    """
    detections = []
    def walk(o, path_stack=None):
        if path_stack is None:
            path_stack = []
        if isinstance(o, dict):
            for k, v in o.items():
                if is_sensitive_key(str(k)):
                    detections.append({"file": relpath, "path": ".".join(path_stack + [str(k)]), "key": k, "value_preview": None if v is None else (v if isinstance(v,str) and len(v)<200 else str(v)[:200])})
                # if value is string and matches token patterns
                if isinstance(v, str):
                    if re.match(r"^(ghp|gho|GITHUB|AKIA|[A-Fa-f0-9]{32,})", v):
                        detections.append({"file": relpath, "path": ".".join(path_stack + [str(k)]), "key": k, "value_preview": "<LIKELY_TOKEN>"})
                walk(v, path_stack + [str(k)])
        elif isinstance(o, list):
            for i, item in enumerate(o):
                walk(item, path_stack + [f"[{i}]"])
    walk(obj)
    return detections

# ----------------------------
# Main
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Parse CI/CD workflow files and extract structured metadata (redact secrets).")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository root path")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Results directory")
    p.add_argument("--write-raw", action="store_true", help="Also write raw workflow files (sanitized) to results/raw_workflows")
    return p.parse_args()

def main():
    args = parse_args()
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    write_raw = args.write_raw

    logger = setup_logging(out_dir)
    logger.info("Starting CI/CD workflow parsing")
    logger.info("Repo dir: %s", repo_dir)
    logger.info("Out dir: %s", out_dir)

    files = find_workflow_files(repo_dir, logger)
    parsed_results: List[Dict[str, Any]] = []
    all_detections: List[Dict[str, Any]] = []

    raw_out_dir = os.path.join(out_dir, "raw_workflows")
    if write_raw:
        os.makedirs(raw_out_dir, exist_ok=True)

    for f in files:
        rel = safe_relpath(repo_dir, f)
        logger.info("Parsing: %s", rel)
        parsed = parse_file(f, repo_dir, logger)
        if parsed is None:
            continue
        # detect secrets
        detections = detect_secrets_in_object(parsed, rel)
        all_detections.extend(detections)
        # optionally redact values in parsed object (replace any sensitive keys' values with REDACTED)
        def redact_inplace(o):
            if isinstance(o, dict):
                for k in list(o.keys()):
                    v = o[k]
                    if is_sensitive_key(str(k)):
                        if isinstance(v, str):
                            o[k] = REDACTED
                        else:
                            o[k] = REDACTED
                    else:
                        redact_inplace(v)
            elif isinstance(o, list):
                for item in o:
                    redact_inplace(item)
        redact_inplace(parsed)

        parsed_results.append(parsed)
        if write_raw:
            try:
                # write sanitized JSON form
                safe_name = rel.replace("/", "__")
                with open(os.path.join(raw_out_dir, f"{safe_name}.json"), "w", encoding="utf-8") as fo:
                    json.dump(parsed, fo, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.warning("Failed to write raw workflow JSON for %s: %s", rel, e)

    # write outputs
    os.makedirs(out_dir, exist_ok=True)
    workflows_out = os.path.join(out_dir, "ci_workflows.json")
    detections_out = os.path.join(out_dir, "ci_env_secrets_report.json")
    try:
        with open(workflows_out, "w", encoding="utf-8") as fo:
            json.dump(parsed_results, fo, indent=2, ensure_ascii=False)
        with open(detections_out, "w", encoding="utf-8") as fo:
            json.dump(all_detections, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote parsed workflows to: %s", workflows_out)
        logger.info("Wrote secrets report to: %s", detections_out)
    except Exception as e:
        logger.error("Failed to write outputs: %s", e)
        sys.exit(2)

    logger.info("CI/CD parsing complete. Workflows=%d detections=%d", len(parsed_results), len(all_detections))
    logger.info("Done.")

if __name__ == "__main__":
    main()
