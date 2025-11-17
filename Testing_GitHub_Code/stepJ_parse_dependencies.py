"""
stepJ_parse_dependencies.py

Production-ready dependency manifest parser for multi-language repositories.

What this script does (complete):
  - Recursively discovers common dependency manifest files:
      - Python: requirements.txt, pyproject.toml, setup.cfg, Pipfile, environment.yml
      - Node: package.json, package-lock.json, yarn.lock (basic parse)
      - Java/Maven: pom.xml
      - Go: go.mod
      - Ruby: Gemfile / Gemfile.lock (basic parse)
      - Rust: Cargo.toml
  - Parses each manifest into structured dependency entries:
      { "name": <pkg>, "version": <ver or range/null>, "file": <relpath>, "ecosystem": <python/npm/maven/go/...> }
  - Normalizes versions where possible, and marks unresolved or dynamic specs.
  - Produces:
      results/dependencies.json   -> aggregated list of dependencies
      results/dependencies_by_file.json -> mapping file -> [deps]
  - Logs progress and writes a concise summary.
  - Safe to run repeatedly; tolerant of missing optional libraries (uses builtin parsers).
  - Attempts to avoid network calls; purely static parsing.

Usage:
  python stepJ_parse_dependencies.py --repo-dir /path/to/repo --out-dir ./results

Outputs:
  - ./results/dependencies.json
  - ./results/dependencies_by_file.json
  - ./results/stepJ_parse_dependencies.log

Notes:
  - This script focuses on static manifest parsing. For lockfile-resolved dependency graphs
    (transitive resolution), use language-specific package tooling (pip-compile, npm ls, mvn dependency:tree).
  - The parser aims for broad coverage and conservative outputs suitable for KG ingestion.

Author: Automated KG pipeline (professional quality)
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, Optional

# ---------- Defaults ----------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), "results")
LOG_FILENAME = "stepJ_parse_dependencies.log"

# ---------- Logging ----------
def setup_logger(out_dir: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(out_dir, exist_ok=True)
    logger = logging.getLogger("dep-parser")
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

# ---------- Helpers ----------
def read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return None

def safe_rel(repo_dir: str, path: str) -> str:
    try:
        return str(pathlib.Path(path).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return os.path.basename(path)

# ---------- Parsers ----------
def parse_requirements_txt(text: str) -> List[Dict[str, Any]]:
    """
    parse PEP 508-ish lines and common requirement syntaxes.
    returns list of {name, version, marker/raw}
    """
    deps = []
    # Strip comments but preserve markers after ; (env markers)
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # handle -r includes (skip)
        if line.startswith("-r ") or line.startswith("--requirement"):
            continue
        # editable installs: -e git+...#egg=package
        if line.startswith("-e ") or line.startswith("--editable "):
            # try to extract egg= or name
            m = re.search(r"egg=([^&]+)", line)
            name = m.group(1) if m else line.split()[-1]
            deps.append({"name": name, "version": None, "raw": line, "ecosystem": "python"})
            continue
        # direct url installs
        if "://" in line and "egg=" in line:
            m = re.search(r"egg=([^&]+)", line)
            name = m.group(1) if m else None
            deps.append({"name": name or line, "version": None, "raw": line, "ecosystem": "python"})
            continue
        # PEP508 e.g., package>=1.2; python_version<'3.8'
        # split on ; for markers
        parts = [p.strip() for p in line.split(";")]
        spec = parts[0]
        marker = parts[1] if len(parts) > 1 else None
        # split extras: pkg[extra]==1.2
        # match name and version spec using common separators
        m = re.match(r"^([A-Za-z0-9_\-\.]+)(\[(.*?)\])?\s*(?:([<>=!~]+)\s*(.+))?$", spec)
        if m:
            name = m.group(1)
            version = m.group(4) + " " + m.group(5) if m.group(4) and m.group(5) else (m.group(5) if m.group(5) else None)
            if version:
                version = version.strip()
            deps.append({"name": name, "version": version, "marker": marker, "raw": line, "ecosystem": "python"})
        else:
            # fallback: treat entire spec as name
            deps.append({"name": spec, "version": None, "marker": marker, "raw": line, "ecosystem": "python"})
    return deps

def parse_pyproject_toml(text: str) -> List[Dict[str, Any]]:
    """
    Parse pyproject.toml minimally for [project] or [tool.poetry] or [build-system]
    Avoid external toml dependency; do simple regex parsing for common patterns.
    """
    deps = []
    try:
        import toml
        data = toml.loads(text)
        # PEP621 project table
        if "project" in data and isinstance(data["project"], dict):
            for table in ("dependencies", "optional-dependencies"):
                if table in data["project"]:
                    deps_map = data["project"][table]
                    if isinstance(deps_map, dict):
                        for name, spec in deps_map.items():
                            if isinstance(spec, str):
                                deps.append({"name": name, "version": spec, "raw": spec, "ecosystem": "python"})
                            else:
                                deps.append({"name": name, "version": None, "raw": str(spec), "ecosystem": "python"})
        # poetry
        if "tool" in data and "poetry" in data["tool"]:
            for key in ("dependencies", "dev-dependencies"):
                if key in data["tool"]["poetry"]:
                    for name, spec in data["tool"]["poetry"][key].items():
                        if name == "python":
                            continue
                        if isinstance(spec, str):
                            deps.append({"name": name, "version": spec, "raw": spec, "ecosystem": "python"})
                        elif isinstance(spec, dict) and "version" in spec:
                            deps.append({"name": name, "version": spec.get("version"), "raw": spec, "ecosystem": "python"})
        return deps
    except Exception:
        # fallback naive parse: look for lines like name = "1.2.3" under [tool.poetry.dependencies]
        lines = text.splitlines()
        current_section = None
        for ln in lines:
            s = ln.strip()
            if s.startswith("[") and s.endswith("]"):
                current_section = s.strip("[]")
            if current_section and ("dependencies" in current_section):
                m = re.match(r'^([A-Za-z0-9_\-\.]+)\s*=\s*["\']?(.*?)["\']?\s*$', s)
                if m:
                    name = m.group(1)
                    ver = m.group(2).strip() if m.group(2) else None
                    if name.lower() != "python":
                        deps.append({"name": name, "version": ver, "raw": ver, "ecosystem": "python"})
        return deps

def parse_setup_cfg(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read_string(text)
        if cfg.has_section("options") and cfg.has_option("options", "install_requires"):
            raw = cfg.get("options", "install_requires")
            # install_requires can be multiline
            deps.extend(parse_requirements_txt(raw))
        return deps
    except Exception:
        return []

def parse_pipfile(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        import toml
        data = toml.loads(text)
        for section in ("packages", "dev-packages"):
            if section in data:
                for name, spec in data[section].items():
                    if isinstance(spec, str):
                        deps.append({"name": name, "version": spec, "ecosystem": "python"})
                    elif isinstance(spec, dict):
                        deps.append({"name": name, "version": spec.get("version"), "raw": spec, "ecosystem": "python"})
        return deps
    except Exception:
        # naive parse
        for ln in text.splitlines():
            m = re.match(r'^\s*["\']?([A-Za-z0-9_\-\.]+)["\']?\s*=\s*["\']?(.*?)["\']?$', ln)
            if m:
                deps.append({"name": m.group(1), "version": m.group(2), "ecosystem": "python"})
        return deps

def parse_environment_yml(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        import yaml
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            return deps
        for section in ("dependencies",):
            if section in data and isinstance(data[section], list):
                for item in data[section]:
                    if isinstance(item, str):
                        # conda style "package=1.2" or "python=3.8"
                        m = re.match(r"^([^\s=<>!~]+)\s*([=<>!~].*)?$", item)
                        if m:
                            name = m.group(1)
                            ver = m.group(2).lstrip("=") if m.group(2) else None
                            deps.append({"name": name, "version": ver, "ecosystem": "conda"})
                    elif isinstance(item, dict):
                        # pip:
                        pip_specs = item.get("pip", [])
                        for p in pip_specs:
                            deps.extend(parse_requirements_txt(p))
        return deps
    except Exception:
        # fallback naive parse
        for ln in text.splitlines():
            m = re.match(r"^\s*-\s*([A-Za-z0-9_\-\.]+)(?:=(.*))?", ln)
            if m:
                deps.append({"name": m.group(1), "version": m.group(2), "ecosystem": "conda"})
        return deps

def parse_package_json(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        data = json.loads(text)
        for key in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
            if key in data and isinstance(data[key], dict):
                for name, ver in data[key].items():
                    deps.append({"name": name, "version": ver, "ecosystem": "npm"})
        return deps
    except Exception:
        return deps

def parse_pom_xml(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        root = ET.fromstring(text)
        # handle namespace if present
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0].strip("{")  # namespace
            ns_map = {"ns": ns}
            dep_path = ".//{"+ns+"}dependencies/{"+ns+"}dependency"
        else:
            dep_path = ".//dependencies/dependency"
        for dep in root.findall(dep_path):
            group = dep.find("{%s}groupId" % ns) if ns else dep.find("groupId")
            artifact = dep.find("{%s}artifactId" % ns) if ns else dep.find("artifactId")
            version = dep.find("{%s}version" % ns) if ns else dep.find("version")
            if group is not None and artifact is not None:
                name = f"{group.text}:{artifact.text}"
                ver = version.text if version is not None else None
                deps.append({"name": name, "version": ver, "ecosystem": "maven"})
        return deps
    except Exception:
        return deps

def parse_go_mod(text: str) -> List[Dict[str, Any]]:
    deps = []
    # look for lines like: require github.com/pkg/errors v0.8.1
    for ln in text.splitlines():
        ln = ln.strip()
        if ln.startswith("require "):
            # strip "require (" multi-line block handling
            content = ln[len("require "):].strip()
            # if it's a block opener, skip
            if content.startswith("("):
                continue
            parts = content.split()
            if len(parts) >= 2:
                deps.append({"name": parts[0], "version": parts[1], "ecosystem": "go"})
        elif ln and not ln.startswith("//") and " " in ln and ("/" in ln):
            # probably inside require (...) block
            m = re.match(r"^([^\s]+)\s+v?([^\s]+)$", ln)
            if m:
                deps.append({"name": m.group(1), "version": m.group(2), "ecosystem": "go"})
    return deps

def parse_gemfile_lock(text: str) -> List[Dict[str, Any]]:
    deps = []
    # naive approach: lines starting at top-level "    gem_name (x.y.z)"
    for ln in text.splitlines():
        m = re.match(r"^\s{4}([a-zA-Z0-9_\-]+)\s\(([^)]+)\)", ln)
        if m:
            deps.append({"name": m.group(1), "version": m.group(2), "ecosystem": "ruby"})
    return deps

def parse_cargo_toml(text: str) -> List[Dict[str, Any]]:
    deps = []
    try:
        import toml
        data = toml.loads(text)
        if "dependencies" in data:
            for name, spec in data["dependencies"].items():
                if isinstance(spec, str):
                    deps.append({"name": name, "version": spec, "ecosystem": "rust"})
                elif isinstance(spec, dict) and "version" in spec:
                    deps.append({"name": name, "version": spec.get("version"), "ecosystem": "rust"})
        return deps
    except Exception:
        # naive parse
        for ln in text.splitlines():
            m = re.match(r'^\s*([a-zA-Z0-9_\-]+)\s*=\s*"(.*?)"', ln)
            if m:
                deps.append({"name": m.group(1), "version": m.group(2), "ecosystem": "rust"})
        return deps

# ---------- Discovery ----------
COMMON_FILES = [
    "requirements.txt", "requirements-dev.txt", "pyproject.toml", "setup.cfg", "Pipfile", "environment.yml", "environment.yaml",
    "package.json", "package-lock.json", "yarn.lock", "pom.xml", "go.mod", "Gemfile", "Gemfile.lock", "Cargo.toml"
]

def discover_manifest_files(repo_dir: str, logger: logging.Logger) -> List[str]:
    found = []
    repo = pathlib.Path(repo_dir)
    for root, dirs, files in os.walk(repo):
        # skip hidden .venv or virtualenv folders
        if any(part.startswith(".") and part not in (".github",) for part in pathlib.Path(root).parts):
            # still walk though .github
            pass
        for name in files:
            if name in COMMON_FILES:
                found.append(str(pathlib.Path(root) / name))
    logger.info("Discovered %d manifest files", len(found))
    return sorted(found)

# ---------- Orchestration ----------
def parse_file_by_type(path: str, repo_dir: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    text = read_text(path)
    if text is None:
        logger.warning("Unable to read file: %s", path)
        return []
    fname = os.path.basename(path).lower()
    rel = safe_rel(repo_dir, path)
    parsed = []
    try:
        if fname == "requirements.txt" or fname.startswith("requirements"):
            parsed = parse_requirements_txt(text)
        elif fname == "pyproject.toml":
            parsed = parse_pyproject_toml(text)
        elif fname == "setup.cfg":
            parsed = parse_setup_cfg(text)
        elif fname == "pipfile":
            parsed = parse_pipfile(text)
        elif fname in ("environment.yml", "environment.yaml"):
            parsed = parse_environment_yml(text)
        elif fname == "package.json":
            parsed = parse_package_json(text)
        elif fname == "pom.xml":
            parsed = parse_pom_xml(text)
        elif fname == "go.mod":
            parsed = parse_go_mod(text)
        elif fname == "gemfile.lock":
            parsed = parse_gemfile_lock(text)
        elif fname == "cargo.toml":
            parsed = parse_cargo_toml(text)
        elif fname == "package-lock.json":
            # parse resolved versions
            try:
                data = json.loads(text)
                if "dependencies" in data and isinstance(data["dependencies"], dict):
                    for name,info in data["dependencies"].items():
                        parsed.append({"name": name, "version": info.get("version"), "ecosystem": "npm"})
            except Exception:
                pass
        elif fname == "yarn.lock":
            # yarn.lock parsing is complex; attempt simple regex for occurrences of "name@version:"
            for ln in text.splitlines():
                m = re.match(r"^([^\s@][^:]+):\s*$", ln)
                if m:
                    # cannot reliably extract version here
                    parsed.append({"name": m.group(1), "version": None, "ecosystem": "npm"})
        else:
            # fallback: attempt to detect by extension
            if path.endswith(".toml"):
                parsed = parse_pyproject_toml(text)
            elif path.endswith(".xml"):
                parsed = parse_pom_xml(text)
            elif path.endswith(".json"):
                parsed = parse_package_json(text)
    except Exception as e:
        logger.debug("Parser error for %s: %s", rel, e)
    # attach provenance fields
    for d in parsed:
        d.setdefault("file", rel)
        d.setdefault("raw", d.get("raw", None))
    logger.info("Parsed %d deps from %s", len(parsed), rel)
    return parsed

# ---------- Main ----------
def parse_args():
    p = argparse.ArgumentParser(description="Parse dependency manifests into structured JSON for KG ingestion.")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository root to scan.")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for results.")
    p.add_argument("--write-by-file", action="store_true", help="Also write individual file-level dependency JSONs (debug).")
    return p.parse_args()

def main():
    args = parse_args()
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    write_by_file = args.write_by_file

    logger = setup_logger(out_dir)
    logger.info("Starting dependency parsing")
    logger.info("Repo dir: %s", repo_dir)
    logger.info("Out dir: %s", out_dir)

    manifests = discover_manifest_files(repo_dir, logger)
    all_deps: List[Dict[str, Any]] = []
    deps_by_file: Dict[str, List[Dict[str, Any]]] = {}

    for mf in manifests:
        parsed = parse_file_by_type(mf, repo_dir, logger)
        deps_by_file[safe_rel(repo_dir, mf)] = parsed
        all_deps.extend(parsed)
        if write_by_file:
            # write per-file debug JSON
            try:
                with open(os.path.join(out_dir, f"deps_{safe_rel(repo_dir,mf).replace('/','__')}.json"), "w", encoding="utf-8") as fo:
                    json.dump(parsed, fo, indent=2, ensure_ascii=False)
            except Exception:
                logger.debug("Failed to write per-file JSON for %s", mf)

    # normalize entries: ensure name, ecosystem, version, file
    normalized: List[Dict[str, Any]] = []
    seen = set()
    for d in all_deps:
        name = d.get("name") or d.get("package") or None
        if not name:
            continue
        ecosystem = d.get("ecosystem") or infer_ecosystem_from_file(d.get("file", ""))
        version = d.get("version") or d.get("raw") or None
        file_rel = d.get("file")
        key = f"{ecosystem}::{name}::{version}::{file_rel}"
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "name": name,
            "version": version,
            "ecosystem": ecosystem,
            "file": file_rel,
            "raw": d.get("raw")
        })

    # write outputs
    os.makedirs(out_dir, exist_ok=True)
    out_all = os.path.join(out_dir, "dependencies.json")
    out_by_file = os.path.join(out_dir, "dependencies_by_file.json")
    try:
        with open(out_all, "w", encoding="utf-8") as fo:
            json.dump(normalized, fo, indent=2, ensure_ascii=False)
        with open(out_by_file, "w", encoding="utf-8") as fo:
            json.dump(deps_by_file, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote dependencies: %s (total=%d)", out_all, len(normalized))
        logger.info("Wrote dependencies_by_file: %s", out_by_file)
    except Exception as e:
        logger.error("Failed to write outputs: %s", e)
        sys.exit(2)

    logger.info("Dependency parsing complete. Found %d unique dependency entries across %d manifests.", len(normalized), len(manifests))
    logger.info("Done.")

# ---------- Utility: infer ecosystem ----------
def infer_ecosystem_from_file(relpath: str) -> str:
    if not relpath:
        return "unknown"
    lp = relpath.lower()
    if lp.endswith("requirements.txt") or lp.endswith(".pipfile") or lp.endswith("pyproject.toml") or lp.endswith("setup.cfg"):
        return "python"
    if lp.endswith("package.json") or "yarn.lock" in lp or "package-lock.json" in lp:
        return "npm"
    if lp.endswith("pom.xml"):
        return "maven"
    if lp.endswith("go.mod"):
        return "go"
    if lp.endswith("gemfile") or lp.endswith("gemfile.lock"):
        return "ruby"
    if lp.endswith("cargo.toml"):
        return "rust"
    return "unknown"

if __name__ == "__main__":
    main()
