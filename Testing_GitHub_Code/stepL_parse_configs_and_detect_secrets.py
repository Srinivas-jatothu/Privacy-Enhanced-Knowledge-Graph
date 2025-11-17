"""
stepL_parse_configs_and_detect_secrets.py

Purpose
-------
- Discover configuration files in a repository (env files, YAML/JSON/TOML/INI/XML/props).
- Parse them into structured JSON.
- Detect likely secrets / tokens and redact their values for safe storage.
- Emit outputs suitable for KG ingestion and security review:
    - results/configs.json                (redacted configs, normalized)
    - results/configs_raw_parsed.json     (original parsed values - still sanitized)
    - results/configs_secrets_report.json (list of detections with locations)
    - results/stepL_parse_configs_and_detect_secrets.log (log)

Notes
-----
- This is a static, local parser only; it does NOT call external services.
- Redaction heuristics are conservative (may flag false positives). Inspect the secrets report manually.
- Requires `pyyaml` and `toml` for full parsing; falls back to conservative text parsing otherwise.

Usage
-----
python stepL_parse_configs_and_detect_secrets.py --repo-dir /path/to/repo --out-dir ./results

Author: PEKG pipeline (professional)
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import pathlib
import re
import sys
from typing import Any, Dict, List, Tuple, Optional

# Optional packages
try:
    import yaml
except Exception:
    yaml = None

try:
    import toml
except Exception:
    toml = None

# --------------------------
# Defaults & config
# --------------------------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), "results")
LOG_FILENAME = "stepL_parse_configs_and_detect_secrets.log"

# Files / globs to look for (common config file names)
CONFIG_FILENAMES = [
    ".env", ".env.example", ".env.sample", "docker-compose.yml", "docker-compose.yaml",
    "config.yml", "config.yaml", "application.yml", "application.yaml", "settings.yml", "settings.yaml",
    "config.json", "package.json", "tsconfig.json", "settings.json",
    "pyproject.toml", "Pipfile", "requirements.txt",
    "setup.cfg", "tox.ini", "pytest.ini",
    "appsettings.json", "appsettings.Development.json",
    ".ini", ".cfg", ".properties", ".xml", ".yml", ".yaml"
]

# Sensitive key patterns (case-insensitive, regex)
SENSITIVE_KEY_PATTERNS = [
    r"(?i)secret", r"(?i)token", r"(?i)key", r"(?i)password", r"(?i)passwd",
    r"(?i)api[_-]?key", r"(?i)credential", r"(?i)auth", r"(?i)aws", r"(?i)azure", r"(?i)gcp",
    r"(?i)private", r"(?i)client[_-]?secret", r"(?i)access[_-]token"
]

# Token-like value heuristics
TOKEN_VALUE_PATTERNS = [
    r"^gh[pousr]_[A-Za-z0-9]{36,}$",     # GitHub tokens (ghp_, gho_, etc)
    r"^[A-Fa-f0-9]{32,}$",               # 32 hex
    r"^[A-Fa-f0-9]{40,}$",               # 40 hex (many tokens)
    r"^[A-Za-z0-9-_]{20,}$",             # long base64-like tokens
    r"^(AKIA|ASIA)[A-Z0-9]{16}$"         # AWS key-ish
]

REDACTED = "<REDACTED>"

# --------------------------
# Logging
# --------------------------
def setup_logging(out_dir: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(out_dir, exist_ok=True)
    log_path = os.path.join(out_dir, LOG_FILENAME)
    logger = logging.getLogger("parse-configs")
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger

# --------------------------
# Utilities
# --------------------------
def safe_rel(repo_dir: str, path: str) -> str:
    try:
        return str(pathlib.Path(path).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return os.path.basename(path)

def discover_config_files(repo_dir: str, logger: logging.Logger) -> List[str]:
    repo = pathlib.Path(repo_dir)
    found = []
    # walk tree, but skip typical big dirs
    skip_dirs = {".git", ".venv", "venv", "__pycache__", "node_modules", ".pytest_cache", ".mypy_cache"}
    for root, dirs, files in os.walk(repo):
        # prune skip dirs
        dirs[:] = [d for d in dirs if d not in skip_dirs and not d.startswith(".cache")]
        for fname in files:
            lower = fname.lower()
            # match exact known names or common suffixes
            if lower in (n.lower() for n in (".env", ".env.sample", ".env.example")) or lower.endswith((".yml", ".yaml", ".json", ".toml", ".ini", ".cfg", ".properties", ".xml")):
                found.append(os.path.join(root, fname))
            else:
                # also check for common names
                for known in ("docker-compose.yml", "pyproject.toml", "requirements.txt", "Pipfile", "package.json", "appsettings.json"):
                    if lower == known:
                        found.append(os.path.join(root, fname))
                        break
    found = sorted(list(dict.fromkeys(found)))
    logger.info("Discovered %d candidate config files", len(found))
    return found

def is_sensitive_key(key: str) -> bool:
    if not key:
        return False
    for pat in SENSITIVE_KEY_PATTERNS:
        if re.search(pat, key):
            return True
    return False

def value_looks_like_token(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    val = value.strip()
    for pat in TOKEN_VALUE_PATTERNS:
        if re.match(pat, val):
            return True
    # also check base64-like / long lengths
    if len(val) >= 40 and re.fullmatch(r"[A-Za-z0-9+/=._-]+", val):
        return True
    if len(val) >= 30 and re.search(r"[A-Za-z0-9_\-]{20,}", val):
        return True
    return False

def redact_if_sensitive(key: str, value: Any) -> Tuple[Any, Optional[Dict[str, Any]]]:
    """
    Return (possibly_redacted_value, detection_info_or_None)
    detection_info contains {"reason":..., "key":key, "value_preview":...}
    """
    # check key name
    if is_sensitive_key(key):
        preview = None
        try:
            preview = (value[:200] if isinstance(value, str) else str(value))
        except Exception:
            preview = None
        return REDACTED, {"reason": "sensitive_key", "key": key, "value_preview": preview}
    # check value token heuristics
    if value_looks_like_token(value):
        preview = (value[:200] if isinstance(value, str) else str(value))
        return REDACTED, {"reason": "token_like_value", "key": key, "value_preview": preview}
    # not flagged
    return value, None

# --------------------------
# Parsers
# --------------------------
def parse_env_file(text: str) -> Dict[str, Any]:
    """
    Parse .env style KEY=VALUE lines (ignores shell constructs).
    """
    out = {}
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        # key=value or export KEY=VALUE
        if ln.startswith("export "):
            ln = ln[len("export "):].strip()
        parts = re.split(r"\s*=\s*", ln, maxsplit=1)
        if len(parts) == 2:
            k, v = parts
            # strip optional quotes
            if v.startswith(("'", '"')) and v.endswith(("'", '"')) and len(v) >= 2:
                v = v[1:-1]
            out[k] = v
        else:
            # fallback: store as raw
            out[parts[0]] = None
    return out

def parse_json_file(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None

def parse_yaml_file(text: str) -> Any:
    if yaml is None:
        # naive fallback: try json
        try:
            return json.loads(text)
        except Exception:
            return None
    try:
        return yaml.safe_load(text)
    except Exception:
        return None

def parse_toml_file(text: str) -> Any:
    if toml:
        try:
            return toml.loads(text)
        except Exception:
            return None
    # fallback: none
    return None

def parse_ini_file(text: str) -> Dict[str, Dict[str, Any]]:
    try:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read_string(text)
        out = {}
        for sec in cfg.sections():
            out[sec] = dict(cfg.items(sec))
        # also include DEFAULT
        if cfg.defaults():
            out["DEFAULT"] = dict(cfg.defaults())
        return out
    except Exception:
        return {}

def parse_xml_file(text: str) -> Any:
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(text)
        # convert to simple nested dict
        def node_to_dict(node):
            d = {node.tag: {} if node.attrib else None}
            children = list(node)
            if children:
                dd = {}
                for c in children:
                    cd = node_to_dict(c)
                    for k, v in cd.items():
                        if k in dd:
                            # convert to list
                            if not isinstance(dd[k], list):
                                dd[k] = [dd[k]]
                            dd[k].append(v)
                        else:
                            dd[k] = v
                d = {node.tag: dd}
            if node.attrib:
                d[node.tag] = d.get(node.tag, {})
                d[node.tag].update({"@attrs": dict(node.attrib)})
            text = (node.text or "").strip()
            if text:
                if d.get(node.tag) is None:
                    d[node.tag] = text
                else:
                    d[node.tag]["#text"] = text
            return d
        return node_to_dict(root)
    except Exception:
        return None

# --------------------------
# Normalization & traversal
# --------------------------
def traverse_and_redact(obj: Any, prefix: str = "") -> Tuple[Any, List[Dict[str, Any]]]:
    """
    Walk nested dict/list structures, redact sensitive values, return (redacted_obj, detections)
    Each detection: {"path": <dotpath>, "key": key, "reason": reason, "value_preview": preview}
    """
    detections = []
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            subpath = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                rv, det = traverse_and_redact(v, subpath)
                new[k] = rv
                detections.extend(det)
            else:
                redacted_value, detection = redact_if_sensitive(k, v)
                new[k] = redacted_value
                if detection:
                    detection_record = {"path": subpath, **detection}
                    detections.append(detection_record)
        return new, detections
    elif isinstance(obj, list):
        new_list = []
        for idx, item in enumerate(obj):
            subpath = f"{prefix}[{idx}]"
            rv, det = traverse_and_redact(item, subpath)
            new_list.append(rv)
            detections.extend(det)
        return new_list, detections
    else:
        # primitive
        # we can't determine key name here; only check token-like values at top level
        if isinstance(obj, str) and value_looks_like_token(obj):
            # redact ambiguous token-like top-level value
            return REDACTED, [{"path": prefix, "reason": "token_like_value", "key": prefix.split(".")[-1] if prefix else "", "value_preview": obj[:200]}]
        return obj, []

# --------------------------
# Main orchestration
# --------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Parse repo config files and detect/redact secrets.")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository root")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory (results/)")
    p.add_argument("--write-raw-parsed", action="store_true", help="Write raw parsed output (be careful with secrets).")
    return p.parse_args()

def main():
    args = parse_args()
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    write_raw = args.write_raw_parsed

    logger = setup_logging(out_dir)
    logger.info("Starting config parsing and secret detection")
    logger.info("Repo dir: %s", repo_dir)
    logger.info("Out dir: %s", out_dir)

    files = discover_config_files(repo_dir, logger)
    redacted_configs = {}
    raw_parsed = {}
    secrets_report = []

    for f in files:
        rel = safe_rel(repo_dir, f)
        logger.info("Processing: %s", rel)
        try:
            with open(f, "r", encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except Exception as e:
            logger.warning("Failed to read %s: %s", rel, e)
            continue

        parsed = None
        lower = os.path.basename(f).lower()
        if lower in (".env", ".env.sample", ".env.example"):
            parsed = parse_env_file(text)
        elif lower.endswith(".json") or os.path.basename(f).lower() == "package.json":
            parsed = parse_json_file(text)
        elif lower.endswith((".yml", ".yaml")):
            parsed = parse_yaml_file(text)
        elif lower.endswith(".toml"):
            parsed = parse_toml_file(text)
        elif lower.endswith((".ini", ".cfg")):
            parsed = parse_ini_file(text)
        elif lower.endswith(".xml"):
            parsed = parse_xml_file(text)
        else:
            # fallback: attempt YAML -> JSON -> env parse
            parsed = parse_yaml_file(text) or parse_json_file(text) or parse_env_file(text)

        if parsed is None:
            logger.debug("No structured parse available for %s; storing raw preview", rel)
            raw_preview = text[:4096]
            # still do token detection on text
            detections = []
            for pat in TOKEN_VALUE_PATTERNS:
                for m in re.finditer(pat, text):
                    detections.append({"file": rel, "path": "<raw_text>", "key": "<unknown>", "reason": "token_like_in_raw_text", "value_preview": m.group(0)[:200]})
            if detections:
                secrets_report.extend(detections)
            redacted_configs[rel] = {"_raw_preview": raw_preview}
            raw_parsed[rel] = {"_raw_preview": raw_preview}
            continue

        # traverse and redact
        redacted, detections = traverse_and_redact(parsed, prefix="")
        redacted_configs[rel] = redacted
        raw_parsed[rel] = parsed if write_raw else None
        # attach file info for each detection
        for d in detections:
            d_rec = {"file": rel, **d}
            secrets_report.append(d_rec)

    # summarize and write outputs
    os.makedirs(out_dir, exist_ok=True)
    out_redacted = os.path.join(out_dir, "configs.json")
    out_raw = os.path.join(out_dir, "configs_raw_parsed.json")
    out_secrets = os.path.join(out_dir, "configs_secrets_report.json")

    try:
        with open(out_redacted, "w", encoding="utf-8") as fo:
            json.dump(redacted_configs, fo, indent=2, ensure_ascii=False)
        with open(out_secrets, "w", encoding="utf-8") as fo:
            json.dump(secrets_report, fo, indent=2, ensure_ascii=False)
        if write_raw:
            with open(out_raw, "w", encoding="utf-8") as fo:
                json.dump(raw_parsed, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote redacted configs to: %s", out_redacted)
        logger.info("Wrote secrets report to: %s", out_secrets)
        if write_raw:
            logger.info("Wrote raw parsed configs to: %s", out_raw)
    except Exception as e:
        logger.error("Failed to write outputs: %s", e)
        sys.exit(2)

    logger.info("Parsing complete. configs=%d detections=%d", len(redacted_configs), len(secrets_report))
    logger.info("Done.")

if __name__ == "__main__":
    main()
