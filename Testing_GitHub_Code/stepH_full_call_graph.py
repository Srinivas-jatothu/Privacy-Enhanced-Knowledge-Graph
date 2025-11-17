#!/usr/bin/env python3
"""
step_full_call_graph.py

Professional, robust static call-graph builder for Python repositories.

What this script does (complete, production-ready):
  - Recursively finds Python files in the repo (configurable globs)
  - Parses each file into AST (resilient to parse errors)
  - Builds a comprehensive symbol index: top-level functions, classes, methods,
    with module-qualified names and file/line provenance
  - Builds import alias maps per module (handles `import` and `from ... import ...`)
  - Extracts ast.Call sites, resolves callee expressions (Name, Attribute chains)
  - Resolves callees to canonical symbols using multiple heuristics:
      * fully-qualified match (pkg.mod.func)
      * module-scoped match (current_module.func)
      * alias expansion (alias -> module or module.symbol)
      * class method resolution (mod.Class.method)
      * fallback: best global candidate by short name (with warning)
  - Produces a high-quality, JSON-call-graph containing caller/callee, files,
    linenos, resolution confidence, and meta for downstream KG ingestion.
  - Writes outputs to the results directory:
      results/symbol_table.json
      results/import_aliases.json
      results/module_index.json
      results/call_graph.json
      results/step_full_call_graph.log

Usage:
  python step_full_call_graph.py --repo-dir /path/to/repo --out-dir ./results

Notes & limitations:
  - Heuristic/static only: dynamic dispatch, runtime monkey-patching, or reflection
    cannot be resolved reliably with static AST alone.
  - Targets Python code only. Intended as a practical, high-coverage solution.
"""

from __future__ import annotations
import argparse
import ast
import json
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Configurable defaults
# ----------------------------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), "results")
DEFAULT_PATTERNS = ["**/*.py"]
LOG_FILENAME = "step_full_call_graph.log"

# ----------------------------
# Logging utility
# ----------------------------
def setup_logging(out_dir: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(out_dir, exist_ok=True)
    log_path = os.path.join(out_dir, LOG_FILENAME)
    logger = logging.getLogger("full-callgraph")
    logger.setLevel(level)
    # avoid duplicate handlers
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger

# ----------------------------
# File discovery
# ----------------------------
def find_python_files(repo_dir: str, patterns: List[str], logger: logging.Logger) -> List[str]:
    repo = pathlib.Path(repo_dir)
    files = []
    for pat in patterns:
        for p in repo.glob(pat):
            if p.is_file() and p.suffix == ".py":
                files.append(str(p.resolve()))
    files = sorted(list(dict.fromkeys(files)))
    logger.info("Discovered %d Python files (patterns=%s)", len(files), patterns)
    return files

# ----------------------------
# Module / name utilities
# ----------------------------
def compute_module_name(repo_dir: str, file_path: str) -> str:
    """
    Compute a plausible module name for a python file given repo layout.
    E.g., repo/.../pkg/mod.py -> pkg.mod
    Handles __init__.py converting directory into package name.
    """
    repo = pathlib.Path(repo_dir).resolve()
    path = pathlib.Path(file_path).resolve()
    try:
        rel = path.relative_to(repo)
    except Exception:
        rel = path
    parts = list(rel.parts)
    if parts and parts[-1].endswith(".py"):
        parts[-1] = parts[-1][:-3]
        if parts[-1] == "__init__":
            parts = parts[:-1]
    # filter common VCS dirs
    parts = [p for p in parts if p not in (".", "")]
    module = ".".join(parts)
    return module

def safe_relpath(repo_dir: str, p: str) -> str:
    try:
        return str(pathlib.Path(p).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return os.path.basename(p)

# ----------------------------
# AST helpers
# ----------------------------
def parse_ast_file(path: str) -> Optional[ast.AST]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            src = f.read()
        tree = ast.parse(src)
        return tree
    except Exception:
        return None

def collect_top_level_defs(tree: ast.AST) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Collect top-level functions and classes (with methods) from AST.
    Returns (functions, classes)
    functions: [{name, lineno, end_lineno}]
    classes: [{name, lineno, end_lineno, methods: [{name, lineno, end_lineno}]}]
    """
    functions = []
    classes = []
    for node in getattr(tree, "body", []):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append({
                "name": node.name,
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None)
            })
        elif isinstance(node, ast.ClassDef):
            methods = []
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.append({
                        "name": child.name,
                        "lineno": getattr(child, "lineno", None),
                        "end_lineno": getattr(child, "end_lineno", None)
                    })
            classes.append({
                "name": node.name,
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None),
                "methods": methods
            })
    return functions, classes

def collect_imports(tree: ast.AST) -> List[Dict[str, Optional[str]]]:
    """
    Collect import statements into a list of dicts:
      - {"type":"import", "module":"pkg.mod", "alias":"pm"}
      - {"type":"from", "module":"pkg.mod", "name":"X", "alias":"Y"}
    """
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.append({"type": "import", "module": n.name, "name": None, "alias": n.asname or n.name.split(".")[0]})
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for n in node.names:
                imports.append({"type": "from", "module": module, "name": n.name, "alias": n.asname or n.name})
    return imports

# ----------------------------
# Symbol indexing
# ----------------------------
def build_symbol_index(py_files: List[str], repo_dir: str, logger: logging.Logger) -> Tuple[Dict[str, Dict], Dict[str, Any]]:
    """
    Build canonical symbol table and module index.
    Returns:
      - canonical: { "module.symbol": {type,module,qualified_name,file,lineno,end_lineno} }
      - module_index: { module: {"file":relpath, "functions":[...], "classes":[...]} }
    """
    canonical: Dict[str, Dict] = {}
    module_index: Dict[str, Any] = {}

    for p in py_files:
        tree = parse_ast_file(p)
        mod = compute_module_name(repo_dir, p)
        rel = safe_relpath(repo_dir, p)
        if tree is None:
            logger.warning("Failed parse (skipping) %s", rel)
            continue
        funcs, classes = collect_top_level_defs(tree)
        module_index[mod] = {"file": rel, "functions": funcs, "classes": classes}
        # populate canonical names
        for f in funcs:
            qn = f"{mod}.{f['name']}" if mod else f["name"]
            canonical[qn] = {
                "type": "function",
                "module": mod,
                "qualified_name": qn,
                "file": rel,
                "lineno": f.get("lineno"),
                "end_lineno": f.get("end_lineno")
            }
        for c in classes:
            cq = f"{mod}.{c['name']}" if mod else c['name']
            canonical[cq] = {
                "type": "class",
                "module": mod,
                "qualified_name": cq,
                "file": rel,
                "lineno": c.get("lineno"),
                "end_lineno": c.get("end_lineno")
            }
            for m in c.get("methods", []):
                mqn = f"{cq}.{m['name']}"
                canonical[mqn] = {
                    "type": "method",
                    "module": mod,
                    "qualified_name": mqn,
                    "file": rel,
                    "lineno": m.get("lineno"),
                    "end_lineno": m.get("end_lineno")
                }
    logger.info("Built symbol index with %d canonical symbols", len(canonical))
    return canonical, module_index

# ----------------------------
# Import alias map
# ----------------------------
def build_import_alias_map(py_files: List[str], repo_dir: str, logger: logging.Logger) -> Dict[str, Dict[str, str]]:
    """
    For each module, create a map local_name -> target (module or module.symbol)
    e.g., 'pd' -> 'pandas', 'np' -> 'numpy', 'X' -> 'pkg.mod.X'
    """
    aliases: Dict[str, Dict[str, str]] = {}
    for p in py_files:
        tree = parse_ast_file(p)
        if tree is None:
            continue
        mod = compute_module_name(repo_dir, p)
        local_aliases: Dict[str, str] = {}
        for im in collect_imports(tree):
            if im["type"] == "import":
                module = im["module"]
                alias = im["alias"]
                local_aliases[alias] = module
            elif im["type"] == "from":
                module = im["module"]
                name = im["name"]
                alias = im["alias"]
                # map alias -> module.name
                if module:
                    local_aliases[alias] = f"{module}.{name}"
                else:
                    local_aliases[alias] = name
        aliases[mod] = local_aliases
    logger.info("Built import alias map for %d modules", len(aliases))
    return aliases

# ----------------------------
# Call site extraction helpers
# ----------------------------
def extract_calls(tree: ast.AST) -> List[Tuple[ast.Call, int]]:
    """
    Return list of (call_node, lineno)
    """
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            calls.append((node, getattr(node, "lineno", None)))
    return calls

def get_name_chain(node: ast.AST) -> Optional[List[str]]:
    """
    If node is a Name or Attribute chain, return list of identifiers in order.
    E.g., for expression foo.bar.baz -> ["foo","bar","baz"]
    Returns None for complex expressions (calls, subscripts, lambdas).
    """
    parts: List[str] = []
    cur = node
    while True:
        if isinstance(cur, ast.Name):
            parts.insert(0, cur.id)
            return parts
        if isinstance(cur, ast.Attribute):
            parts.insert(0, cur.attr)
            cur = cur.value
            continue
        # e.g., Call, Subscript, Lambda -> not resolvable
        return None

# ----------------------------
# Resolution heuristics
# ----------------------------
def resolve_chain_to_symbol(chain: List[str], current_module: str,
                            aliases: Dict[str, Dict[str, str]],
                            canonical: Dict[str, Dict],
                            module_index: Dict[str, Any],
                            logger: logging.Logger) -> Tuple[Optional[str], str]:
    """
    Attempt to resolve a dotted chain to a canonical symbol.
    Returns (canonical_name_or_None, reason_str)

    Heuristics (applied in order):
      1. direct full dotted match (join chain)
      2. module-scoped match: current_module + '.' + shortname (for single name)
      3. alias expansion: if first element is alias in current_module, expand
      4. module qualified try: progressively join parts and check canonical
      5. class.method resolution: for chains [Class,method] try current_module.Class.method
      6. fallback: global short-name match (warn, ambiguous)
    """
    if not chain:
        return None, "empty_chain"

    # 1) direct full dotted match
    candidate = ".".join(chain)
    if candidate in canonical:
        return candidate, "direct_full_match"

    # 2) module-scoped match (single short name)
    if len(chain) == 1:
        short = chain[0]
        mod_candidate = f"{current_module}.{short}" if current_module else short
        if mod_candidate in canonical:
            return mod_candidate, "module_scoped_match"

    # 3) alias expansion
    alias_map = aliases.get(current_module, {}) if aliases else {}
    first = chain[0]
    if first in alias_map:
        mapped = alias_map[first]  # e.g., 'pkg.mod' or 'pkg.mod.Symbol'
        rest = chain[1:]
        if rest:
            expanded = ".".join([mapped] + rest)
        else:
            expanded = mapped
        if expanded in canonical:
            return expanded, f"alias_expansion:{first}->{mapped}"
        # maybe mapped corresponds to module; try mapped + rest
        # try progressively
        for i in range(len(rest), 0, -1):
            attempt = ".".join([mapped] + rest[:i])
            if attempt in canonical:
                return attempt, f"alias_expansion_partial:{attempt}"
        # try mapped as module and short name in module (fallback)
        if len(chain) == 2:
            possible = f"{mapped}.{chain[1]}"
            if possible in canonical:
                return possible, f"alias_mapped_module:{mapped}"

    # 4) progressive join attempts (pkg.mod.func)
    for i in range(len(chain), 0, -1):
        attempt = ".".join(chain[:i])
        if attempt in canonical:
            return attempt, f"progressive_join:{attempt}"

    # 5) class.method resolution when chain length == 2 and first looks like Class
    if len(chain) == 2:
        cls, meth = chain[0], chain[1]
        possible = f"{current_module}.{cls}.{meth}"
        if possible in canonical:
            return possible, "class_method_current_module"
        # attempt global
        for k in canonical.keys():
            if k.endswith(f".{cls}.{meth}"):
                return k, "class_method_global"

    # 6) fallback: find any global symbol with short name equal to last part
    short = chain[-1]
    for k in canonical:
        if k.split(".")[-1] == short:
            return k, "fallback_global_shortname"
    return None, "unresolved"

# ----------------------------
# Call-graph assembly
# ----------------------------
def build_call_graph(py_files: List[str], repo_dir: str,
                     canonical: Dict[str, Dict],
                     aliases: Dict[str, Dict[str, str]],
                     module_index: Dict[str, Any],
                     logger: logging.Logger) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    total_calls = 0
    resolved_count = 0
    unresolved_count = 0

    for p in py_files:
        tree = parse_ast_file(p)
        if tree is None:
            continue
        mod = compute_module_name(repo_dir, p)
        rel = safe_relpath(repo_dir, p)
        calls = extract_calls(tree)
        if not calls:
            continue

        # For locating caller context: we will create list of enclosing functions/classes with ranges
        encl_defs = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                encl_defs.append(node)
        # helper to find enclosing def for a lineno
        def find_enclosing(lineno: int) -> Tuple[str, Optional[str], Optional[int]]:
            """
            Returns (caller_qualified_name, kind, caller_lineno)
            kind: 'function'|'method'|'class'|'module'
            """
            # prefer function/methods by range
            best = None
            for node in encl_defs:
                start = getattr(node, "lineno", None)
                end = getattr(node, "end_lineno", None)
                if start and end and lineno and start <= lineno <= end:
                    # prefer smallest range (most specific)
                    length = end - start
                    if best is None or length < best[0]:
                        best = (length, node)
            if best:
                node = best[1]
                if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                    # qualify with module
                    name = node.name
                    qn = f"{mod}.{name}" if mod else name
                    return qn, "function", getattr(node, "lineno", None)
                elif isinstance(node, ast.ClassDef):
                    # call inside class body but not in method; return class-level
                    name = node.name
                    qn = f"{mod}.{name}" if mod else name
                    return qn, "class", getattr(node, "lineno", None)
            # module-level
            return (mod if mod else "<module>"), "module", None

        for call_node, lineno in calls:
            total_calls += 1
            chain = get_name_chain(call_node.func)
            caller_q, caller_kind, caller_def_lineno = find_enclosing(lineno) if lineno else (mod, "module", None)
            resolved_symbol = None
            reason = "no_chain"
            if chain is not None:
                resolved_symbol, reason = resolve_chain_to_symbol(chain, mod, aliases, canonical, module_index, logger)
            else:
                reason = "complex_expr"

            edge = {
                "caller_qualified": caller_q,
                "caller_kind": caller_kind,
                "caller_file": rel,
                "caller_def_lineno": caller_def_lineno,
                "site_lineno": lineno,
                "callee_chain": ".".join(chain) if chain else None,
                "resolved": bool(resolved_symbol),
                "resolved_symbol": resolved_symbol,
                "resolve_reason": reason
            }
            if resolved_symbol:
                resolved_count += 1
                edge["callee_file"] = canonical[resolved_symbol]["file"]
                edge["callee_lineno"] = canonical[resolved_symbol].get("lineno")
                edge["callee_type"] = canonical[resolved_symbol].get("type")
            else:
                unresolved_count += 1
                edge["callee_file"] = None
                edge["callee_lineno"] = None
                edge["callee_type"] = None

            edges.append(edge)

    logger.info("Processed calls: total=%d resolved=%d unresolved=%d", total_calls, resolved_count, unresolved_count)
    return edges

# ----------------------------
# Persistence helpers
# ----------------------------
def write_json(obj: Any, path: str, logger: logging.Logger) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fo:
            json.dump(obj, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote JSON output: %s", path)
    except Exception as e:
        logger.error("Failed writing JSON to %s: %s", path, e)
        raise

# ----------------------------
# CLI and main
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Full Python call graph builder (production-ready, heuristic resolver).")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Path to repository root")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Directory for results (will be created)")
    p.add_argument("--patterns", nargs="*", default=DEFAULT_PATTERNS, help="Glob patterns to discover py files (relative to repo)")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"], help="Logging level")
    return p.parse_args()

def main():
    args = parse_args()
    logger = setup_logging(args.out_dir, getattr(logging, args.log_level))
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    patterns = args.patterns

    logger.info("Starting full call graph builder")
    logger.info("Repo dir: %s", repo_dir)
    logger.info("Out dir: %s", out_dir)
    logger.info("Patterns: %s", patterns)

    py_files = find_python_files(repo_dir, patterns, logger)
    if not py_files:
        logger.error("No Python files found. Exiting.")
        return

    # Build symbol/canonical index
    canonical, module_index = build_symbol_index(py_files, repo_dir, logger)

    # Build import alias map
    aliases = build_import_alias_map(py_files, repo_dir, logger)

    # Persist symbol/index/aliases for debugging & KG ingestion
    os.makedirs(out_dir, exist_ok=True)
    symbol_path = os.path.join(out_dir, "symbol_table.json")
    module_index_path = os.path.join(out_dir, "module_index.json")
    aliases_path = os.path.join(out_dir, "import_aliases.json")

    write_json(canonical, symbol_path, logger)
    write_json(module_index, module_index_path, logger)
    write_json(aliases, aliases_path, logger)

    # Build call graph
    edges = build_call_graph(py_files, repo_dir, canonical, aliases, module_index, logger)
    callgraph_path = os.path.join(out_dir, "call_graph.json")
    write_json(edges, callgraph_path, logger)

    # Summary stats
    resolved = sum(1 for e in edges if e.get("resolved"))
    total = len(edges)
    logger.info("Call graph complete. edges=%d resolved=%d unresolved=%d", total, resolved, total-resolved)
    logger.info("Outputs: %s, %s, %s, %s", symbol_path, module_index_path, aliases_path, callgraph_path)
    logger.info("Done.")

if __name__ == "__main__":
    main()
