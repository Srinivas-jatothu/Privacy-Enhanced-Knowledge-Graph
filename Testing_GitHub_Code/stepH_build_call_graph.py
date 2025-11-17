"""
stepH_build_call_graph.py

Conservative static call-graph builder for Python (heuristic resolver).

Inputs (from results/ created earlier):
  - results/symbol_table.json
  - results/import_aliases.json
  - results/module_defs.json

Outputs:
  - results/call_graph.json  (list of edges)
  - console logs with counts and examples

Notes:
  - Best-effort resolution: supports simple names and attribute chains (e.g., foo(), mod.func(), obj.method()).
  - Uses import alias map and module_defs to map names to canonical symbols when possible.
  - Does NOT run type inference; dynamic resolution not attempted.
"""

import os, sys, json, ast, logging, pathlib
from typing import Any, Dict, List, Optional, Tuple

# --- config ---
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
RESULTS_DIR = os.path.join(os.getcwd(), "results")
SYMBOLS_PATH = os.path.join(RESULTS_DIR, "symbol_table.json")
ALIASES_PATH = os.path.join(RESULTS_DIR, "import_aliases.json")
MODULE_DEFS_PATH = os.path.join(RESULTS_DIR, "module_defs.json")
OUT_PATH = os.path.join(RESULTS_DIR, "call_graph.json")
GLOB_PATTERNS = ["**/*.py"]
# --------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("callgraph")

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def find_py_files(repo_dir, patterns):
    repo = pathlib.Path(repo_dir)
    files = []
    for pat in patterns:
        for p in repo.glob(pat):
            if p.is_file() and p.suffix == ".py":
                files.append(str(p.resolve()))
    files = sorted(list(dict.fromkeys(files)))
    return files

def compute_module_name(repo_dir, file_path):
    repo = pathlib.Path(repo_dir).resolve()
    path = pathlib.Path(file_path).resolve()
    try:
        rel = path.relative_to(repo)
    except Exception:
        rel = path
    parts = list(rel.parts)
    if parts[-1].endswith(".py"):
        parts[-1] = parts[-1][:-3]
        if parts[-1] == "__init__":
            parts = parts[:-1]
    return ".".join([p for p in parts if p and p != "."])

def get_attr_chain(node: ast.AST) -> Optional[List[str]]:
    """
    If node represents a dotted attribute chain or Name, return list of identifiers.
    E.g., for ast.Call(func=ast.Attribute(ast.Name('a'), 'b')) -> ['a','b'].
    Return None for complex expressions.
    """
    parts = []
    cur = node
    while True:
        if isinstance(cur, ast.Name):
            parts.insert(0, cur.id)
            return parts
        if isinstance(cur, ast.Attribute):
            parts.insert(0, cur.attr)
            cur = cur.value
            continue
        # ignore calls on subscripts, lambdas, calls, etc.
        return None

# def safe_relpath(repo: str, path: str) -> str:
#     """
#     Return a filesystem path for `path` relative to `repo` when possible,
#     otherwise return the original path; this avoids errors when paths are on
#     different drives or unavailable.
#     """
#     try:
#         return os.path.relpath(path, repo)
#     except Exception:
#         return path

def resolve_candidate_from_chain(chain: List[str], current_module: str,
                                 aliases: Dict[str, Dict[str,str]],
                                 symbol_table: Dict[str, Dict],
                                 module_defs: Dict[str, Any]) -> Optional[Tuple[str, Dict]]:
    """
    Heuristic resolver:
      - If chain length == 1: check current_module.shortname -> canonical, else global shortname matches.
      - If chain length >=2: interpret first element: if it's an alias in current module -> expand.
        e.g., chain ['pd','DataFrame','something'] -> map 'pd'->'pandas' then try 'pandas.DataFrame'
      - For chain like ['module','func'] try 'module.func' in symbol_table
    Returns (canonical_name, symbol_info) or None
    """
    if not chain:
        return None
    # direct full candidate
    cand = ".".join(chain)
    if cand in symbol_table:
        return cand, symbol_table[cand]
    # try module-qualified (if first part is module in repo)
    # try join progressively: for i from len-1 to 1, test (".".join(chain[:i]), ".".join(chain[i:]))
    # e.g., chain ['pkg','mod','func'] -> test 'pkg.mod.func'
    for i in range(1, len(chain)+1):
        maybe = ".".join(chain[:i])
        if maybe in symbol_table:
            return maybe, symbol_table[maybe]
    # if single name, try current_module.<name>
    if len(chain) == 1:
        short = chain[0]
        local_q = f"{current_module}.{short}" if current_module else short
        if local_q in symbol_table:
            return local_q, symbol_table[local_q]
        # try find any symbol with short name globally (ambiguous) -> pick first
        for k, v in symbol_table.items():
            if k.split(".")[-1] == short:
                return k, v
    # attempt alias resolution: see if first part maps in aliases of current module
    aliases_map = aliases.get(current_module, {}) if aliases else {}
    first = chain[0]
    if first in aliases_map:
        mapped = aliases_map[first]  # e.g. 'pkg.mod' or 'pkg.mod.Class'
        rest = chain[1:]
        candidate = ".".join([mapped] + rest) if rest else mapped
        if candidate in symbol_table:
            return candidate, symbol_table[candidate]
        # also try candidate shorter
        if mapped in symbol_table:
            return mapped, symbol_table[mapped]
    return None

def extract_calls_from_ast(tree: ast.AST) -> List[Tuple[ast.AST, int]]:
    """
    Return list of tuples (call_node, lineno) for all ast.Call nodes in the AST.
    """
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            lineno = getattr(node, "lineno", None)
            calls.append((node, lineno))
    return calls

def main():
    if not os.path.exists(SYMBOLS_PATH) or not os.path.exists(ALIASES_PATH) or not os.path.exists(MODULE_DEFS_PATH):
        log.error("Required inputs missing. Ensure symbol_table.json, import_aliases.json, module_defs.json exist in results/")
        sys.exit(2)

    symbol_table = load_json(SYMBOLS_PATH)
    aliases = load_json(ALIASES_PATH)
    module_defs = load_json(MODULE_DEFS_PATH)

    py_files = find_py_files(REPO_DIR, GLOB_PATTERNS)
    log.info("Discovered %d python files", len(py_files))

    edges = []
    unresolved = 0
    resolved = 0

    for p in py_files:
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                src = f.read()
            tree = ast.parse(src)
        except Exception as e:
            log.warning("AST parse failed for %s: %s", p, e)
            continue

        mod = compute_module_name(REPO_DIR, p)
        calls = extract_calls_from_ast(tree)
        if not calls:
            continue

        for call_node, lineno in calls:
            # get attribute/name chain for callee
            chain = get_attr_chain(call_node.func)
            caller_q = None
            # attempt to find the containing function or class for the caller
            parent = call_node
            caller_name = None
            caller_lineno = None
            for anc in ast.walk(tree):
                pass  # no-op; we'll find by scanning function defs with ranges instead
            # simple heuristic: locate nearest enclosing FunctionDef by lineno
            enclosing = None
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    start = getattr(node, "lineno", None)
                    end = getattr(node, "end_lineno", None)
                    if start and end and lineno and start <= lineno <= end:
                        enclosing = node
            if enclosing and isinstance(enclosing, (ast.FunctionDef, ast.AsyncFunctionDef)):
                caller_name = enclosing.name
                caller_q = f"{mod}.{caller_name}" if mod else caller_name
                caller_lineno = getattr(enclosing, "lineno", None)
            elif enclosing and isinstance(enclosing, ast.ClassDef):
                # method: try to find method containing the call
                method_enclosing = None
                for child in enclosing.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        s = getattr(child, "lineno", None)
                        e = getattr(child, "end_lineno", None)
                        if s and e and lineno and s <= lineno <= e:
                            method_enclosing = child
                            break
                if method_enclosing:
                    caller_name = f"{enclosing.name}.{method_enclosing.name}"
                    caller_q = f"{mod}.{caller_name}" if mod else caller_name
                    caller_lineno = getattr(method_enclosing, "lineno", None)
                else:
                    caller_name = enclosing.name
                    caller_q = f"{mod}.{caller_name}" if mod else caller_name
                    caller_lineno = getattr(enclosing, "lineno", None)
            else:
                # top-level call, set caller to module-level sentinel
                caller_q = f"{mod}" if mod else "<module>"
                caller_lineno = lineno

            callee_info = None
            resolved_to = None
            if chain is not None:
                resolved = resolve_candidate_from_chain(chain, mod, aliases, symbol_table, module_defs)
                if resolved:
                    resolved_to, callee_info = resolved
                    resolved = True
                    resolved_flag = True
                    resolved_to_info = callee_info
                else:
                    resolved_flag = False
                    unresolved += 1
            else:
                resolved_flag = False
                unresolved += 1

            edge = {
                "caller": caller_q,
                "caller_file": safe_relpath(REPO_DIR, p),
                "caller_lineno": caller_lineno,
                "callee_chain": ".".join(chain) if chain else None,
                "resolved": resolved_flag,
                "callee": resolved_to,
                "callee_file": callee_info.get("file") if callee_info else None,
                "callee_lineno": callee_info.get("lineno") if callee_info else None,
                "site_lineno": lineno
            }
            edges.append(edge)
            if resolved_flag:
                resolved += 1

    log.info("Call graph building complete. Edges=%d resolved=%d unresolved=%d", len(edges), resolved, unresolved)

    # write results
    try:
        with open(OUT_PATH, "w", encoding="utf-8") as fo:
            json.dump(edges, fo, indent=2, ensure_ascii=False)
        log.info("Wrote call graph to: %s", OUT_PATH)
    except Exception as e:
        log.error("Failed to write call graph: %s", e)
        sys.exit(2)

if __name__ == '__main__':
    main()
