# step3_extract_code_entities.py
import os, sys, json, logging, ast
from pathlib import Path

# ---------- CONFIG ----------
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
OUT_DIR = os.path.join(os.getcwd(), "results")
TARGET_DIRS = ["src", "dags"]   # adjust if you want other paths
MAX_FILES = None  # set to int to limit for quick tests
# ----------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("code-entities")

def iter_py_files(repo_dir, targets):
    for t in targets:
        p = Path(repo_dir) / t
        if not p.exists():
            log.info("Target path not found (skipping): %s", p)
            continue
        for fp in p.rglob("*.py"):
            yield fp

def get_signature(func_node: ast.FunctionDef):
    parts = []
    for arg in func_node.args.args:
        parts.append(arg.arg)
    # handle varargs/kw
    if func_node.args.vararg:
        parts.append("*" + func_node.args.vararg.arg)
    if func_node.args.kwarg:
        parts.append("**" + func_node.args.kwarg.arg)
    return "(" + ", ".join(parts) + ")"

def extract_from_file(path):
    try:
        src = path.read_text(encoding="utf-8")
    except Exception as e:
        log.warning("Could not read %s: %s", path, e)
        return None
    try:
        mod = ast.parse(src)
    except Exception as e:
        log.warning("AST parse failed for %s: %s", path, e)
        return None

    module_doc = ast.get_docstring(mod)
    entities = {"file": str(path.relative_to(REPO_DIR)).replace("\\","/"),
                "module_doc": module_doc,
                "functions": [], "classes": []}

    for node in mod.body:
        if isinstance(node, ast.FunctionDef):
            entities["functions"].append({
                "name": node.name,
                "sig": get_signature(node),
                "doc": ast.get_docstring(node)
            })
        elif isinstance(node, ast.ClassDef):
            methods = []
            for n in node.body:
                if isinstance(n, ast.FunctionDef):
                    methods.append({
                        "name": n.name,
                        "sig": get_signature(n),
                        "doc": ast.get_docstring(n)
                    })
            entities["classes"].append({
                "name": node.name,
                "doc": ast.get_docstring(node),
                "methods": methods
            })
    return entities

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    files = list(iter_py_files(REPO_DIR, TARGET_DIRS))
    if MAX_FILES:
        files = files[:MAX_FILES]
    log.info("Found %d Python files under %s", len(files), TARGET_DIRS)
    out = []
    for i, f in enumerate(files, start=1):
        ent = extract_from_file(f)
        if ent:
            out.append(ent)
        if i % 50 == 0:
            log.info("Processed %d files...", i)
    out_path = os.path.join(OUT_DIR, "code_entities.json")
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(out, fo, indent=2, ensure_ascii=False)
    log.info("Wrote %d code-entity entries to %s", len(out), out_path)
    # print small sample
    for e in out[:5]:
        log.info("Sample: %s  funcs=%d classes=%d", e["file"], len(e["functions"]), len(e["classes"]))

if __name__ == "__main__":
    main()
