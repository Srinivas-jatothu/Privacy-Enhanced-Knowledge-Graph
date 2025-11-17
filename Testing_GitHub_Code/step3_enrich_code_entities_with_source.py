# # step3_enrich_code_entities_with_source.py
"""
Enrich results/code_entities.json with start_line, end_line and a small source sample for each function/method.
Outputs results/code_entities_enriched.json
"""
import os, json, logging, ast
from pathlib import Path

# CONFIG
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
IN_PATH = os.path.join(os.getcwd(), "results", "code_entities.json")
OUT_PATH = os.path.join(os.getcwd(), "results", "code_entities_enriched.json")
MAX_SOURCE_LINES = 200  # limit sample lines to avoid huge output
WRITE_FULL = False      # if True, include full source (careful)
# ---------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("enrich-code")

def read_file_lines(abs_path):
    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
            return f.readlines()
    except Exception as e:
        log.warning("Failed to read file %s: %s", abs_path, e)
        return []

def main():
    if not os.path.exists(IN_PATH):
        log.error("Input code_entities not found: %s", IN_PATH)
        return
    with open(IN_PATH, "r", encoding="utf-8") as f:
        entries = json.load(f)

    enriched = []
    total_functions = 0
    total_methods = 0
    for e in entries:
        rel = e.get("file")
        abs_path = os.path.join(REPO_DIR, rel)
        lines = read_file_lines(abs_path)
        # parse AST to find node ranges (a robust way to get lineno/end_lineno)
        try:
            src_text = "".join(lines)
            module_ast = ast.parse(src_text)
        except Exception as ex:
            log.warning("AST parse failed for %s: %s", rel, ex)
            # fallback: attach no ranges
            enriched.append(e)
            continue

        # build map from function/class name -> (start, end, source)
        node_map = {}
        for node in ast.walk(module_ast):
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                name = node.name
                start = getattr(node, "lineno", None)
                end = getattr(node, "end_lineno", None)
                src_segment = None
                if start and end:
                    # convert to 0-based indices
                    segment_lines = lines[start-1:end]
                    if WRITE_FULL:
                        src_segment = "".join(segment_lines)
                    else:
                        src_segment = "".join(segment_lines[:MAX_SOURCE_LINES])
                node_map.setdefault(("func", name, start), []).append({"start": start, "end": end, "source": src_segment})
            elif isinstance(node, ast.ClassDef):
                cname = node.name
                cstart = getattr(node, "lineno", None)
                cend = getattr(node, "end_lineno", None)
                methods = []
                for n in node.body:
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        mstart = getattr(n, "lineno", None)
                        mend = getattr(n, "end_lineno", None)
                        src_segment = None
                        if mstart and mend:
                            segment_lines = lines[mstart-1:mend]
                            if WRITE_FULL:
                                src_segment = "".join(segment_lines)
                            else:
                                src_segment = "".join(segment_lines[:MAX_SOURCE_LINES])
                        methods.append({"name": n.name, "start": mstart, "end": mend, "source": src_segment})
                node_map.setdefault(("class", cname, cstart), []).append({"start": cstart, "end": cend, "methods": methods})

        # enrich functions in e by matching names + taking the first map entry
        for f in e.get("functions", []):
            fname = f.get("name")
            total_functions += 1
            # find matching key
            matched = None
            for k, vals in node_map.items():
                if k[0] == "func" and k[1] == fname:
                    matched = vals[0]
                    break
            if matched:
                f["start_line"] = matched.get("start")
                f["end_line"] = matched.get("end")
                f["source_sample"] = matched.get("source")
            else:
                f["start_line"] = None
                f["end_line"] = None
                f["source_sample"] = None

        for c in e.get("classes", []):
            for method in c.get("methods", []):
                total_methods += 1
                mname = method.get("name")
                # search in node_map class entries by class name
                matched = None
                for k, vals in node_map.items():
                    if k[0] == "class" and k[1] == c.get("name"):
                        # find method inside vals[0]["methods"]
                        for mm in vals[0].get("methods", []):
                            if mm.get("name") == mname:
                                matched = mm
                                break
                        if matched:
                            break
                if matched:
                    method["start_line"] = matched.get("start")
                    method["end_line"] = matched.get("end")
                    method["source_sample"] = matched.get("source")
                else:
                    method["start_line"] = None
                    method["end_line"] = None
                    method["source_sample"] = None

        enriched.append(e)

    # write enriched file
    with open(OUT_PATH, "w", encoding="utf-8") as fo:
        json.dump(enriched, fo, indent=2, ensure_ascii=False)

    log.info("Wrote enriched code entities to: %s", OUT_PATH)
    log.info("Total functions annotated (approx): %d  methods: %d", total_functions, total_methods)
    # show sample
    if enriched:
        sample = enriched[0]
        log.info("Sample record keys: %s", list(sample.keys()))
        if sample.get("functions"):
            log.info("Sample function entry: %s", {k: sample["functions"][0].get(k) for k in ("name","start_line","end_line")})

if __name__ == "__main__":
    main()

"""
Enrich results/code_entities.json with start_line, end_line and a small source sample for each function/method.
Outputs results/code_entities_enriched.json
"""
import os, json, logging, ast
from pathlib import Path

# CONFIG
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
IN_PATH = os.path.join(os.getcwd(), "results", "code_entities.json")
OUT_PATH = os.path.join(os.getcwd(), "results", "code_entities_enriched.json")
MAX_SOURCE_LINES = 200  # limit sample lines to avoid huge output
WRITE_FULL = False      # if True, include full source (careful)
# ---------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("enrich-code")

def read_file_lines(abs_path):
    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
            return f.readlines()
    except Exception as e:
        log.warning("Failed to read file %s: %s", abs_path, e)
        return []

def main():
    if not os.path.exists(IN_PATH):
        log.error("Input code_entities not found: %s", IN_PATH)
        return
    with open(IN_PATH, "r", encoding="utf-8") as f:
        entries = json.load(f)

    enriched = []
    total_functions = 0
    total_methods = 0
    for e in entries:
        rel = e.get("file")
        abs_path = os.path.join(REPO_DIR, rel)
        lines = read_file_lines(abs_path)
        # parse AST to find node ranges (a robust way to get lineno/end_lineno)
        try:
            src_text = "".join(lines)
            module_ast = ast.parse(src_text)
        except Exception as ex:
            log.warning("AST parse failed for %s: %s", rel, ex)
            # fallback: attach no ranges
            enriched.append(e)
            continue

        # build map from function/class name -> (start, end, source)
        node_map = {}
        for node in ast.walk(module_ast):
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                name = node.name
                start = getattr(node, "lineno", None)
                end = getattr(node, "end_lineno", None)
                src_segment = None
                if start and end:
                    # convert to 0-based indices
                    segment_lines = lines[start-1:end]
                    if WRITE_FULL:
                        src_segment = "".join(segment_lines)
                    else:
                        src_segment = "".join(segment_lines[:MAX_SOURCE_LINES])
                node_map.setdefault(("func", name, start), []).append({"start": start, "end": end, "source": src_segment})
            elif isinstance(node, ast.ClassDef):
                cname = node.name
                cstart = getattr(node, "lineno", None)
                cend = getattr(node, "end_lineno", None)
                methods = []
                for n in node.body:
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        mstart = getattr(n, "lineno", None)
                        mend = getattr(n, "end_lineno", None)
                        src_segment = None
                        if mstart and mend:
                            segment_lines = lines[mstart-1:mend]
                            if WRITE_FULL:
                                src_segment = "".join(segment_lines)
                            else:
                                src_segment = "".join(segment_lines[:MAX_SOURCE_LINES])
                        methods.append({"name": n.name, "start": mstart, "end": mend, "source": src_segment})
                node_map.setdefault(("class", cname, cstart), []).append({"start": cstart, "end": cend, "methods": methods})

        # enrich functions in e by matching names + taking the first map entry
        for f in e.get("functions", []):
            fname = f.get("name")
            total_functions += 1
            # find matching key
            matched = None
            for k, vals in node_map.items():
                if k[0] == "func" and k[1] == fname:
                    matched = vals[0]
                    break
            if matched:
                f["start_line"] = matched.get("start")
                f["end_line"] = matched.get("end")
                f["source_sample"] = matched.get("source")
            else:
                f["start_line"] = None
                f["end_line"] = None
                f["source_sample"] = None

        for c in e.get("classes", []):
            for method in c.get("methods", []):
                total_methods += 1
                mname = method.get("name")
                # search in node_map class entries by class name
                matched = None
                for k, vals in node_map.items():
                    if k[0] == "class" and k[1] == c.get("name"):
                        # find method inside vals[0]["methods"]
                        for mm in vals[0].get("methods", []):
                            if mm.get("name") == mname:
                                matched = mm
                                break
                        if matched:
                            break
                if matched:
                    method["start_line"] = matched.get("start")
                    method["end_line"] = matched.get("end")
                    method["source_sample"] = matched.get("source")
                else:
                    method["start_line"] = None
                    method["end_line"] = None
                    method["source_sample"] = None

        enriched.append(e)

    # write enriched file
    with open(OUT_PATH, "w", encoding="utf-8") as fo:
        json.dump(enriched, fo, indent=2, ensure_ascii=False)

    log.info("Wrote enriched code entities to: %s", OUT_PATH)
    log.info("Total functions annotated (approx): %d  methods: %d", total_functions, total_methods)
    # show sample
    if enriched:
        sample = enriched[0]
        log.info("Sample record keys: %s", list(sample.keys()))
        if sample.get("functions"):
            log.info("Sample function entry: %s", {k: sample["functions"][0].get(k) for k in ("name","start_line","end_line")})

if __name__ == "__main__":
    main()
