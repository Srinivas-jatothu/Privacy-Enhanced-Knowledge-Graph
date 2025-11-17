# step2_index_repo_files.py
import os, sys, json, logging, pathlib, io

# ---------- CONFIG ----------
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
OUT_DIR = os.path.join(os.getcwd(), "results")
SAMPLE_DIR = os.path.join(OUT_DIR, "Github_Files_Samples")
MAX_SAMPLE_BYTES = 256 * 1024   # only sample files <= 256 KB
SAMPLE_LINES = 10
# ----------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("indexer")

EXT_CATEGORY = {
    # code
    ".py": "code", ".ipynb": "code", ".js": "code", ".ts": "code", ".java": "code",
    ".cpp": "code", ".c": "code", ".cs": "code", ".go": "code", ".rb": "code",
    # docs
    ".md": "doc", ".rst": "doc", ".txt": "doc",
    # config
    ".yml": "config", ".yaml": "config", ".json": "config", ".ini": "config",
    ".cfg": "config", "Dockerfile": "config",
    # data
    ".csv": "data", ".tsv": "data", ".parquet": "data", ".xlsx": "data",
    # binary (common)
    ".png": "binary", ".jpg": "binary", ".jpeg": "binary", ".gif": "binary",
    ".zip": "binary", ".tar": "binary", ".gz": "binary"
}

def categorize(path):
    name = os.path.basename(path)
    ext = pathlib.Path(name).suffix.lower()
    if name == "Dockerfile":
        return "config", ""
    if ext in EXT_CATEGORY:
        return EXT_CATEGORY[ext], ext
    if ext == "":
        # no extension: could still be scripts (Makefile, etc.)
        if name.lower() in ("makefile", "procfile"):
            return "config", ""
        return "other", ""
    return "other", ext

def is_text_file(path):
    # Simple heuristic: try to open in text mode and read small chunk
    try:
        with open(path, "rb") as f:
            sample = f.read(1024)
        if b"\0" in sample:
            return False
        return True
    except Exception:
        return False

def safe_read_sample(path, max_bytes=MAX_SAMPLE_BYTES, max_lines=SAMPLE_LINES):
    try:
        size = os.path.getsize(path)
        if size > max_bytes:
            return None, size
        if not is_text_file(path):
            return None, size
        lines = []
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for i, ln in enumerate(f):
                if i >= max_lines:
                    break
                lines.append(ln.rstrip("\n"))
        return "\n".join(lines), size
    except Exception as e:
        log.warning("Failed to read sample for %s: %s", path, e)
        return None, None

def main():
    log.info("Repo dir: %s", REPO_DIR)
    if not os.path.isdir(REPO_DIR):
        log.error("Repo directory not found: %s", REPO_DIR)
        sys.exit(2)

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    file_index = []
    counts = {}
    total_files = 0

    for root, dirs, files in os.walk(REPO_DIR):
        for fname in files:
            total_files += 1
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, REPO_DIR)
            cat, ext = categorize(fname)
            size = os.path.getsize(fpath)
            mtime = os.path.getmtime(fpath)
            sample_text, sample_size = safe_read_sample(fpath)
            entry = {
                "relpath": rel.replace("\\", "/"),
                "abs_path": fpath,
                "category": cat,
                "ext": ext,
                "size": size,
                "mtime": mtime,
                "sample_taken": bool(sample_text)
            }
            file_index.append(entry)
            counts[cat] = counts.get(cat, 0) + 1

            # write sample file if we have sample text
            if sample_text:
                safe_name = rel.replace(os.sep, "__").replace("/", "__")
                out_sample = os.path.join(SAMPLE_DIR, safe_name + ".sample.txt")
                try:
                    with open(out_sample, "w", encoding="utf-8") as sf:
                        sf.write(sample_text)
                except Exception as e:
                    log.warning("Could not write sample for %s: %s", rel, e)

            if total_files % 200 == 0:
                log.info("Scanned %d files so far...", total_files)

    # write index
    out_index = os.path.join(OUT_DIR, "file_index.json")
    with open(out_index, "w", encoding="utf-8") as f:
        json.dump({"repo": os.path.basename(REPO_DIR), "total_files": total_files, "counts": counts, "files": file_index}, f, indent=2, ensure_ascii=False)

    log.info("DONE scanning. Total files: %d", total_files)
    log.info("Counts by category: %s", counts)
    log.info("Wrote index to: %s", out_index)
    log.info("Sample files stored in: %s (if any)", SAMPLE_DIR)

if __name__ == "__main__":
    main()
