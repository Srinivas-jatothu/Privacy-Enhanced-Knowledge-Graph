# stepA_generate_manifest.py
import os, sys, json, logging, hashlib, pathlib
from tqdm import tqdm

# Config - adjust as needed
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
OUT_DIR = os.path.join(os.getcwd(), "results")
EXTRACT_TEXT_DIR = os.path.join(OUT_DIR, "extracted_text")
MAX_TEXT_SAMPLE_BYTES = 256 * 1024  # only sample small files
SAMPLE_LINES = 300  # how many lines to save for sampled text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("manifest")

try:
    import magic
    HAVE_MAGIC = True
except Exception:
    HAVE_MAGIC = False
    log.warning("python-magic not available. MIME detection will be based on extension.")

def compute_sha256(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def is_probably_text(mime, path):
    # simple check: mime startswith text/ or common doc types
    if mime:
        if mime.startswith("text/") or any(k in mime for k in ("json","xml","markdown","html","yaml")):
            return True
        # some office files are binary blobs but will be handled by Tika later
    # fallback: small file with no NUL
    try:
        size = os.path.getsize(path)
        if size == 0:
            return True
        if size > MAX_TEXT_SAMPLE_BYTES:
            return False
        with open(path, "rb") as f:
            chunk = f.read(2048)
            if b"\0" in chunk:
                return False
            return True
    except Exception:
        return False

def safe_relpath(path):
    return os.path.relpath(path, REPO_DIR).replace("\\","/")

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(EXTRACT_TEXT_DIR, exist_ok=True)

def write_text_sample(path, rel):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = []
            for i, ln in enumerate(f):
                if i >= SAMPLE_LINES:
                    break
                lines.append(ln.rstrip("\n"))
        out_name = rel.replace("/", "__")
        out_path = os.path.join(EXTRACT_TEXT_DIR, out_name + ".txt")
        with open(out_path, "w", encoding="utf-8") as fo:
            fo.write("\n".join(lines))
        return out_path
    except Exception as e:
        log.debug("Could not write text sample for %s: %s", path, e)
        return None

def main():
    ensure_dirs()
    if not os.path.isdir(REPO_DIR):
        log.error("Repo dir not found: %s", REPO_DIR)
        sys.exit(2)

    manifest = []
    mime_counts = {}
    total = 0
    # walk
    all_paths = []
    for root, dirs, files in os.walk(REPO_DIR):
        for fn in files:
            all_paths.append(os.path.join(root, fn))
    log.info("Will scan %d files under %s", len(all_paths), REPO_DIR)

    for p in tqdm(all_paths, desc="Files"):
        total += 1
        rel = safe_relpath(p)
        try:
            size = os.path.getsize(p)
            mtime = os.path.getmtime(p)
        except Exception as e:
            log.warning("Skipping %s: %s", p, e)
            continue

        mime = None
        if HAVE_MAGIC:
            try:
                mime = magic.from_file(p, mime=True)
            except Exception as e:
                log.debug("magic fail for %s: %s", p, e)
                mime = None

        # fallback based on extension
        if not mime:
            ext = pathlib.Path(p).suffix.lower()
            if ext in (".md", ".txt", ".py", ".yaml", ".yml", ".json", ".xml", ".html"):
                mime = "text/" + ext.lstrip(".")
            elif ext in (".pdf",):
                mime = "application/pdf"
            elif ext in (".docx", ".doc"):
                mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            else:
                mime = "application/octet-stream"

        is_text = is_probably_text(mime, p)
        sha256 = compute_sha256(p)

        if mime:
            mime_counts[mime] = mime_counts.get(mime, 0) + 1

        sample_path = None
        if is_text and size <= MAX_TEXT_SAMPLE_BYTES:
            sample_path = write_text_sample(p, rel)

        manifest_entry = {
            "relpath": rel,
            "abs_path": p,
            "size": size,
            "mtime": mtime,
            "mime": mime,
            "is_text": bool(is_text),
            "sha256": sha256,
            "sample_path": sample_path
        }
        manifest.append(manifest_entry)

    out_manifest = os.path.join(OUT_DIR, "manifest.json")
    with open(out_manifest, "w", encoding="utf-8") as f:
        json.dump({"repo": os.path.basename(REPO_DIR), "files": manifest, "mime_counts": mime_counts}, f, indent=2, ensure_ascii=False)

    log.info("Wrote manifest: %s  (entries: %d)", out_manifest, len(manifest))
    log.info("Top MIME types (sample): %s", dict(list(mime_counts.items())[:10]))

if __name__ == "__main__":
    main()
