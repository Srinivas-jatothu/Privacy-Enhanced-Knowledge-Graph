# stepD_merge_artifacts_into_manifest.py
import os, sys, json, logging, hashlib, pathlib
from tqdm import tqdm

# Paths (adjust if you keep different layout)
BASE = os.getcwd()
MANIFEST = os.path.join(BASE, "results", "manifest.json")
ARTIFACTS_INDEX = os.path.join(BASE, "results", "artifacts_index.json")
ARTIFACTS_ROOT = os.path.join(BASE, "results", "artifacts")
OUT_MANIFEST = os.path.join(BASE, "results", "manifest_enriched.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("merge-artifacts")

try:
    import magic
    HAVE_MAGIC = True
except Exception:
    HAVE_MAGIC = False
    log.warning("python-magic not available; falling back to extension-based mime detection.")

def compute_sha256(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def detect_mime(path):
    if HAVE_MAGIC:
        try:
            return magic.from_file(path, mime=True)
        except Exception as e:
            log.debug("magic failed for %s: %s", path, e)
    # fallback by extension
    ext = pathlib.Path(path).suffix.lower()
    if ext in (".md", ".txt", ".py", ".yaml", ".yml", ".json", ".xml", ".html"):
        return "text/" + ext.lstrip(".")
    if ext == ".pdf":
        return "application/pdf"
    if ext in (".png", ".jpg", ".jpeg"):
        return "image/" + ext.lstrip(".")
    return "application/octet-stream"

def is_probably_text(mime, path, max_bytes=256*1024):
    if mime:
        if mime.startswith("text/") or any(k in mime for k in ("json","xml","markdown","html","yaml")):
            return True
    try:
        size = os.path.getsize(path)
        if size == 0:
            return True
        if size > max_bytes:
            return False
        with open(path, "rb") as f:
            chunk = f.read(2048)
            return b"\0" not in chunk
    except Exception:
        return False

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    if not os.path.exists(MANIFEST):
        log.error("Manifest not found at: %s", MANIFEST)
        sys.exit(2)
    if not os.path.exists(ARTIFACTS_INDEX):
        log.error("Artifacts index not found at: %s", ARTIFACTS_INDEX)
        sys.exit(2)

    manifest = load_json(MANIFEST)
    existing = {entry["relpath"]: entry for entry in manifest.get("files", [])}
    artifacts = load_json(ARTIFACTS_INDEX).get("archives", {})

    new_entries = []
    total_new = 0
    for archive_rel, extracted_list in artifacts.items():
        if isinstance(extracted_list, dict) and "error" in extracted_list:
            log.warning("Archive %s extraction error: %s", archive_rel, extracted_list["error"])
            continue
        for extracted_rel in extracted_list:
            # absolute path under results/artifacts
            abs_path = os.path.join(ARTIFACTS_ROOT, extracted_rel)
            # ensure file exists
            if not os.path.exists(abs_path):
                log.warning("Extracted file missing: %s", abs_path)
                continue
            # compute relpath relative to repo root? we will store under artifacts/...
            relpath = os.path.join("artifacts", extracted_rel).replace("\\","/")
            if relpath in existing:
                continue
            # detect mime, sha, is_text
            mime = detect_mime(abs_path)
            sha = compute_sha256(abs_path)
            is_text = is_probably_text(mime, abs_path)
            entry = {
                "relpath": relpath,
                "abs_path": abs_path,
                "size": os.path.getsize(abs_path),
                "mtime": os.path.getmtime(abs_path),
                "mime": mime,
                "is_text": bool(is_text),
                "sha256": sha,
                "source_archive": archive_rel
            }
            manifest.setdefault("files", []).append(entry)
            existing[relpath] = entry
            new_entries.append(entry)
            total_new += 1

    # write enriched manifest
    with open(OUT_MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    log.info("Processed artifacts: %d archives, added %d new extracted files to manifest.", len(artifacts), total_new)
    if total_new > 0:
        log.info("Wrote enriched manifest to: %s", OUT_MANIFEST)
    else:
        log.info("No new extracted files to add. Enriched manifest still written to: %s", OUT_MANIFEST)

if __name__ == "__main__":
    main()
