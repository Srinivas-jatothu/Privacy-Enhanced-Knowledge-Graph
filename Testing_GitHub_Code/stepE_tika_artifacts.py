# stepE_tika_artifacts.py
import os, json, logging, time
from tika import parser
from tqdm import tqdm

BASE = os.getcwd()
ENRICHED_MANIFEST = os.path.join(BASE, "results", "manifest_enriched.json")
OUT_DIR = os.path.join(BASE, "results", "extracted_text")
TEST_MODE = False        # set True to limit processing for quick test
TEST_LIMIT = 10

# candidate MIME prefixes to feed to Tika (we'll also process many files even if not matching)
TIKA_TARGET_MIMES = (
    "application/pdf",
    "application/vnd.openxmlformats-officedocument",
    "application/msword",
    "text/",
    "application/xml",
    "application/rtf",
    "application/epub+zip",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tika-artifacts")

def safe_name(relpath):
    return relpath.replace("/", "__").replace("\\", "__")

def load_manifest(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def should_tika(entry):
    # process artifact entries or any with a source_archive
    rel = entry.get("relpath","")
    if rel.startswith("artifacts/") or entry.get("source_archive"):
        return True
    # otherwise check mime hint
    mime = entry.get("mime","") or ""
    for p in TIKA_TARGET_MIMES:
        if mime.startswith(p):
            return True
    return False

def extract_to_files(abs_path, rel):
    try:
        parsed = parser.from_file(abs_path)
    except Exception as e:
        log.warning("Tika parse failed for %s : %s", rel, e)
        return False, None
    content = parsed.get("content") or ""
    meta = parsed.get("metadata") or {}
    txt_name = safe_name(rel) + ".txt"
    meta_name = safe_name(rel) + ".meta.json"
    txt_path = os.path.join(OUT_DIR, txt_name)
    meta_path = os.path.join(OUT_DIR, meta_name)
    try:
        with open(txt_path, "w", encoding="utf-8") as fo:
            fo.write(content)
        with open(meta_path, "w", encoding="utf-8") as fm:
            json.dump({"relpath": rel, "manifest_meta": entry, "tika_meta": meta}, fm, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("Write failed for %s : %s", rel, e)
        return False, None
    return True, txt_path

if __name__ == "__main__":
    if not os.path.exists(ENRICHED_MANIFEST):
        log.error("Enriched manifest not found: %s", ENRICHED_MANIFEST)
        raise SystemExit(2)
    os.makedirs(OUT_DIR, exist_ok=True)
    manifest = load_manifest(ENRICHED_MANIFEST)
    files = manifest.get("files", [])

    candidates = [f for f in files if should_tika(f)]
    log.info("Artifact candidates for Tika: %d (TEST_MODE=%s)", len(candidates), TEST_MODE)
    if TEST_MODE:
        candidates = candidates[:TEST_LIMIT]

    processed = 0
    successes = 0
    failures = 0
    # iterate
    for entry in tqdm(candidates, desc="TikaArtifacts"):
        rel = entry.get("relpath")
        abs_path = entry.get("abs_path")
        processed += 1
        if not os.path.exists(abs_path):
            log.warning("Missing file, skipping: %s", abs_path)
            failures += 1
            continue
        ok, txt_path = extract_to_files(abs_path, rel)
        if ok:
            successes += 1
            log.info("Extracted artifact: %s -> %s (chars=%d)", rel, txt_path, os.path.getsize(txt_path) if txt_path else 0)
        else:
            failures += 1
        time.sleep(0.15)

    log.info("Tika on artifacts complete: processed=%d successes=%d failures=%d", processed, successes, failures)
    if successes:
        # show a preview of one successful extraction
        try:
            sample_txt = open(txt_path, "r", encoding="utf-8", errors="replace").read(400)
            log.info("Preview (first 400 chars) of last extracted file:\n%s", sample_txt[:400].replace("\n","\n"))
        except Exception:
            pass
