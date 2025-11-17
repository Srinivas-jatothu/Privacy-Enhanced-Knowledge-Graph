# stepB_tika_extract.py
import os, json, logging, pathlib, time
from tika import parser
from tqdm import tqdm

# -------- CONFIG --------
MANIFEST = os.path.join(os.getcwd(), "results", "manifest.json")
OUT_DIR = os.path.join(os.getcwd(), "results", "extracted_text")
TEST_MODE = True          # set False to run on all matching files
TEST_LIMIT = 6            # number of files to process in test mode
# Optionally include extra mime types you want Tika to parse
TIKA_TARGET_MIMES = (
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document", # .docx
    "application/msword",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",      # .xlsx
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation", # .pptx
    "application/rtf",
    "text/html",
)
# ------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tika-extract")

os.makedirs(OUT_DIR, exist_ok=True)

def safe_name(relpath):
    return relpath.replace("/", "__").replace("\\", "__")

def load_manifest(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def should_process(mime):
    if not mime:
        return False
    for pattern in TIKA_TARGET_MIMES:
        if mime.startswith(pattern):
            return True
    # also accept some xml/markdown/html variants
    if mime.startswith("text/") or "xml" in mime:
        return True
    return False

def extract_file(abs_path):
    # parser.from_file will return dict with 'content' and 'metadata'
    parsed = parser.from_file(abs_path)
    content = parsed.get("content") or ""
    metadata = parsed.get("metadata") or {}
    return content, metadata

def main():
    manifest = load_manifest(MANIFEST)
    files = manifest.get("files", [])
    candidates = [f for f in files if should_process(f.get("mime"))]
    log.info("Found %d candidate documents for Tika extraction (will test %s).", len(candidates), "limited" if TEST_MODE else "all")
    if TEST_MODE:
        candidates = candidates[:TEST_LIMIT]

    processed = 0
    for entry in tqdm(candidates, desc="TikaDocs"):
        rel = entry["relpath"]
        abs_path = entry["abs_path"]
        try:
            content, meta = extract_file(abs_path)
        except Exception as e:
            log.warning("Tika failed for %s: %s", rel, e)
            continue

        txt_name = safe_name(rel) + ".txt"
        meta_name = safe_name(rel) + ".meta.json"
        txt_path = os.path.join(OUT_DIR, txt_name)
        meta_path = os.path.join(OUT_DIR, meta_name)

        try:
            # normalize and write UTF-8 text
            with open(txt_path, "w", encoding="utf-8") as fo:
                fo.write(content or "")
            with open(meta_path, "w", encoding="utf-8") as fm:
                json.dump({"manifest_entry": entry, "tika_metadata": meta}, fm, indent=2, ensure_ascii=False)
            processed += 1
            log.info("Extracted: %s -> %s (len=%d chars)", rel, txt_path, len(content or ""))
        except Exception as e:
            log.warning("Write failed for %s: %s", rel, e)
        # small sleep to be polite
        time.sleep(0.2)

    log.info("Tika extraction done. Processed: %d files. Output dir: %s", processed, OUT_DIR)

if __name__ == "__main__":
    main()
