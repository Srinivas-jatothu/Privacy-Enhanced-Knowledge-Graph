# stepC_extract_archives.py
"""
Extract archives (.zip, .jar, .war, .tar, .tar.gz, .tgz, .apk) into results/artifacts/
Writes results/artifacts_index.json with mapping archive -> extracted files.
Logs progress and errors.
"""
import os, sys, json, logging, shutil, pathlib, zipfile, tarfile
from tqdm import tqdm

# CONFIG
REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
OUT_ARTIFACTS = os.path.join(os.getcwd(), "results", "artifacts")
ARTIFACTS_INDEX = os.path.join(os.getcwd(), "results", "artifacts_index.json")

# archive extensions we handle
ZIP_LIKE = {".zip", ".jar", ".war", ".apk"}
TAR_LIKE = {".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("archives")

def is_archive(path):
    ext = pathlib.Path(path).suffix.lower()
    if ext in ZIP_LIKE or ext in TAR_LIKE:
        return True
    # double ext (e.g., .tar.gz)
    name = os.path.basename(path).lower()
    for t in TAR_LIKE:
        if name.endswith(t):
            return True
    return False

def safe_name(rel):
    return rel.replace("/", "__").replace("\\", "__")

def extract_zip(src_path, dest_dir):
    try:
        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(dest_dir)
        return True, None
    except Exception as e:
        return False, str(e)

def extract_tar(src_path, dest_dir):
    try:
        with tarfile.open(src_path, "r:*") as t:
            t.extractall(dest_dir)
        return True, None
    except Exception as e:
        return False, str(e)

def main():
    if not os.path.isdir(REPO_DIR):
        log.error("Repo dir not found: %s", REPO_DIR)
        sys.exit(2)
    os.makedirs(OUT_ARTIFACTS, exist_ok=True)

    # find archives (walk repo)
    archives = []
    for root, dirs, files in os.walk(REPO_DIR):
        for fn in files:
            p = os.path.join(root, fn)
            if is_archive(p):
                rel = os.path.relpath(p, REPO_DIR).replace("\\", "/")
                archives.append((rel, p))
    log.info("Found %d archive files to process.", len(archives))
    artifacts_index = {}

    for rel, abs_path in tqdm(archives, desc="Archives"):
        safe_dir = safe_name(rel)
        dest = os.path.join(OUT_ARTIFACTS, safe_dir)
        os.makedirs(dest, exist_ok=True)
        ext = pathlib.Path(abs_path).suffix.lower()

        ok = False
        err = None
        if ext in ZIP_LIKE or any(rel.lower().endswith(x) for x in ZIP_LIKE):
            ok, err = extract_zip(abs_path, dest)
        elif ext in TAR_LIKE or any(rel.lower().endswith(x) for x in TAR_LIKE):
            ok, err = extract_tar(abs_path, dest)
        else:
            # try both safely
            ok, err = extract_zip(abs_path, dest)
            if not ok:
                ok, err = extract_tar(abs_path, dest)

        extracted = []
        if ok:
            # list extracted files (relative to OUT_ARTIFACTS)
            for root, dirs, files in os.walk(dest):
                for f in files:
                    extracted.append(os.path.relpath(os.path.join(root, f), OUT_ARTIFACTS).replace("\\","/"))
            artifacts_index[rel] = extracted
            log.info("Extracted %d items from %s -> %s", len(extracted), rel, dest)
        else:
            artifacts_index[rel] = {"error": err}
            log.warning("Failed to extract %s : %s", rel, err)

    # write index
    with open(ARTIFACTS_INDEX, "w", encoding="utf-8") as fo:
        json.dump({"repo": os.path.basename(REPO_DIR), "archives": artifacts_index}, fo, indent=2, ensure_ascii=False)

    log.info("Wrote artifacts index: %s (archives processed: %d)", ARTIFACTS_INDEX, len(archives))
    # print a short summary
    total_extracted_files = sum(len(v) for v in artifacts_index.values() if isinstance(v, list))
    log.info("Total extracted files across all archives: %d", total_extracted_files)

if __name__ == "__main__":
    main()
