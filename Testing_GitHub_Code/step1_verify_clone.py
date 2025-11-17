# step1_verify_clone.py
import os, logging, sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("clone-verify")

# --- UPDATED PATH LOGIC ---
# 1. Start from the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Repo is inside the parent PEKG directory
candidate = os.path.join(script_dir, "..", "Ecommerce-Data-MLOps")

# Normalize
repo_dir = os.path.abspath(candidate)
log.info("Looking for repo at: %s", repo_dir)

if not os.path.isdir(repo_dir):
    log.error("❌ Repo directory not found at: %s", repo_dir)
    # Try automatic search fallback
    root = os.path.abspath(os.path.join(script_dir, ".."))
    log.info("Searching for repository under: %s", root)
    found = None
    for root_dir, dirs, files in os.walk(root):
        if "Ecommerce-Data-MLOps" in dirs:
            found = os.path.join(root_dir, "Ecommerce-Data-MLOps")
            break
    if not found:
        log.error("❌ Could not locate Ecommerce-Data-MLOps anywhere under: %s", root)
        sys.exit(2)
    else:
        repo_dir = found
        log.info("✅ Repository found at (fallback): %s", repo_dir)
else:
    log.info("✅ Repository found at: %s", repo_dir)

# --- LIST TOP-LEVEL FILES ---
top_files = os.listdir(repo_dir)
log.info("Top-level entries (%d): %s", len(top_files), top_files[:50])

# --- READ README OR DOCS ---
readme_candidates = [
    "README.md", "README.rst", "README", "readme.md",
    "docs/README.md", "docs/index.md"
]

for fname in readme_candidates:
    p = os.path.join(repo_dir, fname)
    if os.path.exists(p):
        if os.path.isfile(p):
            log.info("📄 Found README: %s", p)
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                lines = [next(f).rstrip() for _ in range(5)]
            log.info("First 5 lines of README:\n%s", "\n".join(lines))
        else:
            log.info("📁 Found documentation directory: %s", p)
        break
else:
    log.warning("⚠️ No README found in common locations.")
