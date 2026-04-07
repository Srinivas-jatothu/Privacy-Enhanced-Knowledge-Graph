"""
============================================================
config.py
============================================================
PURPOSE:
    Central configuration file for Method 3 pipeline.
    All paths, settings, and constants defined here.
    Every other script imports from this file.
    Change paths here once - affects entire pipeline.

USAGE:
    from config import *
    or
    import config
============================================================
"""

import os

# ============================================================
# BASE PATHS
# ============================================================

BASE_DIR    = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG"
METHOD3_DIR = os.path.join(BASE_DIR, "PEKG_Sanitization", "Method3_RuleAware")
RESULTS_DIR = os.path.join(METHOD3_DIR, "results")

# ============================================================
# INPUT PATHS (originals - never modified)
# ============================================================

ORIGINAL_REPO           = os.path.join(BASE_DIR, "Ecommerce-Data-MLOps")
ORIGINAL_KG_DIR         = os.path.join(BASE_DIR, "Testing_GitHub_Code", "results")
ORIGINAL_NODE_V2        = os.path.join(ORIGINAL_KG_DIR, "node_v2.json")
ORIGINAL_EDGES          = os.path.join(ORIGINAL_KG_DIR, "edges.json")
ORIGINAL_CODE_ENTITIES  = os.path.join(ORIGINAL_KG_DIR, "code_entities_full.json")
KG_BUILD_SCRIPTS_DIR    = os.path.join(BASE_DIR, "Testing_GitHub_Code")
COMMITS_FILE            = os.path.join(BASE_DIR, "Data_Fetching", "results", "commits.json")
PULL_REQUESTS_FILE      = os.path.join(BASE_DIR, "Data_Fetching", "results", "pull_requests.json")

# ============================================================
# OUTPUT PATHS
# ============================================================

SANITIZED_REPO          = os.path.join(BASE_DIR, "Ecommerce-Data-MLOps-Sanitized")
IDENTIFIER_MAP_FILE     = os.path.join(RESULTS_DIR, "identifier_map.json")
IDENTIFIER_MAP_CSV      = os.path.join(RESULTS_DIR, "identifier_map.csv")
SANITIZED_KG_DIR        = os.path.join(RESULTS_DIR, "kg_method3")
SANITIZED_NODE_V2       = os.path.join(SANITIZED_KG_DIR, "node_v2_method3.json")
SANITIZED_EDGES         = os.path.join(SANITIZED_KG_DIR, "edges_method3.json")
SANITIZATION_REPORT     = os.path.join(RESULTS_DIR, "sanitization_report.csv")
VALIDATION_REPORT       = os.path.join(RESULTS_DIR, "validation_report.txt")
SUMMARIES_METHOD3       = os.path.join(RESULTS_DIR, "summaries_method3.jsonl")
CODE_SNIPPETS_METHOD3   = os.path.join(RESULTS_DIR, "code_snippets_method3.jsonl")
EVAL_REPORT             = os.path.join(RESULTS_DIR, "evaluation_report.txt")
EVAL_METRICS_CSV        = os.path.join(RESULTS_DIR, "evaluation_metrics.csv")

# ============================================================
# STDLIB & THIRD-PARTY MODULES BLOCKLIST
# These are NEVER replaced — they are standard/well-known libs
# ============================================================

STDLIB_MODULES = {
    # Python standard library
    "os", "sys", "re", "ast", "csv", "json", "math",
    "time", "datetime", "pathlib", "shutil", "copy",
    "collections", "itertools", "functools", "typing",
    "pickle", "io", "abc", "logging", "warnings",
    "unittest", "pytest", "argparse", "hashlib",
    "subprocess", "threading", "multiprocessing",
    "contextlib", "dataclasses", "enum", "struct",
    "string", "random", "statistics", "tempfile",

    # Data science / ML
    "numpy", "pandas", "scipy", "sklearn", "matplotlib",
    "seaborn", "plotly", "statsmodels", "xgboost",
    "lightgbm", "catboost", "tensorflow", "torch",
    "keras", "transformers", "cv2", "PIL", "skimage",

    # MLOps / pipeline
    "mlflow", "airflow", "dvc", "wandb", "ray",
    "prefect", "luigi", "kedro",

    # Cloud / infra
    "boto3", "botocore", "google", "azure", "flask",
    "fastapi", "django", "requests", "urllib",
    "paramiko", "fabric", "docker",

    # Database
    "sqlalchemy", "psycopg2", "pymongo", "redis",
    "sqlite3",

    # Utilities
    "yaml", "toml", "dotenv", "click", "typer",
    "loguru", "tqdm", "joblib", "dill",

    # Testing
    "mock", "hypothesis",

    # Sub-modules commonly used
    "os.path", "sklearn.preprocessing", "sklearn.decomposition",
    "sklearn.cluster", "sklearn.metrics", "sklearn.model_selection",
    "matplotlib.pyplot", "matplotlib.colors",
    "pandas.core", "numpy.linalg",
}

# ============================================================
# PARAMETER NAMES TO SKIP (too common / too short)
# These appear everywhere and would cause false replacements
# ============================================================

SKIP_PARAMS = {
    # Standard Python
    "self", "cls", "args", "kwargs",

    # Very common variable names that appear in code body too
    "path", "data", "file", "df", "result", "output",
    "input", "value", "key", "index", "name", "type",
    "x", "y", "z", "i", "j", "k", "n", "m",
    "col", "row", "val", "var", "obj", "msg",
    "err", "exc", "e", "ex", "config", "params",
    "arg", "opt", "mode", "flag", "size", "length",
    "width", "height", "count", "num", "idx", "pos",
    "src", "dst", "tmp", "buf", "ret", "res",
    "logger", "log", "level", "fmt", "sep", "end",
    "encoding", "errors", "newline", "delimiter",
}

# ============================================================
# ROLE-AWARE IDENTIFIER CATEGORIES
# ============================================================

ROLE_KEYWORDS = {
    "data_cleaning": [
        "clean", "handle", "fix", "remove", "filter",
        "normalize", "anomaly", "duplicate", "missing",
        "null", "drop", "strip", "validate", "sanitize",
        "preprocess", "deduplicate", "outlier", "impute",
        "correct", "repair", "anomalous", "invalid",
        "zero", "unitprice", "removing"
    ],
    "data_loader": [
        "load", "read", "fetch", "ingest", "download",
        "import", "parse", "extract", "get", "retrieve",
        "unzip", "open", "collect", "acquire", "pull",
        "receive", "loader"
    ],
    "data_saver": [
        "save", "write", "export", "store", "persist",
        "dump", "output", "upload", "push", "send",
        "publish", "saver"
    ],
    "visualization": [
        "plot", "draw", "show", "display", "visualize",
        "render", "heatmap", "chart", "graph", "figure",
        "image", "diagram", "histogram", "scatter",
        "bar", "pie", "radar"
    ],
    "analysis": [
        "analyze", "check", "compute", "calculate",
        "measure", "evaluate", "assess", "examine",
        "inspect", "detect", "correlation", "statistics",
        "metric", "score", "test", "analysis"
    ],
    "ml_model": [
        "train", "predict", "fit", "transform", "scale",
        "pca", "cluster", "classify", "regress", "model",
        "encode", "decode", "embed", "recommend", "infer",
        "learn", "scaler", "analyzer"
    ],
    "feature_engineering": [
        "feature", "engineer", "construct", "build",
        "create", "generate", "derive", "aggregate",
        "combine", "merge", "rfm", "segment", "behavior",
        "seasonal", "geographic", "cancellation",
        "transaction", "unique", "products", "seasonality"
    ],
    "pipeline": [
        "run", "execute", "pipeline", "dag", "airflow",
        "workflow", "orchestrate", "schedule", "trigger",
        "start", "main", "datapipeline"
    ],
    "utility": [
        "util", "helper", "config", "setup", "init",
        "log", "debug", "format", "convert", "map",
        "apply", "wrap", "build", "inference"
    ],
}

FILE_ROLE_KEYWORDS = {
    "data_cleaning":        ["anomaly", "cleaning", "duplicate",
                             "missing", "outlier", "removing", "zero"],
    "data_loader":          ["loader", "download", "unzip", "data"],
    "visualization":        ["correlation", "histogram", "radar", "plot"],
    "analysis":             ["correlation", "pca", "rfm", "seasonality"],
    "ml_model":             ["customers", "behavior", "cluster",
                             "recommender"],
    "feature_engineering":  ["geographic", "cancellation", "transaction",
                             "unique"],
    "pipeline":             ["airflow", "dag", "datapipeline"],
    "deployment":           ["gcpdeploy", "deploy", "serve", "trainer"],
    "testing":              ["test"],
    "utility":              ["scaler", "build", "inference"],
}

# ============================================================
# SANITIZATION SETTINGS
# ============================================================

SCAN_EXTENSIONS = {".py"}

SKIP_FOLDERS = {
    "__pycache__", ".git", ".dvc", "venv",
    "node_modules", ".idea", ".vscode",
    "mlruns", "logs"
}

SKIP_FILES = {"__init__.py"}

# ============================================================
# PLACEHOLDER FORMATS
# ============================================================

FUNC_PLACEHOLDER_FORMAT     = "{role}_func_{n:03d}"
FILE_PLACEHOLDER_FORMAT     = "{role}_file_{n:03d}"
MODULE_PLACEHOLDER_FORMAT   = "module_l{depth}_{n:03d}"
PARAM_PLACEHOLDER_FORMAT    = "param_{n:03d}"
CLASS_PLACEHOLDER_FORMAT    = "{role}_class_{n:03d}"

# ============================================================
# PRIVACY SCAN PATTERNS
# ============================================================

KNOWN_CONTRIBUTORS = [
    "Thomas", "Ashkan", "Ghanavati", "Moheth",
    "Bardia", "Komal", "kokomocha", "baridamm",
    "Thomas-George-T", "AshyScripts"
]

KNOWN_INTERNAL_IDENTIFIERS = [
    "Ecommerce-Data-MLOps",
    "Thomas-George-T",
    "customersegmentation",
]

# ============================================================
# ENSURE OUTPUT DIRECTORIES EXIST
# ============================================================

os.makedirs(RESULTS_DIR,        exist_ok=True)
os.makedirs(SANITIZED_KG_DIR,   exist_ok=True)

# ============================================================
# PRINT CONFIG SUMMARY
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("METHOD 3 CONFIGURATION")
    print("=" * 60)
    print(f"Base dir          : {BASE_DIR}")
    print(f"Original repo     : {ORIGINAL_REPO}")
    print(f"Sanitized repo    : {SANITIZED_REPO}")
    print(f"Results dir       : {RESULTS_DIR}")
    print(f"Sanitized KG dir  : {SANITIZED_KG_DIR}")
    print()
    print(f"STDLIB blocklist  : {len(STDLIB_MODULES)} modules")
    print(f"Param blocklist   : {len(SKIP_PARAMS)} params")
    print()
    print("Role categories defined:")
    for role, keywords in ROLE_KEYWORDS.items():
        print(f"  {role:<25} {len(keywords)} keywords")
    print()
    print("Checking paths exist:")
    paths = {
        "Original repo":    ORIGINAL_REPO,
        "Original KG":      ORIGINAL_NODE_V2,
        "Original edges":   ORIGINAL_EDGES,
        "KG build scripts": KG_BUILD_SCRIPTS_DIR,
    }
    for name, path in paths.items():
        exists = "✓" if os.path.exists(path) else "✗ NOT FOUND"
        print(f"  {name:<20} : {exists}")
    print("=" * 60)