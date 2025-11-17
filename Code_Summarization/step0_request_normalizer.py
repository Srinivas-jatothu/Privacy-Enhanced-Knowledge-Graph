# """
# step0_request_normalizer.py

# Purpose:
#   - Receive and normalize a user request for function-level summarization.
#   - Validate input shape and basic path existence against the known repo root(s).
#   - Emit a canonical JSON file that downstream steps read: summarizer_request.json

# Usage (examples):
#   - CLI: python step0_request_normalizer.py --function "compute_total" --file "src/cart/compute.py"
#   - JSON input: python step0_request_normalizer.py --input-json request.json

# Outputs:
#   - Testing_GitHub_Code/results/summarizer_request.json (canonicalized request)

# Notes:
#   - This script performs light validation only (presence, normalization).
#   - It logs DEBUG-level flow so you can trace what happened.

# """
# import argparse
# import json
# import logging
# import os
# import sys
# from datetime import datetime
# from pathlib import Path


# # -----------------------------
# # Configuration (edit if needed)
# # -----------------------------
# # Where to write the canonical request (final pipeline expects this location)
# DEFAULT_OUTPUT_PATH = Path("..") / "Testing_GitHub_Code" / "results" / "summarizer_request.json"
# # Known repository roots (used for light path validation). Adjust if your repo is in a different location.
# KNOWN_REPO_ROOTS = [
#     Path("../Ecommerce-Data-MLOps").resolve(),
#     Path("../Ecommerce-Data-MLOps").resolve(),
# ]


# # -----------------------------
# # Logging setup
# # -----------------------------
# logger = logging.getLogger("step0_request_normalizer")
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
# handler.setFormatter(formatter)
# logger.addHandler(handler)


# # -----------------------------
# # Helpers
# # -----------------------------

# def normalize_function_name(fn: str) -> str:
#     """Normalize a function name: strip, collapse whitespace."""
#     if fn is None:
#         return ""
#     return " ".join(fn.strip().split())


# def normalize_file_path(fp: str) -> str:
#     """Normalize file path to use forward slashes and remove leading ./"""
#     if fp is None:
#         return ""
#     p = fp.strip()
#     # replace backslashes with forward slashes for canonical storage
#     p = p.replace("\\", "/")
#     # remove leading './' or '/' if user provided absolute path unintentionally
#     p = p.lstrip("./\\")
#     return p


# def find_best_repo_root(file_path: str):
#     """Try to suggest which known repo root contains the provided file path.

#     Returns Path or None. This is a light heuristic and may return None if not found.
#     """
#     for root in KNOWN_REPO_ROOTS:
#         candidate = (root / file_path).resolve()
#         if candidate.exists():
#             return root
#     return None


# def validate_request(function_name: str, file_path: str) -> list:
#     """Run lightweight validation. Returns list of warnings (may be empty).

#     Checks performed:
#       - non-empty function name
#       - non-empty file path
#       - whether file exists under a known repo root
#     """
#     warnings = []
#     if not function_name:
#         warnings.append("function_name is empty")
#     if not file_path:
#         warnings.append("file_path is empty")
#     else:
#         repo_root = find_best_repo_root(file_path)
#         if repo_root is None:
#             warnings.append(
#                 "file not found under known repo roots; downstream steps will still attempt KG lookup"
#             )
#         else:
#             logger.debug(f"File resolved under repo root: {repo_root}")
#     return warnings


# # -----------------------------
# # Main flow
# # -----------------------------

# def build_canonical_request(function_name: str, file_path: str, user: str = None, extra: dict = None) -> dict:
#     now = datetime.utcnow().isoformat() + "Z"
#     fn = normalize_function_name(function_name)
#     fp = normalize_file_path(file_path)
#     request = {
#         "requested_at": now,
#         "requester": user if user else "unknown",
#         "function_name": fn,
#         "file_path": fp,
#         # light metadata to help downstream: where we think the file lives (if resolvable)
#         "resolved_repo_root": None,
#         "validation_warnings": [],
#         "extra": extra or {},
#     }
#     repo_root = find_best_repo_root(fp)
#     if repo_root:
#         request["resolved_repo_root"] = str(repo_root)
#     request["validation_warnings"] = validate_request(fn, fp)
#     return request


# def write_output_json(data: dict, out_path: Path):
#     out_path_parent = out_path.parent
#     out_path_parent.mkdir(parents=True, exist_ok=True)
#     with open(out_path, "w", encoding="utf-8") as fh:
#         json.dump(data, fh, indent=2, ensure_ascii=False)
#     logger.info(f"Wrote canonical request to: {out_path}")


# def parse_args(argv=None):
#     p = argparse.ArgumentParser(prog="step0_request_normalizer.py", description="Normalize summarizer request")
#     p.add_argument("--function", help="Name of the function to summarize", required=False)
#     p.add_argument("--file", help="File path (relative to repo root) containing the function", required=False)
#     p.add_argument("--input-json", help="Path to a JSON file with {function_name, file_path} to load", required=False)
#     p.add_argument("--output", help=f"Output path for canonical request (default: {DEFAULT_OUTPUT_PATH})", required=False)
#     p.add_argument("--user", help="Requester id or email", required=False)
#     return p.parse_args(argv)


# def main(argv=None):
#     args = parse_args(argv)
#     logger.debug("Starting step0_request_normalizer")

#     if args.input_json:
#         logger.debug(f"Loading input JSON from: {args.input_json}")
#         with open(args.input_json, "r", encoding="utf-8") as fh:
#             payload = json.load(fh)
#         function_name = payload.get("function_name") or payload.get("function")
#         file_path = payload.get("file_path") or payload.get("file")
#         extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
#     else:
#         function_name = args.function
#         file_path = args.file
#         extra = {}

#     logger.debug(f"Raw inputs - function: {function_name!r}, file: {file_path!r}")

#     request_obj = build_canonical_request(function_name=function_name or "", file_path=file_path or "", user=args.user, extra=extra)

#     out_path = Path(args.output) if args.output else DEFAULT_OUTPUT_PATH
#     # If output path is relative, resolve it relative to current script dir for predictable behavior
#     if not out_path.is_absolute():
#         out_path = (Path(__file__).parent.resolve() / out_path).resolve()

#     logger.debug(f"Final output path resolved to: {out_path}")
#     write_output_json(request_obj, out_path)

#     # Extra debug summary
#     logger.debug("Request canonicalization complete. Summary:")
#     logger.debug(json.dumps(request_obj, indent=2))


# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as e:
#         logger.exception("Unhandled error in step0_request_normalizer")
#         raise



"""
step0_request_normalizer.py

Purpose:
  - Receive and normalize a user request for function-level summarization.
  - Validate input shape and basic path existence against the known repo root(s).
  - Emit a canonical JSON file that downstream steps read: summarizer_request.json

Usage (examples):
  - CLI: python step0_request_normalizer.py --function "load_data" --file "src/data_loader.py"
  - JSON input: python step0_request_normalizer.py --input-json request.json

Outputs:
  - Testing_GitHub_Code/results/summarizer_request.json (canonicalized request)

Notes:
  - This script performs light validation only (presence, normalization).
  - It logs DEBUG-level flow so you can trace what happened.

"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


# -----------------------------
# Configuration (edit if needed)
# -----------------------------
# Where to write the canonical request (final pipeline expects this location)
DEFAULT_OUTPUT_PATH = Path(".") / "results" / "summarizer_request.json"
# Known repository roots (used for light path validation). Adjust if your repo is in a different location.
KNOWN_REPO_ROOTS = [
    Path("../Ecommerce-Data-MLOps").resolve(),
    Path("../Ecommerce-Data-MLOps").resolve(),
]


# -----------------------------
# Logging setup
# -----------------------------
logger = logging.getLogger("step0_request_normalizer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# -----------------------------
# Helpers
# -----------------------------

def normalize_function_name(fn: str) -> str:
    """Normalize a function name: strip, collapse whitespace."""
    if fn is None:
        return ""
    return " ".join(fn.strip().split())


def normalize_file_path(fp: str) -> str:
    """Normalize file path to use forward slashes and remove leading ./"""
    if fp is None:
        return ""
    p = fp.strip()
    # replace backslashes with forward slashes for canonical storage
    p = p.replace("\\", "/")
    # remove leading './' or '/' if user provided absolute path unintentionally
    p = p.lstrip("./\\")
    return p


def find_best_repo_root(file_path: str):
    """Try to suggest which known repo root contains the provided file path.

    Returns Path or None. This is a light heuristic and may return None if not found.
    """
    for root in KNOWN_REPO_ROOTS:
        candidate = (root / file_path).resolve()
        if candidate.exists():
            return root
    return None


def validate_request(function_name: str, file_path: str) -> list:
    """Run lightweight validation. Returns list of warnings (may be empty).

    Checks performed:
      - non-empty function name
      - non-empty file path
      - whether file exists under a known repo root
    """
    warnings = []
    if not function_name:
        warnings.append("function_name is empty")
    if not file_path:
        warnings.append("file_path is empty")
    else:
        repo_root = find_best_repo_root(file_path)
        if repo_root is None:
            warnings.append(
                "file not found under known repo roots; downstream steps will still attempt KG lookup"
            )
        else:
            logger.debug(f"File resolved under repo root: {repo_root}")
    return warnings


# -----------------------------
# Main flow
# -----------------------------

def build_canonical_request(function_name: str, file_path: str, user: str = None, extra: dict = None) -> dict:
    now = datetime.utcnow().isoformat() + "Z"
    fn = normalize_function_name(function_name)
    fp = normalize_file_path(file_path)
    request = {
        "requested_at": now,
        "requester": user if user else "unknown",
        "function_name": fn,
        "file_path": fp,
        # light metadata to help downstream: where we think the file lives (if resolvable)
        "resolved_repo_root": None,
        "validation_warnings": [],
        "extra": extra or {},
    }
    repo_root = find_best_repo_root(fp)
    if repo_root:
        request["resolved_repo_root"] = str(repo_root)
    request["validation_warnings"] = validate_request(fn, fp)
    return request


def write_output_json(data: dict, out_path: Path):
    out_path_parent = out_path.parent
    out_path_parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    logger.info(f"Wrote canonical request to: {out_path}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="step0_request_normalizer.py", description="Normalize summarizer request")
    p.add_argument("--function", help="Name of the function to summarize", required=False)
    p.add_argument("--file", help="File path (relative to repo root) containing the function", required=False)
    p.add_argument("--input-json", help="Path to a JSON file with {function_name, file_path} to load", required=False)
    p.add_argument("--output", help=f"Output path for canonical request (default: {DEFAULT_OUTPUT_PATH})", required=False)
    p.add_argument("--user", help="Requester id or email", required=False)
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logger.debug("Starting step0_request_normalizer")

    if args.input_json:
        logger.debug(f"Loading input JSON from: {args.input_json}")
        with open(args.input_json, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        function_name = payload.get("function_name") or payload.get("function")
        file_path = payload.get("file_path") or payload.get("file")
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    else:
        function_name = args.function
        file_path = args.file
        extra = {}

    logger.debug(f"Raw inputs - function: {function_name!r}, file: {file_path!r}")

    request_obj = build_canonical_request(function_name=function_name or "", file_path=file_path or "", user=args.user, extra=extra)

    out_path = Path(args.output) if args.output else DEFAULT_OUTPUT_PATH
    # If output path is relative, resolve it relative to current script dir for predictable behavior
    if not out_path.is_absolute():
        out_path = (Path(__file__).parent.resolve() / out_path).resolve()

    logger.debug(f"Final output path resolved to: {out_path}")
    write_output_json(request_obj, out_path)

    # Extra debug summary
    logger.debug("Request canonicalization complete. Summary:")
    logger.debug(json.dumps(request_obj, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Unhandled error in step0_request_normalizer")
        raise
