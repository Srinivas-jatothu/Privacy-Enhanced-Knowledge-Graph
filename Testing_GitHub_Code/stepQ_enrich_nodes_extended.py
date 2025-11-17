# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # stepQ_enrich_nodes_extended.py

# # Enrich nodes.json with:
# #   - hash (file content SHA1)
# #   - introduced_by_commit (top commit that introduced the node)
# #   - introduced_by_pr (PR number/title if available)
# #   - modified_by_commits (list of commits that modified the node / function)

# # Outputs:
# #   - results/node_v2.json (by default)
# #   - results/kg_check/stepQ_debug.json (detailed per-node debug)

# # Usage:
# #   python stepQ_enrich_nodes_extended.py --repo-dir "..\\Ecommerce-Data-MLOps"

# # Options:
# #   --replace           Replace original results/nodes.json with enriched nodes (creates backup)
# #   --backup-nodes      Create a timestamped backup of results/nodes.json before any replacement
# #   --sample-only N     Only process first N nodes (useful for a quick dry-run)
# # """
# # from __future__ import annotations
# # import argparse
# # import json
# # import os
# # import hashlib
# # import datetime
# # import sys
# # from typing import Any, Dict, List, Optional

# # # --------------------------
# # # Helpers
# # # --------------------------
# # def load_json(path: str):
# #     if not os.path.exists(path):
# #         return None
# #     with open(path, "r", encoding="utf-8") as fh:
# #         return json.load(fh)

# # def write_json(path: str, obj: Any):
# #     os.makedirs(os.path.dirname(path), exist_ok=True)
# #     with open(path, "w", encoding="utf-8") as fh:
# #         json.dump(obj, fh, indent=2, ensure_ascii=False)

# # def compute_sha1_of_file(fullpath: str) -> Optional[str]:
# #     try:
# #         h = hashlib.sha1()
# #         with open(fullpath, "rb") as fh:
# #             while True:
# #                 chunk = fh.read(8192)
# #                 if not chunk:
# #                     break
# #                 h.update(chunk)
# #         return h.hexdigest()
# #     except Exception as e:
# #         return None

# # def iso_now():
# #     return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# # # --------------------------
# # # Build indexes from inputs
# # # --------------------------
# # def build_commit_meta_index(commits: List[Dict]) -> Dict[str, Dict]:
# #     meta = {}
# #     if not isinstance(commits, list):
# #         return meta
# #     for c in commits:
# #         if not isinstance(c, dict):
# #             continue
# #         sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
# #         if not sha:
# #             continue
# #         msg = c.get("message") or (c.get("commit") and c["commit"].get("message")) or c.get("message_head")
# #         author = None
# #         date = None
# #         if c.get("commit") and isinstance(c["commit"].get("author"), dict):
# #             date = c["commit"]["author"].get("date")
# #             if not author:
# #                 author = c["commit"]["author"].get("name")
# #         if not author and isinstance(c.get("author"), dict):
# #             author = c["author"].get("login") or c["author"].get("name")
# #         if not date:
# #             date = c.get("date") or c.get("author_date")
# #         meta[sha] = {"sha": sha, "message": msg, "author": author, "date": date}
# #         # also store short sha key
# #         if len(sha) >= 7:
# #             meta[sha[:7]] = meta[sha]
# #     return meta

# # def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
# #     out = {}
# #     if isinstance(mapping, dict):
# #         for k, v in mapping.items():
# #             key = str(k)
# #             if not v:
# #                 out[key] = []
# #             else:
# #                 out[key] = [str(x) for x in v]
# #         return out
# #     if isinstance(mapping, list):
# #         for it in mapping:
# #             if not isinstance(it, dict):
# #                 continue
# #             key = it.get("commit") or it.get("sha")
# #             prs = it.get("prs") or it.get("pull_requests") or it.get("pr_list")
# #             if key and prs:
# #                 out[str(key)] = [str(x.get("number") if isinstance(x, dict) else x) for x in prs]
# #         return out
# #     return out

# # def build_pr_meta(prs: List[Dict]) -> Dict[str, Dict]:
# #     meta = {}
# #     if not isinstance(prs, list):
# #         return meta
# #     for p in prs:
# #         if isinstance(p, dict):
# #             num = p.get("number") or p.get("id")
# #             if num is None:
# #                 continue
# #             title = p.get("title")
# #             author = None
# #             user = p.get("user")
# #             if isinstance(user, dict):
# #                 author = user.get("login") or user.get("name")
# #             elif isinstance(user, str):
# #                 author = user
# #             if not author:
# #                 top_author = p.get("author")
# #                 if isinstance(top_author, dict):
# #                     author = top_author.get("login") or top_author.get("name")
# #                 elif isinstance(top_author, str):
# #                     author = top_author
# #             meta[str(num)] = {"title": title, "author": author}
# #         else:
# #             meta[str(p)] = {"title": None, "author": None}
# #     return meta

# # # --------------------------
# # # Lookup helpers
# # # --------------------------
# # def find_prs_for_sha(sha: str, commit_to_prs_norm: Dict[str, List[str]], commit_meta: Dict[str, Dict]) -> List[str]:
# #     if not sha:
# #         return []
# #     if sha in commit_to_prs_norm:
# #         return commit_to_prs_norm[sha] or []
# #     if sha.lower() in commit_to_prs_norm:
# #         return commit_to_prs_norm[sha.lower()] or []
# #     pref = sha[:7]
# #     for k in commit_to_prs_norm.keys():
# #         try:
# #             if str(k).startswith(pref) or str(k).lower().startswith(pref.lower()):
# #                 return commit_to_prs_norm[k] or []
# #         except Exception:
# #             continue
# #     for full in commit_meta.keys():
# #         if str(full).startswith(pref) or str(full).lower().startswith(pref.lower()):
# #             if full in commit_to_prs_norm:
# #                 return commit_to_prs_norm[full] or []
# #             if str(full).lower() in commit_to_prs_norm:
# #                 return commit_to_prs_norm[str(full).lower()] or []
# #     return []

# # # --------------------------
# # # Main enrichment
# # # --------------------------
# # def main():
# #     p = argparse.ArgumentParser()
# #     p.add_argument("--nodes", default="results/nodes.json", help="Path to canonical nodes.json")
# #     p.add_argument("--func-commits", default="results/function_commits.json", help="Function->commit mapping")
# #     p.add_argument("--commits", default="results/commits.json", help="Commits metadata")
# #     p.add_argument("--commit-to-prs", default="results/commit_to_prs.json", help="Commit->PRs mapping")
# #     p.add_argument("--prs", default="results/pull_requests.json", help="Pull requests metadata")
# #     p.add_argument("--file-index", default="results/file_index.json", help="File index (optional)")
# #     p.add_argument("--repo-dir", default=".", help="Path to local repo root containing .git and source files")
# #     p.add_argument("--out", default="results/node_v2.json", help="Output enriched nodes file")
# #     p.add_argument("--debug-out", default="results/kg_check/stepQ_debug.json", help="Debug JSON output (detailed)")
# #     p.add_argument("--sample-only", type=int, default=0, help="Process only first N nodes (0 = all)")
# #     p.add_argument("--replace", action="store_true", help="Replace original nodes.json with enriched output (creates backup if --backup-nodes given)")
# #     p.add_argument("--backup-nodes", action="store_true", help="Create backup of original nodes.json before replacement")
# #     args = p.parse_args()

# #     # Load inputs
# #     print("[INFO] Loading inputs...")
# #     nodes = load_json(args.nodes)
# #     if nodes is None:
# #         print(f"[ERROR] nodes.json not found at {args.nodes}")
# #         return 2
# #     func_commits = load_json(args.func_commits) or {}
# #     commits = load_json(args.commits) or []
# #     commit_to_prs = load_json(args.commit_to_prs) or {}
# #     prs = load_json(args.prs) or []
# #     file_index = load_json(args.file_index) or {}

# #     commit_meta = build_commit_meta_index(commits)
# #     commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
# #     pr_meta = build_pr_meta(prs)

# #     # Stats and debug container
# #     total_nodes = len(nodes)
# #     sample_limit = args.sample_only if args.sample_only > 0 else None
# #     processed = 0
# #     enriched_count = 0
# #     debug = {"run_ts": iso_now(), "repo_dir": args.repo_dir, "processed": []}

# #     # Helper to resolve a repo-relative path for a node
# #     def resolve_path_from_node(node: Dict) -> Optional[str]:
# #         # Try standard places
# #         if node.get("path"):
# #             rel = node.get("path")
# #             if os.path.isabs(rel):
# #                 # if absolute, try relative to repo_dir
# #                 try_rel = os.path.relpath(rel, args.repo_dir)
# #                 if not try_rel.startswith(".."):
# #                     rel = try_rel.replace("\\", "/")
# #             return rel.replace("\\", "/")
# #         # older nodes may keep path in attrs or file field
# #         attrs = node.get("attrs") or {}
# #         for k in ("file", "path", "filename", "filepath"):
# #             if attrs.get(k):
# #                 return str(attrs.get(k)).replace("\\", "/")
# #         # fallback to label if it looks like a path
# #         label = node.get("label")
# #         if label and "/" in label and label.endswith(".py"):
# #             return label.replace("\\","/")
# #         return None

# #     # Ensure output folder exists
# #     os.makedirs(os.path.dirname(args.out), exist_ok=True)
# #     os.makedirs(os.path.dirname(args.debug_out), exist_ok=True)

# #     print(f"[INFO] Processing {total_nodes} nodes (sample_only={args.sample_only}) ...")
# #     for node in nodes:
# #         if sample_limit is not None and processed >= sample_limit:
# #             break
# #         processed += 1

# #         node_debug: Dict[str, Any] = {"id": node.get("id"), "type": node.get("type"), "label": node.get("label"), "attempts": []}
# #         attrs = node.setdefault("attrs", {})

# #         # 1) compute hash for File nodes (and for functions we also attach file_hash)
# #         if node.get("type") == "File" or node.get("type") == "SourceFile":
# #             path = resolve_path_from_node(node)
# #             if path:
# #                 node_debug["attempts"].append({"action": "compute_hash", "path": path})
# #                 full = os.path.join(args.repo_dir, path)
# #                 if os.path.exists(full):
# #                     sha1 = compute_sha1_of_file(full)
# #                     if sha1:
# #                         node["hash"] = sha1
# #                         node_debug["hash"] = sha1
# #                     else:
# #                         node_debug["hash_error"] = "hash_compute_failed"
# #                 else:
# #                     node_debug["hash_error"] = "file_not_found"
# #             else:
# #                 node_debug["hash_error"] = "no_path"
# #         elif node.get("type") == "Function":
# #             # for functions, attempt to compute the file_hash (if file present)
# #             fpath = None
# #             attrs = node.get("attrs") or {}
# #             fpath = attrs.get("file") or attrs.get("path")
# #             if not fpath:
# #                 # try to extract file from qualified_name if it's path-like (path:lineno)
# #                 q = node.get("qualified_name")
# #                 if q and ".py" in q:
# #                     # try common pattern 'dags/src/airflow.py:31' or 'module.funcname'
# #                     if ":" in q:
# #                         cand = q.split(":",1)[0]
# #                         fpath = cand
# #             if fpath:
# #                 fullfp = os.path.join(args.repo_dir, fpath)
# #                 node_debug["attempts"].append({"action":"function_file_hash", "file": fpath})
# #                 if os.path.exists(fullfp):
# #                     h = compute_sha1_of_file(fullfp)
# #                     if h:
# #                         attrs["file_hash"] = h
# #                         node_debug["file_hash"] = h
# #                     else:
# #                         node_debug["file_hash_error"] = "hash_failed"
# #                 else:
# #                     node_debug["file_hash_error"] = "file_not_found"

# #         # 2) introduced_by_commit & modified_by_commits
# #         # For functions — use function_commits; for files — try file_index or fallback
# #         node_type = node.get("type")
# #         qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
# #         top_commit = None
# #         modified_commits: List[str] = []

# #         if node_type == "Function":
# #             # look up by qualified name or id
# #             mapping = None
# #             if isinstance(func_commits, dict):
# #                 mapping = func_commits.get(qname) or func_commits.get(node.get("id"))
# #             if not mapping:
# #                 # some function keys are stored without module prefix — try suffix match
# #                 short = qname.split(".")[-1] if qname else None
# #                 if short:
# #                     for k, v in func_commits.items():
# #                         if isinstance(k, str) and k.endswith(short):
# #                             mapping = v
# #                             node_debug["attempts"].append({"action":"heuristic_suffix_match","matched_key":k})
# #                             break
# #             if mapping and isinstance(mapping, dict):
# #                 # mapping may include top_commit or top_commit.sha or blame_summary
# #                 # normalized representation handling
# #                 tc = None
# #                 if mapping.get("top_commit") and isinstance(mapping["top_commit"], dict):
# #                     tc = mapping["top_commit"].get("sha") or mapping["top_commit"].get("id")
# #                 elif mapping.get("top_commit") and isinstance(mapping["top_commit"], str):
# #                     tc = mapping.get("top_commit")
# #                 elif mapping.get("top_commit_sha"):
# #                     tc = mapping.get("top_commit_sha")
# #                 elif mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
# #                     # choose the most frequent key if multiple
# #                     try:
# #                         # keys are shas; pick the first
# #                         ks = list(mapping["blame_summary"].keys())
# #                         if ks:
# #                             tc = ks[0]
# #                     except Exception:
# #                         pass
# #                 if tc:
# #                     top_commit = tc
# #                     attrs["introduced_by_commit"] = tc
# #                     node_debug["introduced_by_commit"] = tc
# #                 # gather modified_by_commits from blame_summary keys if present
# #                 if mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
# #                     modified_commits = list(mapping["blame_summary"].keys())
# #                 elif mapping.get("top_commit") and isinstance(mapping.get("top_commit"), dict):
# #                     sc = mapping["top_commit"].get("sha")
# #                     if sc:
# #                         modified_commits = [sc]
# #                 # record
# #                 if modified_commits:
# #                     attrs["modified_by_commits"] = modified_commits
# #                     node_debug["modified_by_commits"] = modified_commits
# #             else:
# #                 node_debug["attempts"].append({"action":"no_function_commits_mapping_found", "qname": qname})

# #         elif node_type in ("File", "SourceFile", "Config"):
# #             # try file_index (if provided) to extract blame/commits
# #             path = resolve_path_from_node(node)
# #             if path:
# #                 node_debug["attempts"].append({"action":"resolve_file_index", "path": path})
# #                 # file_index may have entries keyed by path
# #                 finfo = None
# #                 if isinstance(file_index, dict):
# #                     finfo = file_index.get(path) or file_index.get(os.path.basename(path))
# #                 if finfo and isinstance(finfo, dict):
# #                     # finfo might have 'blame_summary' or 'commits' or 'top_commit'
# #                     tc = None
# #                     if finfo.get("top_commit") and isinstance(finfo["top_commit"], dict):
# #                         tc = finfo["top_commit"].get("sha") or finfo["top_commit"].get("id")
# #                     if not tc and finfo.get("commits") and isinstance(finfo["commits"], list) and finfo["commits"]:
# #                         # pick first commit
# #                         first = finfo["commits"][0]
# #                         tc = first.get("sha") if isinstance(first, dict) else (first if isinstance(first, str) else None)
# #                     if tc:
# #                         node["introduced_by_commit"] = tc
# #                         node_debug["introduced_by_commit"] = tc
# #                         top_commit = tc
# #                     # collect modified_by_commits
# #                     if finfo.get("commits") and isinstance(finfo["commits"], list):
# #                         modified_commits = [str(x.get("sha") if isinstance(x, dict) else x) for x in finfo["commits"]]
# #                     elif finfo.get("blame_summary") and isinstance(finfo["blame_summary"], dict):
# #                         modified_commits = list(finfo["blame_summary"].keys())
# #                     if modified_commits:
# #                         node["modified_by_commits"] = modified_commits
# #                         node_debug["modified_by_commits"] = modified_commits
# #                 else:
# #                     # fallback: try to compute from git metadata if the file exists in repo
# #                     full = os.path.join(args.repo_dir, path)
# #                     if os.path.exists(full):
# #                         # attempt to run a quick blame whole-file if function_commits lacks it
# #                         node_debug["attempts"].append({"action":"fallback_file_blame_wholefile", "path": path})
# #                         # We avoid running git here (keep script pure-Python). If user wants blame-based fallback we can extend.
# #                         # For now we will mark as no_file_index_and_repo_file_exists to signal possibility.
# #                         node_debug["note"] = "file_exists_but_no_file_index_entry; consider running blame-based mapping"
# #                     else:
# #                         node_debug["attempts"].append({"action":"file_not_found_in_repo", "path": path})
# #             else:
# #                 node_debug["attempts"].append({"action":"no_path_for_file_node"})

# #         else:
# #             # for other node types (Module, Package, Commit, PullRequest) we attempt a minimal enrichment if possible
# #             # e.g., for Commit nodes we can link to PRs via commit_to_prs
# #             if node_type == "Commit":
# #                 # get sha from node attrs
# #                 sha = (node.get("attrs") or {}).get("sha") or node.get("id") and node.get("id").split(":",1)[-1]
# #                 if sha:
# #                     node_debug["attempts"].append({"action":"commit_pr_lookup", "sha": sha})
# #                     pr_list = find_prs_for_sha(sha, commit_to_prs_norm, commit_meta)
# #                     if pr_list:
# #                         node["prs"] = pr_list
# #                         node_debug["prs"] = pr_list

# #         # 3) introduced_by_pr: if we have top_commit, look up PRs and attach first PR (if any)
# #         if (node.get("introduced_by_commit") or top_commit) and not node.get("introduced_by_pr"):
# #             tc = node.get("introduced_by_commit") or top_commit
# #             if tc:
# #                 prs_found = find_prs_for_sha(tc, commit_to_prs_norm, commit_meta)
# #                 if prs_found:
# #                     node["introduced_by_pr"] = prs_found[0]
# #                     # also attach title if available
# #                     prinfo = pr_meta.get(str(prs_found[0]))
# #                     if prinfo and prinfo.get("title"):
# #                         node["introduced_by_pr_title"] = prinfo.get("title")
# #                     node_debug["introduced_by_pr"] = node.get("introduced_by_pr")
# #                 else:
# #                     node_debug["introduced_by_pr"] = None

# #         # finalize debug
# #         debug["processed"].append(node_debug)

# #         # counting
# #         if node.get("introduced_by_commit") or node.get("modified_by_commits"):
# #             enriched_count += 1

# #     # Write out enriched nodes and debug
# #     write_json(args.out, nodes)
# #     write_json(args.debug_out, debug)

# #     print("[INFO] Enrichment finished.")
# #     print(f"[INFO] Processed nodes: {processed}; enriched nodes (have commit/mod list): {enriched_count}")
# #     print(f"[INFO] Output written to: {args.out}")
# #     print(f"[INFO] Debug written to: {args.debug_out}")

# #     # replace original nodes.json if requested
# #     if args.replace:
# #         try:
# #             if args.backup_nodes:
# #                 bak = args.nodes + f".bak.{int(datetime.datetime.now().timestamp())}"
# #                 write_json(bak, load_json(args.nodes) or {})
# #                 print(f"[INFO] Backup of original nodes.json saved to {bak}")
# #             # overwrite nodes.json
# #             write_json(args.nodes, nodes)
# #             print(f"[WARN] Replaced original nodes.json with enriched nodes (saved at {args.nodes})")
# #         except Exception as e:
# #             print(f"[ERROR] Failed to replace nodes.json: {e}", file=sys.stderr)

# # if __name__ == "__main__":
# #     main()






# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# stepQ_enrich_nodes_extended.py (blame-capable)

# Enrich nodes.json with:
#   - hash (file content SHA1)
#   - introduced_by_commit (top commit that introduced the node)
#   - introduced_by_pr (PR number/title if available)
#   - modified_by_commits (list of commits that modified the node / function)

# This version optionally runs `git blame` (when --blame-fallback) to recover commit info
# for files that lack entries in file_index.json.

# Outputs:
#   - results/node_v2.json (by default)
#   - results/kg_check/stepQ_debug.json (detailed per-node debug)

# Usage (examples):
#   # quick test (first 100 nodes)
#   python stepQ_enrich_nodes_extended.py --repo-dir "..\\Ecommerce-Data-MLOps" --sample-only 100 --blame-fallback

#   # full run (blame fallback)
#   python stepQ_enrich_nodes_extended.py --repo-dir "..\\Ecommerce-Data-MLOps" --blame-fallback

# Notes:
#  - Requires `git` on PATH if --blame-fallback is used and repo-dir points to a git clone.
#  - The blame fallback is best-effort; it will skip files not tracked by git.
# """
# from __future__ import annotations
# import argparse
# import json
# import os
# import hashlib
# import datetime
# import sys
# import subprocess
# from collections import Counter
# from typing import Any, Dict, List, Optional, Tuple

# SHA_RE_HEX = "0123456789abcdef"
# # --------------------------
# # IO helpers
# # --------------------------
# def load_json(path: str):
#     if not os.path.exists(path):
#         return None
#     with open(path, "r", encoding="utf-8") as fh:
#         return json.load(fh)

# def write_json(path: str, obj: Any):
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     with open(path, "w", encoding="utf-8") as fh:
#         json.dump(obj, fh, indent=2, ensure_ascii=False)

# def compute_sha1_of_file(fullpath: str) -> Optional[str]:
#     try:
#         h = hashlib.sha1()
#         with open(fullpath, "rb") as fh:
#             while True:
#                 chunk = fh.read(8192)
#                 if not chunk:
#                     break
#                 h.update(chunk)
#         return h.hexdigest()
#     except Exception as e:
#         return None

# def iso_now():
#     return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# # --------------------------
# # Git / blame helpers
# # --------------------------
# def run_git_blame_wholefile(repo_dir: str, relpath: str) -> Tuple[List[str], Optional[str]]:
#     """
#     Run `git blame --line-porcelain -- <file>` and return list of SHAs (may contain duplicates).
#     Returns (shas_list, error_message_or_none)
#     """
#     try:
#         full = os.path.join(repo_dir, relpath)
#         if not os.path.exists(full):
#             return [], "file_not_found"
#         # execute git blame --line-porcelain file
#         out = subprocess.check_output(
#             ["git", "-C", repo_dir, "blame", "--line-porcelain", "--", relpath],
#             stderr=subprocess.STDOUT,
#             timeout=30
#         )
#         text = out.decode("utf-8", errors="replace")
#         shas = []
#         for line in text.splitlines():
#             if not line:
#                 continue
#             # lines with SHA start a blame block; first token is sha
#             parts = line.split()
#             if parts and len(parts[0]) >= 7 and all(c.lower() in SHA_RE_HEX for c in parts[0][:7]):
#                 shas.append(parts[0])
#         return shas, None
#     except subprocess.CalledProcessError as e:
#         return [], f"git_blame_failed:{e.returncode}:{str(e.output)[:2000]}"
#     except Exception as e:
#         return [], f"git_blame_exception:{e}"

# def choose_top_sha_from_blame(shas: List[str]) -> Optional[str]:
#     if not shas:
#         return None
#     # count frequency and choose most frequent; tie-breaker: first occurrence
#     ctr = Counter(shas)
#     most_common = ctr.most_common()
#     if not most_common:
#         return None
#     return most_common[0][0]

# # --------------------------
# # Index building helpers
# # --------------------------
# def build_commit_meta_index(commits: List[Dict]) -> Dict[str, Dict]:
#     meta = {}
#     if not isinstance(commits, list):
#         return meta
#     for c in commits:
#         if not isinstance(c, dict):
#             continue
#         sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
#         if not sha:
#             continue
#         msg = c.get("message") or (c.get("commit") and c["commit"].get("message")) or c.get("message_head")
#         author = None
#         date = None
#         if c.get("commit") and isinstance(c["commit"].get("author"), dict):
#             date = c["commit"]["author"].get("date")
#             if not author:
#                 author = c["commit"]["author"].get("name")
#         if not author and isinstance(c.get("author"), dict):
#             author = c["author"].get("login") or c["author"].get("name")
#         if not date:
#             date = c.get("date") or c.get("author_date")
#         meta[sha] = {"sha": sha, "message": msg, "author": author, "date": date}
#         if len(sha) >= 7:
#             meta[sha[:7]] = meta[sha]
#     return meta

# def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
#     out = {}
#     if isinstance(mapping, dict):
#         for k, v in mapping.items():
#             out[str(k)] = [str(x) for x in v] if v else []
#         return out
#     if isinstance(mapping, list):
#         for it in mapping:
#             if not isinstance(it, dict):
#                 continue
#             key = it.get("commit") or it.get("sha")
#             prs = it.get("prs") or it.get("pull_requests") or it.get("pr_list")
#             if key and prs:
#                 out[str(key)] = [str(x.get("number") if isinstance(x, dict) else x) for x in prs]
#         return out
#     return out

# def build_pr_meta(prs: List[Dict]) -> Dict[str, Dict]:
#     meta = {}
#     if not isinstance(prs, list):
#         return meta
#     for p in prs:
#         if isinstance(p, dict):
#             num = p.get("number") or p.get("id")
#             if num is None:
#                 continue
#             title = p.get("title")
#             author = None
#             user = p.get("user")
#             if isinstance(user, dict):
#                 author = user.get("login") or user.get("name")
#             elif isinstance(user, str):
#                 author = user
#             if not author:
#                 top_author = p.get("author")
#                 if isinstance(top_author, dict):
#                     author = top_author.get("login") or top_author.get("name")
#                 elif isinstance(top_author, str):
#                     author = top_author
#             meta[str(num)] = {"title": title, "author": author}
#         else:
#             meta[str(p)] = {"title": None, "author": None}
#     return meta

# def find_prs_for_sha(sha: str, commit_to_prs_norm: Dict[str, List[str]], commit_meta: Dict[str, Dict]) -> List[str]:
#     if not sha:
#         return []
#     if sha in commit_to_prs_norm:
#         return commit_to_prs_norm[sha] or []
#     if sha.lower() in commit_to_prs_norm:
#         return commit_to_prs_norm[sha.lower()] or []
#     pref = sha[:7]
#     for k in commit_to_prs_norm.keys():
#         try:
#             if str(k).startswith(pref) or str(k).lower().startswith(pref.lower()):
#                 return commit_to_prs_norm[k] or []
#         except Exception:
#             continue
#     for full in commit_meta.keys():
#         if str(full).startswith(pref) or str(full).lower().startswith(pref.lower()):
#             if full in commit_to_prs_norm:
#                 return commit_to_prs_norm[full] or []
#             if str(full).lower() in commit_to_prs_norm:
#                 return commit_to_prs_norm[str(full).lower()] or []
#     return []

# # --------------------------
# # Node helpers
# # --------------------------
# def resolve_path_from_node(node: Dict, repo_dir: str) -> Optional[str]:
#     if node.get("path"):
#         rel = node.get("path")
#         if os.path.isabs(rel):
#             try:
#                 relp = os.path.relpath(rel, repo_dir)
#                 if not relp.startswith(".."):
#                     return relp.replace("\\","/")
#             except Exception:
#                 pass
#         return rel.replace("\\","/")
#     attrs = node.get("attrs") or {}
#     for k in ("file", "path", "filename", "filepath"):
#         if attrs.get(k):
#             return str(attrs.get(k)).replace("\\","/")
#     label = node.get("label")
#     if label and "/" in label and label.endswith(".py"):
#         return label.replace("\\","/")
#     # also support qualified_name as path:lineno
#     q = node.get("qualified_name")
#     if q and q.endswith(".py") and ":" in q:
#         return q.split(":",1)[0]
#     return None

# # --------------------------
# # Main
# # --------------------------
# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--nodes", default="results/nodes.json")
#     p.add_argument("--func-commits", default="results/function_commits.json")
#     p.add_argument("--commits", default="results/commits.json")
#     p.add_argument("--commit-to-prs", default="results/commit_to_prs.json")
#     p.add_argument("--prs", default="results/pull_requests.json")
#     p.add_argument("--file-index", default="results/file_index.json")
#     p.add_argument("--repo-dir", default=".", help="path to repo root containing .git and source files")
#     p.add_argument("--out", default="results/node_v2.json")
#     p.add_argument("--debug-out", default="results/kg_check/stepQ_debug.json")
#     p.add_argument("--sample-only", type=int, default=0)
#     p.add_argument("--replace", action="store_true")
#     p.add_argument("--backup-nodes", action="store_true")
#     p.add_argument("--blame-fallback", action="store_true", help="If set, run git blame on files missing file_index entries")
#     p.add_argument("--blame-timeout", type=int, default=30, help="Timeout seconds for git blame calls")
#     args = p.parse_args()

#     print("[INFO] Loading inputs...")
#     nodes = load_json(args.nodes)
#     if nodes is None:
#         print(f"[ERROR] nodes.json not found at {args.nodes}")
#         return 2
#     func_commits = load_json(args.func_commits) or {}
#     commits = load_json(args.commits) or []
#     commit_to_prs = load_json(args.commit_to_prs) or {}
#     prs = load_json(args.prs) or []
#     file_index = load_json(args.file_index) or {}

#     commit_meta = build_commit_meta_index(commits)
#     commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
#     pr_meta = build_pr_meta(prs)

#     total_nodes = len(nodes)
#     sample_limit = args.sample_only if args.sample_only > 0 else None
#     processed = 0
#     enriched_count = 0
#     debug = {"run_ts": iso_now(), "repo_dir": args.repo_dir, "blame_fallback_enabled": bool(args.blame_fallback), "processed": []}

#     # optional backup
#     if args.replace and args.backup_nodes:
#         bak = args.nodes + f".bak.{int(datetime.datetime.now().timestamp())}"
#         write_json(bak, load_json(args.nodes) or {})
#         print(f"[INFO] Backup original nodes.json -> {bak}")

#     print(f"[INFO] Processing {total_nodes} nodes (sample_only={args.sample_only}) ...")
#     for node in nodes:
#         if sample_limit is not None and processed >= sample_limit:
#             break
#         processed += 1

#         node_debug: Dict[str, Any] = {"id": node.get("id"), "type": node.get("type"), "label": node.get("label"), "attempts": []}
#         attrs = node.setdefault("attrs", {})

#         # compute file hash if file node or function with file
#         node_type = node.get("type")
#         if node_type in ("File","SourceFile","Config"):
#             path = resolve_path_from_node(node, args.repo_dir)
#             if path:
#                 node_debug["attempts"].append({"action":"compute_hash", "path": path})
#                 fullpath = os.path.join(args.repo_dir, path)
#                 if os.path.exists(fullpath):
#                     sha1 = compute_sha1_of_file(fullpath)
#                     if sha1:
#                         node["hash"] = sha1
#                         node_debug["hash"] = sha1
#                     else:
#                         node_debug["hash_error"] = "hash_failed"
#                 else:
#                     node_debug["hash_error"] = "file_not_found"
#             else:
#                 node_debug["hash_error"] = "no_path"

#         elif node_type == "Function":
#             # optional: attach file_hash if file present
#             fpath = attrs.get("file") or attrs.get("path")
#             if not fpath:
#                 q = node.get("qualified_name")
#                 if q and ":" in q and q.endswith(".py"):
#                     fpath = q.split(":",1)[0]
#             if fpath:
#                 node_debug["attempts"].append({"action":"function_file_hash", "file": fpath})
#                 fullfp = os.path.join(args.repo_dir, fpath)
#                 if os.path.exists(fullfp):
#                     h = compute_sha1_of_file(fullfp)
#                     if h:
#                         attrs["file_hash"] = h
#                         node_debug["file_hash"] = h
#                     else:
#                         node_debug["file_hash_error"] = "hash_failed"
#                 else:
#                     node_debug["file_hash_error"] = "file_not_found"

#         # provenance: introduced_by_commit and modified_by_commits
#         top_commit = None
#         modified_commits: List[str] = []

#         # Functions first: use func_commits mapping
#         if node_type == "Function":
#             qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
#             mapping = None
#             if isinstance(func_commits, dict):
#                 mapping = func_commits.get(qname) or func_commits.get(node.get("id"))
#             if not mapping:
#                 # suffix heuristic
#                 short = qname.split(".")[-1] if qname else None
#                 if short:
#                     for k, v in func_commits.items():
#                         if isinstance(k, str) and k.endswith(short):
#                             mapping = v
#                             node_debug["attempts"].append({"action":"heuristic_suffix_match","matched_key":k})
#                             break
#             if mapping:
#                 # mapping may be dict with top_commit or blame_summary
#                 tc = None
#                 if isinstance(mapping, dict):
#                     if mapping.get("top_commit") and isinstance(mapping["top_commit"], dict):
#                         tc = mapping["top_commit"].get("sha") or mapping["top_commit"].get("id")
#                     elif mapping.get("top_commit") and isinstance(mapping["top_commit"], str):
#                         tc = mapping.get("top_commit")
#                     elif mapping.get("top_commit_sha"):
#                         tc = mapping.get("top_commit_sha")
#                     elif mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
#                         ks = list(mapping["blame_summary"].keys())
#                         if ks:
#                             tc = ks[0]
#                     # collect modified commit keys
#                     if mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
#                         modified_commits = list(mapping["blame_summary"].keys())
#                 elif isinstance(mapping, list):
#                     # list of shas
#                     modified_commits = [str(x) for x in mapping]
#                     if modified_commits:
#                         tc = modified_commits[0]
#                 if tc:
#                     node["introduced_by_commit"] = tc
#                     node_debug["introduced_by_commit"] = tc
#                     top_commit = tc
#                 if modified_commits:
#                     node["modified_by_commits"] = modified_commits
#                     node_debug["modified_by_commits"] = modified_commits
#             else:
#                 node_debug["attempts"].append({"action":"no_function_commits_mapping","qname": qname})

#         # Files / Configs: prefer file_index, fallback to git blame if enabled
#         if node_type in ("File","SourceFile","Config"):
#             path = resolve_path_from_node(node, args.repo_dir)
#             if path:
#                 node_debug["attempts"].append({"action":"resolve_file_index","path": path})
#                 finfo = None
#                 if isinstance(file_index, dict):
#                     finfo = file_index.get(path) or file_index.get(os.path.basename(path))
#                 if finfo and isinstance(finfo, dict):
#                     # extract top commit and commit list
#                     tc = None
#                     if finfo.get("top_commit") and isinstance(finfo["top_commit"], dict):
#                         tc = finfo["top_commit"].get("sha") or finfo["top_commit"].get("id")
#                     if not tc and finfo.get("commits") and isinstance(finfo["commits"], list) and finfo["commits"]:
#                         first = finfo["commits"][0]
#                         tc = first.get("sha") if isinstance(first, dict) else (first if isinstance(first, str) else None)
#                     if tc:
#                         node["introduced_by_commit"] = tc
#                         node_debug["introduced_by_commit"] = tc
#                         top_commit = tc
#                     # collect modified commits
#                     if finfo.get("commits") and isinstance(finfo["commits"], list):
#                         modified_commits = [str(x.get("sha") if isinstance(x, dict) else x) for x in finfo["commits"]]
#                     elif finfo.get("blame_summary") and isinstance(finfo["blame_summary"], dict):
#                         modified_commits = list(finfo["blame_summary"].keys())
#                     if modified_commits:
#                         node["modified_by_commits"] = modified_commits
#                         node_debug["modified_by_commits"] = modified_commits
#                 else:
#                     node_debug["attempts"].append({"action":"no_file_index_entry","path": path})
#                     # fallback to git blame if allowed
#                     if args.blame_fallback:
#                         node_debug["attempts"].append({"action":"blame_wholefile_attempt","path": path})
#                         shas, err = run_git_blame_wholefile(args.repo_dir, path)
#                         if err:
#                             node_debug["attempts"].append({"action":"blame_failed","error": err})
#                         else:
#                             node_debug["attempts"].append({"action":"blame_shas_sample","sample": shas[:10]})
#                             if shas:
#                                 top = choose_top_sha_from_blame(shas)
#                                 uniq = list(dict.fromkeys(shas))  # preserve order, unique
#                                 if top:
#                                     node["introduced_by_commit"] = top
#                                     node_debug["introduced_by_commit"] = top
#                                     top_commit = top
#                                 if uniq:
#                                     node["modified_by_commits"] = uniq
#                                     node_debug["modified_by_commits"] = uniq
#                     else:
#                         node_debug["note"] = "file_exists_but_no_file_index_entry; enable --blame-fallback to attach commits"
#             else:
#                 node_debug["attempts"].append({"action":"no_path_for_file_node"})

#         # For Commit nodes: attach PRs if available
#         if node_type == "Commit":
#             sha = (node.get("attrs") or {}).get("sha") or (node.get("id") and node.get("id").split(":",1)[-1])
#             if sha:
#                 node_debug["attempts"].append({"action":"commit_pr_lookup", "sha": sha})
#                 prs_found = find_prs_for_sha(sha, commit_to_prs_norm, commit_meta)
#                 if prs_found:
#                     node["prs"] = prs_found
#                     node_debug["prs"] = prs_found

#         # If we have introduced_by_commit (top_commit), try to attach introduced_by_pr and PR title
#         if (node.get("introduced_by_commit") or top_commit) and not node.get("introduced_by_pr"):
#             tc = node.get("introduced_by_commit") or top_commit
#             if tc:
#                 prs_found = find_prs_for_sha(tc, commit_to_prs_norm, commit_meta)
#                 if prs_found:
#                     node["introduced_by_pr"] = prs_found[0]
#                     prinfo = pr_meta.get(str(prs_found[0]))
#                     if prinfo and prinfo.get("title"):
#                         node["introduced_by_pr_title"] = prinfo.get("title")
#                     node_debug["introduced_by_pr"] = node["introduced_by_pr"]

#         # finalize debug record and count enriched nodes
#         debug["processed"].append(node_debug)
#         if node.get("introduced_by_commit") or node.get("modified_by_commits"):
#             enriched_count += 1

#     # write outputs
#     write_json(args.out, nodes)
#     write_json(args.debug_out, debug)

#     print("[INFO] Enrichment finished.")
#     print(f"[INFO] Processed nodes: {processed}; enriched nodes (have commit/mod list): {enriched_count}")
#     print(f"[INFO] Output written to: {args.out}")
#     print(f"[INFO] Debug written to: {args.debug_out}")

#     # replace original nodes.json if requested
#     if args.replace:
#         try:
#             if args.backup_nodes:
#                 bak = args.nodes + f".bak.{int(datetime.datetime.now().timestamp())}"
#                 write_json(bak, load_json(args.nodes) or {})
#                 print(f"[INFO] Backup of original nodes.json saved to {bak}")
#             write_json(args.nodes, nodes)
#             print(f"[WARN] Replaced original nodes.json with enriched nodes (saved at {args.nodes})")
#         except Exception as e:
#             print(f"[ERROR] Failed to replace nodes.json: {e}", file=sys.stderr)

# if __name__ == "__main__":
#     main()







#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stepQ_enrich_nodes_extended.py (blame-capable complete)

Enrich nodes.json with:
  - hash (file content SHA1)
  - introduced_by_commit (top commit that introduced the node)
  - introduced_by_pr (PR number/title if available)
  - modified_by_commits (list of commits that modified the node / function)

This script uses:
  - results/nodes.json
  - results/function_commits.json
  - results/commits.json
  - results/commit_to_prs.json
  - results/pull_requests.json
  - results/file_index.json (optional)
  - local repo (for git blame when --blame-fallback is passed)

Outputs:
  - results/node_v2.json (by default)
  - results/kg_check/stepQ_debug.json (detailed per-node debug)

Usage example:
  python stepQ_enrich_nodes_extended.py --repo-dir "..\\Ecommerce-Data-MLOps" --blame-fallback
"""
from __future__ import annotations
import argparse
import json
import os
import hashlib
import datetime
import sys
import subprocess
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

SHA_RE_HEX = "0123456789abcdef"

# ---------- IO ----------
def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def iso_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def compute_sha1_of_file(fullpath: str) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with open(fullpath, "rb") as fh:
            while True:
                chunk = fh.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

# ---------- git blame helpers ----------
def run_git_blame_wholefile(repo_dir: str, relpath: str, timeout: int = 30) -> Tuple[List[str], Optional[str]]:
    try:
        full = os.path.join(repo_dir, relpath)
        if not os.path.exists(full):
            return [], "file_not_found"
        out = subprocess.check_output(
            ["git", "-C", repo_dir, "blame", "--line-porcelain", "--", relpath],
            stderr=subprocess.STDOUT,
            timeout=timeout
        )
        text = out.decode("utf-8", errors="replace")
        shas = []
        for line in text.splitlines():
            if not line:
                continue
            parts = line.split()
            if parts and len(parts[0]) >= 7 and all(c.lower() in SHA_RE_HEX for c in parts[0][:7]):
                shas.append(parts[0])
        return shas, None
    except subprocess.CalledProcessError as e:
        out = e.output.decode("utf-8", errors="replace") if getattr(e, "output", None) else str(e)
        return [], f"git_blame_failed:{e.returncode}:{out[:2000]}"
    except Exception as e:
        return [], f"git_blame_exception:{e}"

def choose_top_sha_from_blame(shas: List[str]) -> Optional[str]:
    if not shas:
        return None
    ctr = Counter(shas)
    most_common = ctr.most_common()
    if not most_common:
        return None
    return most_common[0][0]

# ---------- indexing helpers ----------
def build_commit_meta_index(commits: List[Dict]) -> Dict[str, Dict]:
    meta = {}
    if not isinstance(commits, list):
        return meta
    for c in commits:
        if not isinstance(c, dict):
            continue
        sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
        if not sha:
            continue
        msg = c.get("message") or (c.get("commit") and c["commit"].get("message")) or c.get("message_head")
        author = None
        date = None
        if c.get("commit") and isinstance(c["commit"].get("author"), dict):
            date = c["commit"]["author"].get("date")
            if not author:
                author = c["commit"]["author"].get("name")
        if not author and isinstance(c.get("author"), dict):
            author = c["author"].get("login") or c["author"].get("name")
        if not date:
            date = c.get("date") or c.get("author_date")
        meta[sha] = {"sha": sha, "message": msg, "author": author, "date": date}
        if len(sha) >= 7:
            meta[sha[:7]] = meta[sha]
    return meta

def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
    out = {}
    if isinstance(mapping, dict):
        for k, v in mapping.items():
            out[str(k)] = [str(x) for x in v] if v else []
        return out
    if isinstance(mapping, list):
        for it in mapping:
            if not isinstance(it, dict):
                continue
            key = it.get("commit") or it.get("sha")
            prs = it.get("prs") or it.get("pull_requests") or it.get("pr_list")
            if key and prs:
                out[str(key)] = [str(x.get("number") if isinstance(x, dict) else x) for x in prs]
        return out
    return out

def build_pr_meta(prs: List[Dict]) -> Dict[str, Dict]:
    meta = {}
    if not isinstance(prs, list):
        return meta
    for p in prs:
        if isinstance(p, dict):
            num = p.get("number") or p.get("id")
            if num is None:
                continue
            title = p.get("title")
            author = None
            user = p.get("user")
            if isinstance(user, dict):
                author = user.get("login") or user.get("name")
            elif isinstance(user, str):
                author = user
            if not author:
                top_author = p.get("author")
                if isinstance(top_author, dict):
                    author = top_author.get("login") or top_author.get("name")
                elif isinstance(top_author, str):
                    author = top_author
            meta[str(num)] = {"title": title, "author": author}
        else:
            meta[str(p)] = {"title": None, "author": None}
    return meta

def find_prs_for_sha(sha: str, commit_to_prs_norm: Dict[str, List[str]], commit_meta: Dict[str, Dict]) -> List[str]:
    if not sha:
        return []
    if sha in commit_to_prs_norm:
        return commit_to_prs_norm[sha] or []
    if sha.lower() in commit_to_prs_norm:
        return commit_to_prs_norm[sha.lower()] or []
    pref = sha[:7]
    for k in commit_to_prs_norm.keys():
        try:
            if str(k).startswith(pref) or str(k).lower().startswith(pref.lower()):
                return commit_to_prs_norm[k] or []
        except Exception:
            continue
    for full in commit_meta.keys():
        if str(full).startswith(pref) or str(full).lower().startswith(pref.lower()):
            if full in commit_to_prs_norm:
                return commit_to_prs_norm[full] or []
            if str(full).lower() in commit_to_prs_norm:
                return commit_to_prs_norm[str(full).lower()] or []
    return []

# ---------- node helpers ----------
def resolve_path_from_node(node: Dict, repo_dir: str) -> Optional[str]:
    if node.get("path"):
        rel = node.get("path")
        if os.path.isabs(rel):
            try:
                relp = os.path.relpath(rel, repo_dir)
                if not relp.startswith(".."):
                    return relp.replace("\\","/")
            except Exception:
                pass
        return rel.replace("\\","/")
    attrs = node.get("attrs") or {}
    for k in ("file", "path", "filename", "filepath"):
        if attrs.get(k):
            return str(attrs.get(k)).replace("\\","/")
    label = node.get("label")
    if label and "/" in label and label.endswith(".py"):
        return label.replace("\\","/")
    q = node.get("qualified_name")
    if q and q.endswith(".py") and ":" in q:
        return q.split(":",1)[0]
    return None

# ---------- main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default="results/nodes.json")
    p.add_argument("--func-commits", default="results/function_commits.json")
    p.add_argument("--commits", default="results/commits.json")
    p.add_argument("--commit-to-prs", default="results/commit_to_prs.json")
    p.add_argument("--prs", default="results/pull_requests.json")
    p.add_argument("--file-index", default="results/file_index.json")
    p.add_argument("--repo-dir", default=".", help="path to repo root containing .git and source files")
    p.add_argument("--out", default="results/node_v2.json")
    p.add_argument("--debug-out", default="results/kg_check/stepQ_debug.json")
    p.add_argument("--sample-only", type=int, default=0)
    p.add_argument("--replace", action="store_true")
    p.add_argument("--backup-nodes", action="store_true")
    p.add_argument("--blame-fallback", action="store_true", help="If set, run git blame on files missing file_index entries")
    p.add_argument("--blame-timeout", type=int, default=30, help="Timeout seconds for git blame calls")
    args = p.parse_args()

    print("[INFO] Loading inputs...")
    nodes = load_json(args.nodes)
    if nodes is None:
        print(f"[ERROR] nodes.json not found at {args.nodes}")
        return 2
    func_commits = load_json(args.func_commits) or {}
    commits = load_json(args.commits) or []
    commit_to_prs = load_json(args.commit_to_prs) or {}
    prs = load_json(args.prs) or []
    file_index = load_json(args.file_index) or {}

    commit_meta = build_commit_meta_index(commits)
    commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
    pr_meta = build_pr_meta(prs)

    total_nodes = len(nodes)
    sample_limit = args.sample_only if args.sample_only > 0 else None
    processed = 0
    enriched_count = 0
    debug = {"run_ts": iso_now(), "repo_dir": args.repo_dir, "blame_fallback_enabled": bool(args.blame_fallback), "processed": []}

    if args.replace and args.backup_nodes:
        bak = args.nodes + f".bak.{int(datetime.datetime.now().timestamp())}"
        write_json(bak, load_json(args.nodes) or {})
        print(f"[INFO] Backup original nodes.json -> {bak}")

    print(f"[INFO] Processing {total_nodes} nodes (sample_only={args.sample_only}) ...")
    for node in nodes:
        if sample_limit is not None and processed >= sample_limit:
            break
        processed += 1

        node_debug: Dict[str, Any] = {"id": node.get("id"), "type": node.get("type"), "label": node.get("label"), "attempts": []}
        attrs = node.setdefault("attrs", {})

        node_type = node.get("type")
        # compute hash
        if node_type in ("File","SourceFile","Config"):
            path = resolve_path_from_node(node, args.repo_dir)
            if path:
                node_debug["attempts"].append({"action":"compute_hash", "path": path})
                fullpath = os.path.join(args.repo_dir, path)
                if os.path.exists(fullpath):
                    sha1 = compute_sha1_of_file(fullpath)
                    if sha1:
                        node["hash"] = sha1
                        node_debug["hash"] = sha1
                    else:
                        node_debug["hash_error"] = "hash_failed"
                else:
                    node_debug["hash_error"] = "file_not_found"
            else:
                node_debug["hash_error"] = "no_path"

        elif node_type == "Function":
            fpath = attrs.get("file") or attrs.get("path")
            if not fpath:
                q = node.get("qualified_name")
                if q and ":" in q and q.endswith(".py"):
                    fpath = q.split(":",1)[0]
            if fpath:
                node_debug["attempts"].append({"action":"function_file_hash", "file": fpath})
                fullfp = os.path.join(args.repo_dir, fpath)
                if os.path.exists(fullfp):
                    h = compute_sha1_of_file(fullfp)
                    if h:
                        attrs["file_hash"] = h
                        node_debug["file_hash"] = h
                    else:
                        node_debug["file_hash_error"] = "hash_failed"
                else:
                    node_debug["file_hash_error"] = "file_not_found"

        # provenance
        top_commit = None
        modified_commits: List[str] = []

        if node_type == "Function":
            qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
            mapping = None
            if isinstance(func_commits, dict):
                mapping = func_commits.get(qname) or func_commits.get(node.get("id"))
            if not mapping:
                short = qname.split(".")[-1] if qname else None
                if short:
                    for k, v in func_commits.items():
                        if isinstance(k, str) and k.endswith(short):
                            mapping = v
                            node_debug["attempts"].append({"action":"heuristic_suffix_match","matched_key":k})
                            break
            if mapping:
                tc = None
                if isinstance(mapping, dict):
                    if mapping.get("top_commit") and isinstance(mapping["top_commit"], dict):
                        tc = mapping["top_commit"].get("sha") or mapping["top_commit"].get("id")
                    elif mapping.get("top_commit") and isinstance(mapping["top_commit"], str):
                        tc = mapping.get("top_commit")
                    elif mapping.get("top_commit_sha"):
                        tc = mapping.get("top_commit_sha")
                    elif mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
                        ks = list(mapping["blame_summary"].keys())
                        if ks:
                            tc = ks[0]
                    if mapping.get("blame_summary") and isinstance(mapping["blame_summary"], dict):
                        modified_commits = list(mapping["blame_summary"].keys())
                elif isinstance(mapping, list):
                    modified_commits = [str(x) for x in mapping]
                    if modified_commits:
                        tc = modified_commits[0]
                if tc:
                    node["introduced_by_commit"] = tc
                    node_debug["introduced_by_commit"] = tc
                    top_commit = tc
                if modified_commits:
                    node["modified_by_commits"] = modified_commits
                    node_debug["modified_by_commits"] = modified_commits
            else:
                node_debug["attempts"].append({"action":"no_function_commits_mapping","qname": qname})

        if node_type in ("File","SourceFile","Config"):
            path = resolve_path_from_node(node, args.repo_dir)
            if path:
                node_debug["attempts"].append({"action":"resolve_file_index","path": path})
                finfo = None
                if isinstance(file_index, dict):
                    finfo = file_index.get(path) or file_index.get(os.path.basename(path))
                if finfo and isinstance(finfo, dict):
                    tc = None
                    if finfo.get("top_commit") and isinstance(finfo["top_commit"], dict):
                        tc = finfo["top_commit"].get("sha") or finfo["top_commit"].get("id")
                    if not tc and finfo.get("commits") and isinstance(finfo["commits"], list) and finfo["commits"]:
                        first = finfo["commits"][0]
                        tc = first.get("sha") if isinstance(first, dict) else (first if isinstance(first, str) else None)
                    if tc:
                        node["introduced_by_commit"] = tc
                        node_debug["introduced_by_commit"] = tc
                        top_commit = tc
                    if finfo.get("commits") and isinstance(finfo["commits"], list):
                        modified_commits = [str(x.get("sha") if isinstance(x, dict) else x) for x in finfo["commits"]]
                    elif finfo.get("blame_summary") and isinstance(finfo["blame_summary"], dict):
                        modified_commits = list(finfo["blame_summary"].keys())
                    if modified_commits:
                        node["modified_by_commits"] = modified_commits
                        node_debug["modified_by_commits"] = modified_commits
                else:
                    node_debug["attempts"].append({"action":"no_file_index_entry","path": path})
                    if args.blame_fallback:
                        node_debug["attempts"].append({"action":"blame_wholefile_attempt","path": path})
                        shas, err = run_git_blame_wholefile(args.repo_dir, path, timeout=args.blame_timeout)
                        if err:
                            node_debug["attempts"].append({"action":"blame_failed","error": err})
                        else:
                            node_debug["attempts"].append({"action":"blame_shas_sample","sample": shas[:10]})
                            if shas:
                                top = choose_top_sha_from_blame(shas)
                                uniq = list(dict.fromkeys(shas))
                                if top:
                                    node["introduced_by_commit"] = top
                                    node_debug["introduced_by_commit"] = top
                                    top_commit = top
                                if uniq:
                                    node["modified_by_commits"] = uniq
                                    node_debug["modified_by_commits"] = uniq
                    else:
                        node_debug["note"] = "file_exists_but_no_file_index_entry; enable --blame-fallback to attach commits"
            else:
                node_debug["attempts"].append({"action":"no_path_for_file_node"})

        if node_type == "Commit":
            sha = (node.get("attrs") or {}).get("sha") or (node.get("id") and node.get("id").split(":",1)[-1])
            if sha:
                node_debug["attempts"].append({"action":"commit_pr_lookup", "sha": sha})
                prs_found = find_prs_for_sha(sha, commit_to_prs_norm, commit_meta)
                if prs_found:
                    node["prs"] = prs_found
                    node_debug["prs"] = prs_found

        if (node.get("introduced_by_commit") or top_commit) and not node.get("introduced_by_pr"):
            tc = node.get("introduced_by_commit") or top_commit
            if tc:
                prs_found = find_prs_for_sha(tc, commit_to_prs_norm, commit_meta)
                if prs_found:
                    node["introduced_by_pr"] = prs_found[0]
                    prinfo = pr_meta.get(str(prs_found[0]))
                    if prinfo and prinfo.get("title"):
                        node["introduced_by_pr_title"] = prinfo.get("title")
                    node_debug["introduced_by_pr"] = node["introduced_by_pr"]

        debug["processed"].append(node_debug)
        if node.get("introduced_by_commit") or node.get("modified_by_commits"):
            enriched_count += 1

    write_json(args.out, nodes)
    write_json(args.debug_out, debug)

    print("[INFO] Enrichment finished.")
    print(f"[INFO] Processed nodes: {processed}; enriched nodes (have commit/mod list): {enriched_count}")
    print(f"[INFO] Output written to: {args.out}")
    print(f"[INFO] Debug written to: {args.debug_out}")

    if args.replace:
        try:
            if args.backup_nodes:
                bak = args.nodes + f".bak.{int(datetime.datetime.now().timestamp())}"
                write_json(bak, load_json(args.nodes) or {})
                print(f"[INFO] Backup of original nodes.json saved to {bak}")
            write_json(args.nodes, nodes)
            print(f"[WARN] Replaced original nodes.json with enriched nodes (saved at {args.nodes})")
        except Exception as e:
            print(f"[ERROR] Failed to replace nodes.json: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
