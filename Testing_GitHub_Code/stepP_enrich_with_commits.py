# # # # stepP_enrich_with_commits.py
# # # """
# # # Step P — Enrich Function nodes with top commit and PR metadata.

# # # Usage:
# # #   python stepP_enrich_with_commits.py
# # #   (Optional) --results-dir ./results --backup-nodes

# # # Outputs (written to results/ by default):
# # #   - nodes_with_commit.json
# # #   - kg_check/commit_enrichment_report.json

# # # Note: By default it does NOT overwrite results/nodes.json. Use --replace only after review.
# # # """
# # # from __future__ import annotations
# # # import json
# # # import os
# # # import argparse
# # # import datetime
# # # from typing import Any, Dict, List

# # # def load_json(path: str):
# # #     if not os.path.exists(path):
# # #         return None
# # #     with open(path, "r", encoding="utf-8") as fh:
# # #         return json.load(fh)

# # # def write_json(path: str, obj: Any):
# # #     os.makedirs(os.path.dirname(path), exist_ok=True)
# # #     with open(path, "w", encoding="utf-8") as fh:
# # #         json.dump(obj, fh, indent=2, ensure_ascii=False)

# # # def build_commit_meta(commits: List[Dict]) -> Dict[str, Dict]:
# # #     """
# # #     Build a metadata dict keyed by commit SHA.
# # #     Also register lowercase and 7-char-prefix keys pointing to same metadata
# # #     to help matching short-sha lookups.
# # #     """
# # #     meta = {}
# # #     if not isinstance(commits, list):
# # #         return meta
# # #     for c in commits:
# # #         if not isinstance(c, dict):
# # #             continue
# # #         sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
# # #         if not sha:
# # #             sha = c.get("commit_sha") or c.get("commitId")
# # #         if not sha:
# # #             continue
# # #         msg = c.get("message") or (c.get("commit") and c["commit"].get("message"))
# # #         author = None
# # #         date = None
# # #         if c.get("commit") and isinstance(c["commit"].get("author"), dict):
# # #             date = c["commit"]["author"].get("date")
# # #             if not author:
# # #                 author = c["commit"]["author"].get("name")
# # #         if not author and isinstance(c.get("author"), dict):
# # #             author = c["author"].get("login") or c["author"].get("name")
# # #         if not date:
# # #             date = c.get("date")
# # #         entry = {"sha": sha, "message": msg, "author": author, "date": date}
# # #         meta[sha] = entry
# # #         try:
# # #             meta[sha.lower()] = entry
# # #         except Exception:
# # #             pass
# # #         try:
# # #             short = sha[:7]
# # #             if short not in meta:
# # #                 meta[short] = entry
# # #         except Exception:
# # #             pass
# # #     return meta

# # # def normalize_commit_list(raw) -> List[str]:
# # #     if not raw:
# # #         return []
# # #     out = []
# # #     for it in raw:
# # #         if isinstance(it, str):
# # #             out.append(it)
# # #         elif isinstance(it, (int, float)):
# # #             out.append(str(int(it)))
# # #         elif isinstance(it, dict):
# # #             s = it.get("sha") or it.get("id") or (it.get("commit") and it["commit"].get("sha"))
# # #             if not s:
# # #                 s = it.get("commit_sha") or it.get("commitId")
# # #             if s:
# # #                 out.append(s)
# # #     # de-duplicate while preserving order
# # #     seen = set()
# # #     final = []
# # #     for v in out:
# # #         if v not in seen:
# # #             final.append(v)
# # #             seen.add(v)
# # #     return final

# # # def pick_top_commit(shas: List[str], commit_meta: Dict[str, Dict]) -> str | None:
# # #     """
# # #     Pick most recent commit sha from provided list using commit_meta dates.
# # #     If none have dates, return first sha (fallback).
# # #     """
# # #     best = None
# # #     best_date = None
# # #     for sha in shas:
# # #         m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
# # #         if m and m.get("date"):
# # #             try:
# # #                 dt = datetime.datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
# # #             except Exception:
# # #                 dt = None
# # #             if dt and (best_date is None or dt > best_date):
# # #                 best_date = dt
# # #                 best = m.get("sha") or sha
# # #     if best:
# # #         return best
# # #     return shas[0] if shas else None

# # # def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
# # #     """
# # #     Normalize commit_to_prs mapping into dict keyed by commit-sha string -> list of PR numbers (strings).
# # #     Accepts either dict or list forms. This function does NOT try to correct reversed orientation.
# # #     """
# # #     if isinstance(mapping, dict):
# # #         out = {}
# # #         for k, v in mapping.items():
# # #             key = str(k)
# # #             if not v:
# # #                 out[key] = []
# # #                 continue
# # #             try:
# # #                 cleaned = []
# # #                 for x in v:
# # #                     if isinstance(x, dict):
# # #                         num = x.get("number") or x.get("id") or x.get("pr_number")
# # #                         if num is None:
# # #                             prn = x.get("pull_request") or x.get("pull")
# # #                             if isinstance(prn, dict):
# # #                                 num = prn.get("number") or prn.get("id")
# # #                         if num is not None:
# # #                             cleaned.append(str(num))
# # #                         else:
# # #                             cleaned.append(str(x))
# # #                     elif isinstance(x, (int, float)):
# # #                         cleaned.append(str(int(x)))
# # #                     else:
# # #                         cleaned.append(str(x))
# # #                 out[key] = cleaned
# # #             except Exception:
# # #                 out[key] = [str(x) for x in v] if v else []
# # #         return out
# # #     if isinstance(mapping, list):
# # #         out = {}
# # #         for it in mapping:
# # #             if not isinstance(it, dict):
# # #                 continue
# # #             key = it.get("commit") or it.get("sha") or it.get("commit_sha") or it.get("commitId")
# # #             prs = it.get("prs") or it.get("pull_requests") or it.get("prs_list") or it.get("pr_list") or it.get("pulls")
# # #             if key:
# # #                 if not prs:
# # #                     out[str(key)] = []
# # #                 else:
# # #                     cleaned = []
# # #                     for x in prs:
# # #                         if isinstance(x, dict):
# # #                             num = x.get("number") or x.get("id") or x.get("pr_number")
# # #                             if num is not None:
# # #                                 cleaned.append(str(num))
# # #                             else:
# # #                                 cleaned.append(str(x))
# # #                         elif isinstance(x, (int, float)):
# # #                             cleaned.append(str(int(x)))
# # #                         else:
# # #                             cleaned.append(str(x))
# # #                     out[str(key)] = cleaned
# # #         return out
# # #     return {}

# # # def ensure_commit_to_prs_orientation(commit_to_prs_norm: Dict[str, List[str]]) -> Dict[str, List[str]]:
# # #     """
# # #     Sometimes commit_to_prs.json is reversed: keys are PR numbers and values are lists of commits.
# # #     Detect that case (most keys are numeric) and invert mapping into commit->prs.
# # #     Otherwise return mapping unchanged.
# # #     """
# # #     if not isinstance(commit_to_prs_norm, dict):
# # #         return {}
# # #     keys = list(commit_to_prs_norm.keys())
# # #     if not keys:
# # #         return {}
# # #     # detect majority-of-keys-are-numeric -> likely reversed mapping PR->commits
# # #     numeric_keys = sum(1 for k in keys if str(k).isdigit())
# # #     if numeric_keys > len(keys) * 0.6:
# # #         # invert mapping
# # #         inverted = {}
# # #         for pr_key, commits in commit_to_prs_norm.items():
# # #             for comm in (commits or []):
# # #                 cstr = str(comm)
# # #                 inverted.setdefault(cstr, []).append(str(pr_key))
# # #         return inverted
# # #     # otherwise try heuristic: if values look like SHAs (long hex), invert as well
# # #     sample_vals = []
# # #     for v in list(commit_to_prs_norm.values())[:50]:
# # #         if v and isinstance(v, list):
# # #             sample_vals.extend(v[:5])
# # #     hex_like = sum(1 for s in sample_vals if isinstance(s, str) and len(s) >= 7 and all(ch in "0123456789abcdefABCDEF" for ch in s[:7]))
# # #     if hex_like > max(3, len(sample_vals) * 0.2):
# # #         # Looks like values are commits, so invert
# # #         inverted = {}
# # #         for pr_key, commits in commit_to_prs_norm.items():
# # #             for comm in (commits or []):
# # #                 cstr = str(comm)
# # #                 inverted.setdefault(cstr, []).append(str(pr_key))
# # #         return inverted
# # #     # default: assume mapping is commit -> prs already
# # #     return commit_to_prs_norm

# # # def find_matching_pr_list_for_sha(sha: str, commit_to_prs_norm: dict, commit_meta: dict) -> list:
# # #     """
# # #     Try to find a PR list for `sha` using many strategies:
# # #       - exact match
# # #       - lowercase exact match
# # #       - startswith 7-char prefix
# # #       - contains the prefix anywhere in the key
# # #       - match by searching commit_to_prs_norm values (if mapping is strange)
# # #       - match via commit_meta keys
# # #     Returns list of PR numbers (strings) or [].
# # #     """
# # #     if not sha:
# # #         return []
# # #     sha_s = str(sha)
# # #     lower = sha_s.lower()
# # #     pref = sha_s[:7]

# # #     # exact matches
# # #     if sha_s in commit_to_prs_norm:
# # #         return commit_to_prs_norm.get(sha_s) or []
# # #     if lower in commit_to_prs_norm:
# # #         return commit_to_prs_norm.get(lower) or []

# # #     # startswith / contains heuristics
# # #     for k, v in commit_to_prs_norm.items():
# # #         ks = str(k)
# # #         if ks.startswith(pref) or ks.lower().startswith(pref.lower()):
# # #             return v or []
# # #     for k, v in commit_to_prs_norm.items():
# # #         ks = str(k)
# # #         if pref in ks or lower in ks:
# # #             return v or []

# # #     # try resolving pref via commit_meta keys (full sha keys)
# # #     for full in commit_meta.keys():
# # #         fulls = str(full)
# # #         if fulls.startswith(pref) or pref in fulls or fulls.lower().startswith(pref.lower()):
# # #             if fulls in commit_to_prs_norm:
# # #                 return commit_to_prs_norm.get(fulls) or []
# # #             if fulls.lower() in commit_to_prs_norm:
# # #                 return commit_to_prs_norm.get(fulls.lower()) or []

# # #     # try searching values (in case mapping is reversed or nested weirdly)
# # #     try:
# # #         for k, v in commit_to_prs_norm.items():
# # #             if isinstance(v, (list, tuple)):
# # #                 for item in v:
# # #                     if str(item) == sha_s or str(item).startswith(pref) or pref in str(item):
# # #                         # we found the commit inside a value list -> treat the key as PR or container
# # #                         # return the key if it's a PR number, else continue collecting
# # #                         # best effort: if k looks numeric, return [k], else try to find all k's
# # #                         if str(k).isdigit():
# # #                             return [str(k)]
# # #                         else:
# # #                             # return the full list as-is (it's probably the PR list)
# # #                             return v or []
# # #     except Exception:
# # #         pass

# # #     return []

# # # def main():
# # #     p = argparse.ArgumentParser()
# # #     p.add_argument("--results-dir", default="results", help="Directory where artifacts are stored")
# # #     p.add_argument("--replace", action="store_true", help="Replace results/nodes.json with enriched file (dangerous; backup first)")
# # #     p.add_argument("--backup-nodes", action="store_true", help="Make a timestamped backup of results/nodes.json before writing outputs")
# # #     args = p.parse_args()

# # #     R = args.results_dir
# # #     nodes_path = os.path.join(R, "nodes.json")
# # #     func_commits_path = os.path.join(R, "function_commits.json")
# # #     commits_path = os.path.join(R, "commits.json")
# # #     commit_to_prs_path = os.path.join(R, "commit_to_prs.json")
# # #     prs_path = os.path.join(R, "pull_requests.json")
# # #     out_nodes = os.path.join(R, "nodes_with_commit.json")
# # #     report_path = os.path.join(R, "kg_check", "commit_enrichment_report.json")
# # #     debug_path = os.path.join(R, "kg_check", "debug_commit_pr_matching.json")

# # #     nodes = load_json(nodes_path)
# # #     if nodes is None:
# # #         print(f"[ERROR] nodes.json not found at {nodes_path}. Run merge step first.")
# # #         return 1
# # #     function_commits = load_json(func_commits_path) or {}
# # #     commits = load_json(commits_path) or []
# # #     commit_to_prs = load_json(commit_to_prs_path) or {}
# # #     prs = load_json(prs_path) or []

# # #     if args.backup_nodes:
# # #         bak = os.path.join(R, f"nodes.json.bak.{int(datetime.datetime.now().timestamp())}")
# # #         write_json(bak, nodes)
# # #         print(f"[INFO] Backed up nodes.json to {bak}")

# # #     commit_meta = build_commit_meta(commits)

# # #     # Build PR metadata robustly — handle dict entries and plain numbers/strings
# # #     pr_meta = {}
# # #     if isinstance(prs, list):
# # #         for p in prs:
# # #             if isinstance(p, dict):
# # #                 num = p.get("number") or p.get("id")
# # #                 if num is None:
# # #                     num = p.get("pr_number") or p.get("pr") or p.get("pull_request_number")
# # #                 if num is None:
# # #                     continue
# # #                 title = p.get("title")
# # #                 author = None
# # #                 user_field = p.get("user") or p.get("author")
# # #                 if isinstance(user_field, dict):
# # #                     author = user_field.get("login") or user_field.get("name")
# # #                 elif isinstance(user_field, str):
# # #                     author = user_field
# # #                 pr_meta[str(num)] = {"title": title, "author": author}
# # #             else:
# # #                 try:
# # #                     if isinstance(p, (int, float)) or (isinstance(p, str) and str(p).isdigit()):
# # #                         prnum = str(int(p))
# # #                     else:
# # #                         prnum = str(p)
# # #                     pr_meta[prnum] = {"title": None, "author": None}
# # #                 except Exception:
# # #                     pr_meta[str(p)] = {"title": None, "author": None}

# # #     commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
# # #     # ensure orientation: if mapping was PR -> commits, invert it to commit -> PRs
# # #     commit_to_prs_norm = ensure_commit_to_prs_orientation(commit_to_prs_norm)

# # #     enriched = 0
# # #     attached_prs = 0
# # #     missing_mapping = 0
# # #     details = []
# # #     example_top_values = []

# # #     for node in nodes:
# # #         if node.get("type") != "Function":
# # #             continue
# # #         qname = node.get("qualified_name") or node.get("id").split(":",1)[-1]
# # #         commit_list = None
# # #         if isinstance(function_commits, dict):
# # #             commit_list = function_commits.get(qname) or function_commits.get(node.get("id"))
# # #         if not commit_list:
# # #             short = qname.split(".")[-1]
# # #             for k, v in (function_commits or {}).items():
# # #                 if isinstance(k, str) and k.endswith(short):
# # #                     commit_list = v
# # #                     break
# # #         if not commit_list:
# # #             missing_mapping += 1
# # #             details.append({"id": node["id"], "status": "no_commits_found"})
# # #             continue
# # #         shas = normalize_commit_list(commit_list)
# # #         top = pick_top_commit(shas, commit_meta)
# # #         if top:
# # #             example_top_values.append(top)
# # #         if not top:
# # #             details.append({"id": node["id"], "status": "no_valid_sha"})
# # #             continue
# # #         attrs = node.setdefault("attrs", {})
# # #         attrs["introduced_by_commit"] = top
# # #         cm = commit_meta.get(top) or commit_meta.get(str(top).lower()) or commit_meta.get(str(top)[:7]) or {}
# # #         if cm.get("message"):
# # #             attrs["introduced_by_commit_message"] = cm.get("message")
# # #         if cm.get("author"):
# # #             attrs["introduced_by_commit_author"] = cm.get("author")
# # #         if cm.get("date"):
# # #             attrs["introduced_by_commit_date"] = cm.get("date")

# # #         # Robust PR lookup
# # #         pr_list = find_matching_pr_list_for_sha(top, commit_to_prs_norm, commit_meta)

# # #         if pr_list:
# # #             prnum = pr_list[0]
# # #             attrs["introduced_by_pr"] = prnum
# # #             prinfo = pr_meta.get(str(prnum)) or {}
# # #             if prinfo.get("title"):
# # #                 attrs["introduced_by_pr_title"] = prinfo.get("title")
# # #             attached_prs += 1
# # #         attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method": "commit_enrichment", "commit": top, "pr": attrs.get("introduced_by_pr")}]
# # #         enriched += 1
# # #         details.append({"id": node["id"], "commit": top, "pr": attrs.get("introduced_by_pr")})

# # #     # Write enriched nodes file
# # #     write_json(out_nodes, nodes)

# # #     # Diagnostic debug file
# # #     try:
# # #         debug_obj = {
# # #             "commit_to_prs_keys_sample": list(commit_to_prs_norm.keys())[:200],
# # #             "commit_meta_keys_sample": list(commit_meta.keys())[:200],
# # #             "example_top_values": example_top_values[:200],
# # #             "stats": {
# # #                 "commit_to_prs_keys_count": len(commit_to_prs_norm),
# # #                 "commit_meta_keys_count": len(commit_meta),
# # #             }
# # #         }
# # #         write_json(debug_path, debug_obj)
# # #     except Exception:
# # #         pass

# # #     report = {
# # #         "nodes_total": len(nodes),
# # #         "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
# # #         "enriched_functions": enriched,
# # #         "attached_prs": attached_prs,
# # #         "functions_missing_commits_mapping": missing_mapping,
# # #         "sample": details[:200]
# # #     }
# # #     write_json(report_path, report)
# # #     print(f"[INFO] Enrichment complete. Wrote: {out_nodes}")
# # #     print(f"[INFO] Report: {report_path}")
# # #     print(f"[INFO] Summary: {report['enriched_functions']} functions enriched, {report['attached_prs']} PRs attached.")
# # #     if args.replace:
# # #         backup = os.path.join(R, f"nodes.json.replace.bak.{int(datetime.datetime.now().timestamp())}")
# # #         write_json(backup, load_json(nodes_path))
# # #         write_json(nodes_path, nodes)
# # #         print(f"[WARN] Replaced original nodes.json (backup at {backup})")

# # # if __name__ == "__main__":
# # #     main()





# # # stepP_enrich_with_commits.py
# # """
# # Step P — Enrich Function nodes with top commit and PR metadata.

# # Usage:
# #   python stepP_enrich_with_commits.py
# #   (Optional) --results-dir ./results --backup-nodes

# # Outputs (written to results/ by default):
# #   - nodes_with_commit.json
# #   - kg_check/commit_enrichment_report.json

# # Note: By default it does NOT overwrite results/nodes.json. Use --replace only after review.
# # """
# # from __future__ import annotations
# # import json
# # import os
# # import argparse
# # import datetime
# # import re
# # from typing import Any, Dict, List

# # HEX_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")

# # def load_json(path: str):
# #     if not os.path.exists(path):
# #         return None
# #     with open(path, "r", encoding="utf-8") as fh:
# #         return json.load(fh)

# # def write_json(path: str, obj: Any):
# #     os.makedirs(os.path.dirname(path), exist_ok=True)
# #     with open(path, "w", encoding="utf-8") as fh:
# #         json.dump(obj, fh, indent=2, ensure_ascii=False)

# # def build_commit_meta(commits: List[Dict]) -> Dict[str, Dict]:
# #     """
# #     Build a metadata dict keyed by commit SHA.
# #     Also register lowercase and 7-char-prefix keys pointing to same metadata
# #     to help matching short-sha lookups.
# #     """
# #     meta = {}
# #     if not isinstance(commits, list):
# #         return meta
# #     for c in commits:
# #         if not isinstance(c, dict):
# #             continue
# #         sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha"))
# #         if not sha:
# #             sha = c.get("commit_sha") or c.get("commitId")
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
# #         entry = {"sha": sha, "message": msg, "author": author, "date": date}
# #         meta[sha] = entry
# #         try:
# #             meta[sha.lower()] = entry
# #         except Exception:
# #             pass
# #         try:
# #             short = sha[:7]
# #             if short not in meta:
# #                 meta[short] = entry
# #         except Exception:
# #             pass
# #     return meta

# # def normalize_commit_list(raw) -> List[str]:
# #     """
# #     Normalize a raw commit-list value to a list of plausible sha strings.
# #     Filters out non-sha strings (like filenames) and returns only hex-like tokens
# #     length >= 7 (short shas allowed).
# #     """
# #     if not raw:
# #         return []
# #     out = []
# #     for it in raw:
# #         if isinstance(it, str):
# #             s = it.strip()
# #             if HEX_SHA_RE.match(s):
# #                 out.append(s)
# #             else:
# #                 # maybe the string embeds a sha (e.g., "commit:abcd123..."), try to extract
# #                 m = re.search(r"([0-9a-fA-F]{7,40})", s)
# #                 if m:
# #                     out.append(m.group(1))
# #         elif isinstance(it, (int, float)):
# #             out.append(str(int(it)))
# #         elif isinstance(it, dict):
# #             s = it.get("sha") or it.get("id") or (it.get("commit") and it["commit"].get("sha"))
# #             if not s:
# #                 s = it.get("commit_sha") or it.get("commitId")
# #             if isinstance(s, str) and HEX_SHA_RE.match(s):
# #                 out.append(s)
# #             elif isinstance(s, str):
# #                 m = re.search(r"([0-9a-fA-F]{7,40})", s)
# #                 if m:
# #                     out.append(m.group(1))
# #     # de-duplicate while preserving order
# #     seen = set()
# #     final = []
# #     for v in out:
# #         if v not in seen:
# #             final.append(v)
# #             seen.add(v)
# #     return final

# # def pick_top_commit(shas: List[str], commit_meta: Dict[str, Dict]) -> str | None:
# #     """
# #     Pick the most recent commit sha from the provided list using commit_meta dates.
# #     If none have dates, prefer a sha that appears in commit_meta (full/lower/short),
# #     otherwise return None (no valid sha).
# #     """
# #     if not shas:
# #         return None
# #     best = None
# #     best_date = None
# #     for sha in shas:
# #         m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
# #         if m and m.get("date"):
# #             try:
# #                 dt = datetime.datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
# #             except Exception:
# #                 dt = None
# #             if dt and (best_date is None or dt > best_date):
# #                 best_date = dt
# #                 best = m.get("sha") or sha
# #     if best:
# #         return best
# #     # no dated commit found — choose a sha that exists in commit_meta if possible
# #     for sha in shas:
# #         if sha in commit_meta or str(sha).lower() in commit_meta or str(sha)[:7] in commit_meta:
# #             # return the canonical full sha if available in meta, else the sha itself
# #             m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
# #             if m and m.get("sha"):
# #                 return m.get("sha")
# #             return sha
# #     # otherwise none valid
# #     return None

# # def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
# #     """
# #     Normalize commit_to_prs mapping into a dict keyed by commit-sha string,
# #     each value is a list of PR numbers (strings).
# #     Accepts either dict or list forms.
# #     """
# #     if isinstance(mapping, dict):
# #         out = {}
# #         for k, v in mapping.items():
# #             key = str(k)
# #             if not v:
# #                 out[key] = []
# #                 continue
# #             try:
# #                 cleaned = []
# #                 for x in v:
# #                     if isinstance(x, dict):
# #                         num = x.get("number") or x.get("id") or x.get("pr_number")
# #                         if num is None:
# #                             prn = x.get("pull_request") or x.get("pull")
# #                             if isinstance(prn, dict):
# #                                 num = prn.get("number") or prn.get("id")
# #                         if num is not None:
# #                             cleaned.append(str(num))
# #                         else:
# #                             cleaned.append(str(x))
# #                     elif isinstance(x, (int, float)):
# #                         cleaned.append(str(int(x)))
# #                     else:
# #                         cleaned.append(str(x))
# #                 out[key] = cleaned
# #             except Exception:
# #                 out[key] = [str(x) for x in v] if v else []
# #         return out
# #     if isinstance(mapping, list):
# #         out = {}
# #         for it in mapping:
# #             if not isinstance(it, dict):
# #                 continue
# #             key = it.get("commit") or it.get("sha") or it.get("commit_sha") or it.get("commitId")
# #             prs = it.get("prs") or it.get("pull_requests") or it.get("prs_list") or it.get("pr_list") or it.get("pulls")
# #             if key:
# #                 if not prs:
# #                     out[str(key)] = []
# #                 else:
# #                     cleaned = []
# #                     for x in prs:
# #                         if isinstance(x, dict):
# #                             num = x.get("number") or x.get("id") or x.get("pr_number")
# #                             if num is not None:
# #                                 cleaned.append(str(num))
# #                             else:
# #                                 cleaned.append(str(x))
# #                         elif isinstance(x, (int, float)):
# #                             cleaned.append(str(int(x)))
# #                         else:
# #                             cleaned.append(str(x))
# #                     out[str(key)] = cleaned
# #         return out
# #     return {}

# # def ensure_commit_to_prs_orientation(commit_to_prs_norm: Dict[str, List[str]]) -> Dict[str, List[str]]:
# #     """
# #     Detect reversed mapping (PR -> commits) and invert to commit -> PRs if needed.
# #     """
# #     if not isinstance(commit_to_prs_norm, dict):
# #         return {}
# #     keys = list(commit_to_prs_norm.keys())
# #     if not keys:
# #         return {}
# #     numeric_keys = sum(1 for k in keys if str(k).isdigit())
# #     if numeric_keys > len(keys) * 0.6:
# #         inverted = {}
# #         for pr_key, commits in commit_to_prs_norm.items():
# #             for comm in (commits or []):
# #                 cstr = str(comm)
# #                 inverted.setdefault(cstr, []).append(str(pr_key))
# #         return inverted
# #     # heuristic: if values look hex-like, invert
# #     sample_vals = []
# #     for v in list(commit_to_prs_norm.values())[:50]:
# #         if v and isinstance(v, list):
# #             sample_vals.extend(v[:5])
# #     hex_like = sum(1 for s in sample_vals if isinstance(s, str) and len(s) >= 7 and all(ch in "0123456789abcdefABCDEF" for ch in s[:7]))
# #     if hex_like > max(3, len(sample_vals) * 0.2):
# #         inverted = {}
# #         for pr_key, commits in commit_to_prs_norm.items():
# #             for comm in (commits or []):
# #                 cstr = str(comm)
# #                 inverted.setdefault(cstr, []).append(str(pr_key))
# #         return inverted
# #     return commit_to_prs_norm

# # def find_matching_pr_list_for_sha(sha: str, commit_to_prs_norm: dict, commit_meta: dict) -> list:
# #     """
# #     Try to find a PR list for `sha` using multiple strategies.
# #     """
# #     if not sha:
# #         return []
# #     sha_s = str(sha)
# #     lower = sha_s.lower()
# #     pref = sha_s[:7]

# #     # exact/lower
# #     if sha_s in commit_to_prs_norm:
# #         return commit_to_prs_norm.get(sha_s) or []
# #     if lower in commit_to_prs_norm:
# #         return commit_to_prs_norm.get(lower) or []

# #     # startswith / contains heuristics on keys
# #     for k, v in commit_to_prs_norm.items():
# #         ks = str(k)
# #         if ks.startswith(pref) or ks.lower().startswith(pref.lower()):
# #             return v or []
# #     for k, v in commit_to_prs_norm.items():
# #         ks = str(k)
# #         if pref in ks or lower in ks:
# #             return v or []

# #     # resolve via commit_meta keys
# #     for full in commit_meta.keys():
# #         fulls = str(full)
# #         if fulls.startswith(pref) or pref in fulls or fulls.lower().startswith(pref.lower()):
# #             if fulls in commit_to_prs_norm:
# #                 return commit_to_prs_norm.get(fulls) or []
# #             if fulls.lower() in commit_to_prs_norm:
# #                 return commit_to_prs_norm.get(fulls.lower()) or []

# #     # search values (fallback)
# #     try:
# #         for k, v in commit_to_prs_norm.items():
# #             if isinstance(v, (list, tuple)):
# #                 for item in v:
# #                     it = str(item)
# #                     if it == sha_s or it.startswith(pref) or pref in it:
# #                         if str(k).isdigit():
# #                             return [str(k)]
# #                         else:
# #                             return v or []
# #     except Exception:
# #         pass

# #     return []

# # def main():
# #     p = argparse.ArgumentParser()
# #     p.add_argument("--results-dir", default="results", help="Directory where artifacts are stored")
# #     p.add_argument("--replace", action="store_true", help="Replace results/nodes.json with enriched file (dangerous; backup first)")
# #     p.add_argument("--backup-nodes", action="store_true", help="Make a timestamped backup of results/nodes.json before writing outputs")
# #     args = p.parse_args()

# #     R = args.results_dir
# #     nodes_path = os.path.join(R, "nodes.json")
# #     func_commits_path = os.path.join(R, "function_commits.json")
# #     commits_path = os.path.join(R, "commits.json")
# #     commit_to_prs_path = os.path.join(R, "commit_to_prs.json")
# #     prs_path = os.path.join(R, "pull_requests.json")
# #     out_nodes = os.path.join(R, "nodes_with_commit.json")
# #     report_path = os.path.join(R, "kg_check", "commit_enrichment_report.json")
# #     debug_path = os.path.join(R, "kg_check", "debug_commit_pr_matching.json")

# #     nodes = load_json(nodes_path)
# #     if nodes is None:
# #         print(f"[ERROR] nodes.json not found at {nodes_path}. Run merge step first.")
# #         return 1
# #     function_commits = load_json(func_commits_path) or {}
# #     commits = load_json(commits_path) or []
# #     commit_to_prs = load_json(commit_to_prs_path) or {}
# #     prs = load_json(prs_path) or []

# #     if args.backup_nodes:
# #         bak = os.path.join(R, f"nodes.json.bak.{int(datetime.datetime.now().timestamp())}")
# #         write_json(bak, nodes)
# #         print(f"[INFO] Backed up nodes.json to {bak}")

# #     commit_meta = build_commit_meta(commits)

# #     # Build PR metadata robustly — handle dict entries and plain numbers/strings
# #     pr_meta = {}
# #     if isinstance(prs, list):
# #         for p in prs:
# #             if isinstance(p, dict):
# #                 num = p.get("number") or p.get("id")
# #                 if num is None:
# #                     num = p.get("pr_number") or p.get("pr") or p.get("pull_request_number")
# #                 if num is None:
# #                     continue
# #                 title = p.get("title")
# #                 author = None
# #                 user_field = p.get("user") or p.get("author")
# #                 if isinstance(user_field, dict):
# #                     author = user_field.get("login") or user_field.get("name")
# #                 elif isinstance(user_field, str):
# #                     author = user_field
# #                 pr_meta[str(num)] = {"title": title, "author": author}
# #             else:
# #                 try:
# #                     if isinstance(p, (int, float)) or (isinstance(p, str) and str(p).isdigit()):
# #                         prnum = str(int(p))
# #                     else:
# #                         prnum = str(p)
# #                     pr_meta[prnum] = {"title": None, "author": None}
# #                 except Exception:
# #                     pr_meta[str(p)] = {"title": None, "author": None}

# #     commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
# #     commit_to_prs_norm = ensure_commit_to_prs_orientation(commit_to_prs_norm)

# #     enriched = 0
# #     attached_prs = 0
# #     missing_mapping = 0
# #     details = []
# #     example_top_values = []
# #     total_filtered_entries = 0

# #     for node in nodes:
# #         if node.get("type") != "Function":
# #             continue
# #         qname = node.get("qualified_name") or node.get("id").split(":",1)[-1]
# #         commit_list = None
# #         if isinstance(function_commits, dict):
# #             commit_list = function_commits.get(qname) or function_commits.get(node.get("id"))
# #         if not commit_list:
# #             short = qname.split(".")[-1]
# #             for k, v in (function_commits or {}).items():
# #                 if isinstance(k, str) and k.endswith(short):
# #                     commit_list = v
# #                     break
# #         if not commit_list:
# #             missing_mapping += 1
# #             details.append({"id": node["id"], "status": "no_commits_found"})
# #             continue
# #         # normalize and filter non-sha tokens
# #         shas = normalize_commit_list(commit_list)
# #         # count how many entries were filtered (for debug)
# #         filtered = max(0, len(commit_list) - len(shas)) if isinstance(commit_list, (list, tuple)) else 0
# #         total_filtered_entries += filtered

# #         top = pick_top_commit(shas, commit_meta)
# #         if top:
# #             example_top_values.append(top)
# #         if not top:
# #             details.append({"id": node["id"], "status": "no_valid_sha", "filtered_entries": filtered})
# #             continue
# #         attrs = node.setdefault("attrs", {})
# #         attrs["introduced_by_commit"] = top
# #         cm = commit_meta.get(top) or commit_meta.get(str(top).lower()) or commit_meta.get(str(top)[:7]) or {}
# #         if cm.get("message"):
# #             attrs["introduced_by_commit_message"] = cm.get("message")
# #         if cm.get("author"):
# #             attrs["introduced_by_commit_author"] = cm.get("author")
# #         if cm.get("date"):
# #             attrs["introduced_by_commit_date"] = cm.get("date")

# #         pr_list = find_matching_pr_list_for_sha(top, commit_to_prs_norm, commit_meta)

# #         if pr_list:
# #             prnum = pr_list[0]
# #             attrs["introduced_by_pr"] = prnum
# #             prinfo = pr_meta.get(str(prnum)) or {}
# #             if prinfo.get("title"):
# #                 attrs["introduced_by_pr_title"] = prinfo.get("title")
# #             attached_prs += 1
# #         attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method": "commit_enrichment", "commit": top, "pr": attrs.get("introduced_by_pr")}]
# #         enriched += 1
# #         details.append({"id": node["id"], "commit": top, "pr": attrs.get("introduced_by_pr")})

# #     write_json(out_nodes, nodes)

# #     try:
# #         debug_obj = {
# #             "commit_to_prs_keys_sample": list(commit_to_prs_norm.keys())[:200],
# #             "commit_meta_keys_sample": list(commit_meta.keys())[:200],
# #             "example_top_values": example_top_values[:200],
# #             "stats": {
# #                 "commit_to_prs_keys_count": len(commit_to_prs_norm),
# #                 "commit_meta_keys_count": len(commit_meta),
# #                 "total_filtered_commit_list_entries": total_filtered_entries
# #             }
# #         }
# #         write_json(debug_path, debug_obj)
# #     except Exception:
# #         pass

# #     report = {
# #         "nodes_total": len(nodes),
# #         "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
# #         "enriched_functions": enriched,
# #         "attached_prs": attached_prs,
# #         "functions_missing_commits_mapping": missing_mapping,
# #         "sample": details[:200]
# #     }
# #     write_json(report_path, report)
# #     print(f"[INFO] Enrichment complete. Wrote: {out_nodes}")
# #     print(f"[INFO] Report: {report_path}")
# #     print(f"[INFO] Summary: {report['enriched_functions']} functions enriched, {report['attached_prs']} PRs attached.")
# #     if args.replace:
# #         backup = os.path.join(R, f"nodes.json.replace.bak.{int(datetime.datetime.now().timestamp())}")
# #         write_json(backup, load_json(nodes_path))
# #         write_json(nodes_path, nodes)
# #         print(f"[WARN] Replaced original nodes.json (backup at {backup})")

# # if __name__ == "__main__":
# #     main()




# # stepP_enrich_with_commits.py
# """
# Step P — Enrich Function nodes with top commit and PR metadata.
# Robust version: better function_commits discovery + detailed diagnostics.
# """
# from __future__ import annotations
# import json
# import os
# import argparse
# import datetime
# import re
# from typing import Any, Dict, List, Optional, Tuple

# HEX_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")

# def load_json(path: str):
#     if not os.path.exists(path):
#         return None
#     with open(path, "r", encoding="utf-8") as fh:
#         return json.load(fh)

# def write_json(path: str, obj: Any):
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     with open(path, "w", encoding="utf-8") as fh:
#         json.dump(obj, fh, indent=2, ensure_ascii=False)

# def build_commit_meta(commits: List[Dict]) -> Dict[str, Dict]:
#     meta = {}
#     if not isinstance(commits, list):
#         return meta
#     for c in commits:
#         if not isinstance(c, dict):
#             continue
#         sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha")) or c.get("commit_sha") or c.get("commitId")
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
#         entry = {"sha": sha, "message": msg, "author": author, "date": date}
#         meta[sha] = entry
#         try:
#             meta[sha.lower()] = entry
#         except Exception:
#             pass
#         try:
#             short = sha[:7]
#             if short not in meta:
#                 meta[short] = entry
#         except Exception:
#             pass
#     return meta

# def normalize_commit_list(raw) -> List[str]:
#     if not raw:
#         return []
#     out = []
#     for it in raw:
#         if isinstance(it, str):
#             s = it.strip()
#             if HEX_SHA_RE.match(s):
#                 out.append(s)
#             else:
#                 m = re.search(r"([0-9a-fA-F]{7,40})", s)
#                 if m:
#                     out.append(m.group(1))
#         elif isinstance(it, (int, float)):
#             out.append(str(int(it)))
#         elif isinstance(it, dict):
#             s = it.get("sha") or it.get("id") or (it.get("commit") and it["commit"].get("sha")) or it.get("commit_sha") or it.get("commitId")
#             if isinstance(s, str) and HEX_SHA_RE.match(s):
#                 out.append(s)
#             elif isinstance(s, str):
#                 m = re.search(r"([0-9a-fA-F]{7,40})", s)
#                 if m:
#                     out.append(m.group(1))
#     # de-duplicate preserving order
#     seen = set()
#     final = []
#     for v in out:
#         if v not in seen:
#             final.append(v)
#             seen.add(v)
#     return final

# def pick_top_commit(shas: List[str], commit_meta: Dict[str, Dict]) -> Optional[str]:
#     if not shas:
#         return None
#     best = None
#     best_date = None
#     for sha in shas:
#         m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
#         if m and m.get("date"):
#             try:
#                 dt = datetime.datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
#             except Exception:
#                 dt = None
#             if dt and (best_date is None or dt > best_date):
#                 best_date = dt
#                 best = m.get("sha") or sha
#     if best:
#         return best
#     # prefer any sha that exists in commit_meta
#     for sha in shas:
#         if sha in commit_meta or str(sha).lower() in commit_meta or str(sha)[:7] in commit_meta:
#             m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
#             if m and m.get("sha"):
#                 return m.get("sha")
#             return sha
#     return None

# def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
#     if isinstance(mapping, dict):
#         out = {}
#         for k, v in mapping.items():
#             key = str(k)
#             if not v:
#                 out[key] = []
#                 continue
#             try:
#                 cleaned = []
#                 for x in v:
#                     if isinstance(x, dict):
#                         num = x.get("number") or x.get("id") or x.get("pr_number")
#                         if num is None:
#                             prn = x.get("pull_request") or x.get("pull")
#                             if isinstance(prn, dict):
#                                 num = prn.get("number") or prn.get("id")
#                         if num is not None:
#                             cleaned.append(str(num))
#                         else:
#                             cleaned.append(str(x))
#                     elif isinstance(x, (int, float)):
#                         cleaned.append(str(int(x)))
#                     else:
#                         cleaned.append(str(x))
#                 out[key] = cleaned
#             except Exception:
#                 out[key] = [str(x) for x in v] if v else []
#         return out
#     if isinstance(mapping, list):
#         out = {}
#         for it in mapping:
#             if not isinstance(it, dict):
#                 continue
#             key = it.get("commit") or it.get("sha") or it.get("commit_sha") or it.get("commitId")
#             prs = it.get("prs") or it.get("pull_requests") or it.get("prs_list") or it.get("pr_list") or it.get("pulls")
#             if key:
#                 if not prs:
#                     out[str(key)] = []
#                 else:
#                     cleaned = []
#                     for x in prs:
#                         if isinstance(x, dict):
#                             num = x.get("number") or x.get("id") or x.get("pr_number")
#                             if num is not None:
#                                 cleaned.append(str(num))
#                             else:
#                                 cleaned.append(str(x))
#                         elif isinstance(x, (int, float)):
#                             cleaned.append(str(int(x)))
#                         else:
#                             cleaned.append(str(x))
#                     out[str(key)] = cleaned
#         return out
#     return {}

# def ensure_commit_to_prs_orientation(commit_to_prs_norm: Dict[str, List[str]]) -> Dict[str, List[str]]:
#     if not isinstance(commit_to_prs_norm, dict):
#         return {}
#     keys = list(commit_to_prs_norm.keys())
#     if not keys:
#         return {}
#     numeric_keys = sum(1 for k in keys if str(k).isdigit())
#     if numeric_keys > len(keys) * 0.6:
#         inverted = {}
#         for pr_key, commits in commit_to_prs_norm.items():
#             for comm in (commits or []):
#                 cstr = str(comm)
#                 inverted.setdefault(cstr, []).append(str(pr_key))
#         return inverted
#     # heuristic invert if values look like SHAs
#     sample_vals = []
#     for v in list(commit_to_prs_norm.values())[:50]:
#         if v and isinstance(v, list):
#             sample_vals.extend(v[:5])
#     hex_like = sum(1 for s in sample_vals if isinstance(s, str) and len(s) >= 7 and all(ch in "0123456789abcdefABCDEF" for ch in s[:7]))
#     if hex_like > max(3, len(sample_vals) * 0.2):
#         inverted = {}
#         for pr_key, commits in commit_to_prs_norm.items():
#             for comm in (commits or []):
#                 cstr = str(comm)
#                 inverted.setdefault(cstr, []).append(str(pr_key))
#         return inverted
#     return commit_to_prs_norm

# def find_matching_pr_list_for_sha(sha: str, commit_to_prs_norm: dict, commit_meta: dict) -> List[str]:
#     if not sha:
#         return []
#     sha_s = str(sha)
#     lower = sha_s.lower()
#     pref = sha_s[:7]
#     if sha_s in commit_to_prs_norm:
#         return commit_to_prs_norm.get(sha_s) or []
#     if lower in commit_to_prs_norm:
#         return commit_to_prs_norm.get(lower) or []
#     for k, v in commit_to_prs_norm.items():
#         ks = str(k)
#         if ks.startswith(pref) or ks.lower().startswith(pref.lower()):
#             return v or []
#     for k, v in commit_to_prs_norm.items():
#         ks = str(k)
#         if pref in ks or lower in ks:
#             return v or []
#     for full in commit_meta.keys():
#         fulls = str(full)
#         if fulls.startswith(pref) or pref in fulls or fulls.lower().startswith(pref.lower()):
#             if fulls in commit_to_prs_norm:
#                 return commit_to_prs_norm.get(fulls) or []
#             if fulls.lower() in commit_to_prs_norm:
#                 return commit_to_prs_norm.get(fulls.lower()) or []
#     try:
#         for k, v in commit_to_prs_norm.items():
#             if isinstance(v, (list, tuple)):
#                 for item in v:
#                     it = str(item)
#                     if it == sha_s or it.startswith(pref) or pref in it:
#                         if str(k).isdigit():
#                             return [str(k)]
#                         else:
#                             return v or []
#     except Exception:
#         pass
#     return []

# # NEW: robust function -> commit-list discovery
# def get_commit_list_for_node(node: Dict, qname: str, function_commits) -> Tuple[Optional[List], str]:
#     """
#     Return (commit_list_or_None, strategy_string)
#     strategy_string describes how it was found (or why not).
#     Handles various shapes for function_commits (dict, list, reversed, nested).
#     """
#     # 1. direct dict lookup by qualified_name
#     if isinstance(function_commits, dict):
#         # exact by qualified_name or id
#         if qname and qname in function_commits:
#             return function_commits[qname], "dict_exact_qname"
#         nodeid = node.get("id")
#         if nodeid and nodeid in function_commits:
#             return function_commits[nodeid], "dict_exact_nodeid"
#         # direct numeric/inexact: try lowercased keys
#         if qname and qname.lower() in function_commits:
#             return function_commits[qname.lower()], "dict_qname_lower"
#         # try suffix match (short name)
#         short = qname.split(".")[-1] if qname else None
#         if short:
#             for k, v in function_commits.items():
#                 if isinstance(k, str) and k.endswith(short):
#                     return v, "dict_suffix_match"
#             # try contains
#             for k, v in function_commits.items():
#                 if isinstance(k, str) and short in k:
#                     return v, "dict_contains_short"
#         # maybe keys are numeric PRs mapping to commits (reversed)
#         # detect if values are strings/sha lists: handled elsewhere (ensure orientation)
#     # 2. function_commits is a list of dicts (common when produced differently)
#     if isinstance(function_commits, list):
#         # try to find by qname or node id fields inside list items
#         for it in function_commits:
#             if not isinstance(it, dict):
#                 continue
#             # common field names
#             cand_keys = ["qualified_name", "qualified", "qname", "function", "id", "node_id", "key", "name"]
#             for ck in cand_keys:
#                 if ck in it and str(it.get(ck)) == str(qname):
#                     # take its 'commits' / 'shas' / 'values' or the object itself
#                     for val_key in ("commits", "shas", "commit_list", "values", "prs"):
#                         if val_key in it:
#                             return it[val_key], f"list_item_match_{ck}_{val_key}"
#                     # fallback: if item itself is list-like, return it
#                     return it, f"list_item_match_{ck}"
#         # try fuzzy match: item key contains short name
#         short = qname.split(".")[-1] if qname else None
#         if short:
#             for it in function_commits:
#                 if isinstance(it, dict):
#                     for v in it.values():
#                         if isinstance(v, str) and short in v:
#                             # maybe it's the key inside; return 'commits' property if present
#                             if "commits" in it:
#                                 return it["commits"], "list_fuzzy_commits"
#                             # else return None
#         # try if list contains mapping { "function": "name", "commits":[..] } etc — handled above
#     # 3. try scanning function_commits keys/values for node id or function short name
#     try:
#         short = qname.split(".")[-1] if qname else None
#         if isinstance(function_commits, dict):
#             # search in values for something referencing the function name
#             for k, v in function_commits.items():
#                 if isinstance(k, str) and qname and qname in k:
#                     return v, "dict_key_contains_qname"
#                 if short and isinstance(k, str) and short in k:
#                     return v, "dict_key_contains_short"
#             # sometimes mapping is reversed: keys are prs and values are lists of commits -> handled by ensure_commit_to_prs_orientation earlier
#     except Exception:
#         pass
#     # 4. last resort: check node.attrs for commit-like fields
#     attrs = node.get("attrs") or {}
#     for candidate_field in ("introduced_by_commit", "commits", "commit", "source_commit"):
#         if candidate_field in attrs:
#             val = attrs[candidate_field]
#             if isinstance(val, (list, tuple)):
#                 return val, "attrs_field_list"
#             return [val], "attrs_field_single"
#     # couldn't find
#     return None, "not_found"

# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--results-dir", default="results", help="Directory where artifacts are stored")
#     p.add_argument("--replace", action="store_true", help="Replace results/nodes.json with enriched file (dangerous; backup first)")
#     p.add_argument("--backup-nodes", action="store_true", help="Make a timestamped backup of results/nodes.json before writing outputs")
#     args = p.parse_args()

#     R = args.results_dir
#     nodes_path = os.path.join(R, "nodes.json")
#     func_commits_path = os.path.join(R, "function_commits.json")
#     commits_path = os.path.join(R, "commits.json")
#     commit_to_prs_path = os.path.join(R, "commit_to_prs.json")
#     prs_path = os.path.join(R, "pull_requests.json")
#     out_nodes = os.path.join(R, "nodes_with_commit.json")
#     report_path = os.path.join(R, "kg_check", "commit_enrichment_report.json")
#     debug_path = os.path.join(R, "kg_check", "debug_commit_pr_matching.json")

#     nodes = load_json(nodes_path)
#     if nodes is None:
#         print(f"[ERROR] nodes.json not found at {nodes_path}. Run merge step first.")
#         return 1
#     function_commits = load_json(func_commits_path)
#     commits = load_json(commits_path) or []
#     commit_to_prs = load_json(commit_to_prs_path) or {}
#     prs = load_json(prs_path) or []

#     if args.backup_nodes:
#         bak = os.path.join(R, f"nodes.json.bak.{int(datetime.datetime.now().timestamp())}")
#         write_json(bak, nodes)
#         print(f"[INFO] Backed up nodes.json to {bak}")

#     # build helper metadata
#     commit_meta = build_commit_meta(commits)
#     pr_meta = {}
#     if isinstance(prs, list):
#         for p in prs:
#             if isinstance(p, dict):
#                 num = p.get("number") or p.get("id") or p.get("pr_number") or p.get("pr")
#                 if num is None:
#                     continue
#                 title = p.get("title")
#                 author = None
#                 user_field = p.get("user") or p.get("author")
#                 if isinstance(user_field, dict):
#                     author = user_field.get("login") or user_field.get("name")
#                 elif isinstance(user_field, str):
#                     author = user_field
#                 pr_meta[str(num)] = {"title": title, "author": author}
#             else:
#                 try:
#                     if isinstance(p, (int, float)) or (isinstance(p, str) and str(p).isdigit()):
#                         prnum = str(int(p))
#                     else:
#                         prnum = str(p)
#                     pr_meta[prnum] = {"title": None, "author": None}
#                 except Exception:
#                     pr_meta[str(p)] = {"title": None, "author": None}

#     commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
#     # make sure orientation is commit->prs
#     def ensure_orientation(x): 
#         # inline simple version to avoid circular imports
#         if not isinstance(x, dict):
#             return {}
#         keys = list(x.keys())
#         numeric_keys = sum(1 for k in keys if str(k).isdigit())
#         if numeric_keys > len(keys) * 0.6:
#             inverted = {}
#             for pr_key, commits_list in x.items():
#                 for comm in (commits_list or []):
#                     inverted.setdefault(str(comm), []).append(str(pr_key))
#             return inverted
#         # heuristic invert if values are hex-like
#         sample_vals = []
#         for v in list(x.values())[:50]:
#             if v and isinstance(v, list):
#                 sample_vals.extend(v[:5])
#         hex_like = sum(1 for s in sample_vals if isinstance(s, str) and len(s) >= 7 and all(ch in "0123456789abcdefABCDEF" for ch in s[:7]))
#         if hex_like > max(3, len(sample_vals) * 0.2):
#             inverted = {}
#             for pr_key, commits_list in x.items():
#                 for comm in (commits_list or []):
#                     inverted.setdefault(str(comm), []).append(str(pr_key))
#             return inverted
#         return x
#     commit_to_prs_norm = ensure_orientation(commit_to_prs_norm)

#     enriched = 0
#     attached_prs = 0
#     missing_mapping = 0
#     details = []
#     debug_nodes_sample = []
#     func_commits_sample = {}
#     diag_entries = []

#     # build a small sample of the function_commits structure for debugging
#     if function_commits is not None:
#         try:
#             if isinstance(function_commits, dict):
#                 func_commits_sample = {k: (function_commits[k] if isinstance(function_commits[k], (list,dict)) else str(function_commits[k])) for i,k in enumerate(list(function_commits.keys())[:200])}
#             elif isinstance(function_commits, list):
#                 func_commits_sample = {"type":"list","len": len(function_commits), "first_items": function_commits[:50]}
#             else:
#                 func_commits_sample = {"type": str(type(function_commits)), "repr": str(function_commits)[:500]}
#         except Exception as e:
#             func_commits_sample = {"err": str(e)}

#     # iterate nodes
#     for i, node in enumerate(nodes):
#         if node.get("type") != "Function":
#             continue
#         if len(debug_nodes_sample) < 40:
#             debug_nodes_sample.append({"id": node.get("id"), "qualified_name": node.get("qualified_name"), "attrs_keys": list((node.get("attrs") or {}).keys())})
#         qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
#         commit_list, strategy = get_commit_list_for_node(node, qname, function_commits)
#         diag = {"id": node.get("id"), "qualified_name": qname, "strategy": strategy}
#         if not commit_list:
#             missing_mapping += 1
#             diag["status"] = "no_commits_found"
#             diag_entries.append(diag)
#             details.append({"id": node["id"], "status": "no_commits_found"})
#             continue
#         # normalize commit list to plausible SHAs
#         shas = normalize_commit_list(commit_list)
#         diag["raw_commit_list_sample"] = (commit_list[:5] if isinstance(commit_list, list) else str(commit_list)[:200])
#         diag["normalized_shas_sample"] = shas[:10]
#         if not shas:
#             diag["status"] = "no_valid_shas_after_normalize"
#             diag_entries.append(diag)
#             details.append({"id": node["id"], "status": "no_valid_shas_after_normalize"})
#             continue
#         top = pick_top_commit(shas, commit_meta)
#         diag["picked_top"] = top
#         if not top:
#             diag["status"] = "no_top_commit_found"
#             diag_entries.append(diag)
#             details.append({"id": node["id"], "status": "no_top_commit_found"})
#             continue
#         # success path: enrich node
#         attrs = node.setdefault("attrs", {})
#         attrs["introduced_by_commit"] = top
#         cm = commit_meta.get(top) or commit_meta.get(str(top).lower()) or commit_meta.get(str(top)[:7]) or {}
#         if cm.get("message"):
#             attrs["introduced_by_commit_message"] = cm.get("message")
#         if cm.get("author"):
#             attrs["introduced_by_commit_author"] = cm.get("author")
#         if cm.get("date"):
#             attrs["introduced_by_commit_date"] = cm.get("date")
#         # lookup PRs for this commit
#         pr_list = find_matching_pr_list_for_sha(top, commit_to_prs_norm, commit_meta) if commit_to_prs_norm else []
#         if pr_list:
#             prnum = pr_list[0]
#             attrs["introduced_by_pr"] = prnum
#             prinfo = pr_meta.get(str(prnum)) or {}
#             if prinfo.get("title"):
#                 attrs["introduced_by_pr_title"] = prinfo.get("title")
#             attached_prs += 1
#         attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method": "commit_enrichment", "commit": top, "pr": attrs.get("introduced_by_pr")}]
#         enriched += 1
#         details.append({"id": node["id"], "commit": top, "pr": attrs.get("introduced_by_pr")})
#         diag["status"] = "enriched"
#         diag_entries.append(diag)

#     # write outputs
#     write_json(out_nodes, nodes)

#     # debug object: sample nodes, function_commits sample, and diagnostics per-node for first 200
#     try:
#         debug_obj = {
#             "nodes_sample": debug_nodes_sample,
#             "function_commits_sample": func_commits_sample,
#             "diag_entries_sample": diag_entries[:200],
#             "commit_to_prs_keys_sample": list(commit_to_prs_norm.keys())[:200] if isinstance(commit_to_prs_norm, dict) else [],
#             "commit_meta_keys_sample": list(commit_meta.keys())[:200],
#             "stats": {
#                 "nodes_total": len(nodes),
#                 "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
#                 "enriched_functions": enriched,
#                 "attached_prs": attached_prs,
#                 "functions_missing_commits_mapping": missing_mapping,
#             }
#         }
#         write_json(debug_path, debug_obj)
#     except Exception:
#         pass

#     report = {
#         "nodes_total": len(nodes),
#         "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
#         "enriched_functions": enriched,
#         "attached_prs": attached_prs,
#         "functions_missing_commits_mapping": missing_mapping,
#         "sample": details[:200]
#     }
#     write_json(report_path, report)
#     print(f"[INFO] Enrichment complete. Wrote: {out_nodes}")
#     print(f"[INFO] Report: {report_path}")
#     print(f"[INFO] Summary: {report['enriched_functions']} functions enriched, {report['attached_prs']} PRs attached.")
#     if args.replace:
#         backup = os.path.join(R, f"nodes.json.replace.bak.{int(datetime.datetime.now().timestamp())}")
#         write_json(backup, load_json(nodes_path))
#         write_json(nodes_path, nodes)
#         print(f"[WARN] Replaced original nodes.json (backup at {backup})")

# if __name__ == "__main__":
#     main()






# stepP_enrich_with_commits.py
"""
Step P — Enrich Function nodes with top commit and PR metadata.

Usage:
  python stepP_enrich_with_commits.py
  (Optional) --results-dir ./results --backup-nodes

Outputs (written to results/ by default):
  - nodes_with_commit.json
  - kg_check/commit_enrichment_report.json

Note: By default it does NOT overwrite results/nodes.json. Use --replace only after review.
"""
from __future__ import annotations
import json
import os
import argparse
import datetime
import re
from typing import Any, Dict, List, Optional, Tuple

HEX_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")

def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def build_commit_meta(commits: List[Dict]) -> Dict[str, Dict]:
    meta = {}
    if not isinstance(commits, list):
        return meta
    for c in commits:
        if not isinstance(c, dict):
            continue
        sha = c.get("sha") or c.get("id") or (c.get("commit") and c["commit"].get("sha")) or c.get("commit_sha") or c.get("commitId")
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
        entry = {"sha": sha, "message": msg, "author": author, "date": date}
        meta[sha] = entry
        try:
            meta[sha.lower()] = entry
        except Exception:
            pass
        try:
            short = sha[:7]
            if short not in meta:
                meta[short] = entry
        except Exception:
            pass
    return meta

def normalize_commit_list(raw) -> List[str]:
    """
    Normalize a raw commit-list value to a list of plausible sha strings.
    Filters out non-sha strings and extracts embedded SHAs when possible.
    """
    if not raw:
        return []
    out = []
    if isinstance(raw, dict):
        # if it's a dict mapping sha -> info (like blame_summary), return keys
        all_keys = list(raw.keys())
        if all(isinstance(k, str) and re.search(r"[0-9a-fA-F]{7}", k) for k in all_keys):
            out.extend(all_keys)
    elif isinstance(raw, str):
        s = raw.strip()
        m = re.findall(r"([0-9a-fA-F]{7,40})", s)
        out.extend(m)
    else:
        for it in raw:
            if isinstance(it, str):
                s = it.strip()
                if HEX_SHA_RE.match(s):
                    out.append(s)
                else:
                    m = re.search(r"([0-9a-fA-F]{7,40})", s)
                    if m:
                        out.append(m.group(1))
            elif isinstance(it, (int, float)):
                out.append(str(int(it)))
            elif isinstance(it, dict):
                s = it.get("sha") or it.get("id") or (it.get("commit") and it["commit"].get("sha")) or it.get("top_commit") or it.get("commit_sha") or it.get("commitId")
                if isinstance(s, dict):
                    # nested object: maybe s itself contains sha field
                    s2 = s.get("sha") or s.get("id")
                    if isinstance(s2, str):
                        if HEX_SHA_RE.match(s2):
                            out.append(s2)
                        else:
                            m = re.search(r"([0-9a-fA-F]{7,40})", s2)
                            if m:
                                out.append(m.group(1))
                elif isinstance(s, str):
                    if HEX_SHA_RE.match(s):
                        out.append(s)
                    else:
                        m = re.search(r"([0-9a-fA-F]{7,40})", s)
                        if m:
                            out.append(m.group(1))
                else:
                    # maybe the dict is a mapping from sha->info (blame_summary)
                    keys = list(it.keys())
                    if keys and all(isinstance(k, str) and re.search(r"[0-9a-fA-F]{7}", k) for k in keys):
                        out.extend(keys)
    # de-duplicate preserving order
    seen = set()
    final = []
    for v in out:
        if v not in seen:
            final.append(v)
            seen.add(v)
    return final

def pick_top_commit(shas: List[str], commit_meta: Dict[str, Dict]) -> Optional[str]:
    if not shas:
        return None
    best = None
    best_date = None
    for sha in shas:
        m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
        if m and m.get("date"):
            try:
                dt = datetime.datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
            except Exception:
                dt = None
            if dt and (best_date is None or dt > best_date):
                best_date = dt
                best = m.get("sha") or sha
    if best:
        return best
    # prefer any sha that exists in commit_meta
    for sha in shas:
        if sha in commit_meta or str(sha).lower() in commit_meta or str(sha)[:7] in commit_meta:
            m = commit_meta.get(sha) or commit_meta.get(str(sha).lower()) or commit_meta.get(str(sha)[:7])
            if m and m.get("sha"):
                return m.get("sha")
            return sha
    return None

def normalize_commit_to_prs(mapping) -> Dict[str, List[str]]:
    if isinstance(mapping, dict):
        out = {}
        for k, v in mapping.items():
            key = str(k)
            if not v:
                out[key] = []
                continue
            try:
                cleaned = []
                for x in v:
                    if isinstance(x, dict):
                        num = x.get("number") or x.get("id") or x.get("pr_number")
                        if num is None:
                            prn = x.get("pull_request") or x.get("pull")
                            if isinstance(prn, dict):
                                num = prn.get("number") or prn.get("id")
                        if num is not None:
                            cleaned.append(str(num))
                        else:
                            cleaned.append(str(x))
                    elif isinstance(x, (int, float)):
                        cleaned.append(str(int(x)))
                    else:
                        cleaned.append(str(x))
                out[key] = cleaned
            except Exception:
                out[key] = [str(x) for x in v] if v else []
        return out
    if isinstance(mapping, list):
        out = {}
        for it in mapping:
            if not isinstance(it, dict):
                continue
            key = it.get("commit") or it.get("sha") or it.get("commit_sha") or it.get("commitId")
            prs = it.get("prs") or it.get("pull_requests") or it.get("prs_list") or it.get("pr_list") or it.get("pulls")
            if key:
                if not prs:
                    out[str(key)] = []
                else:
                    cleaned = []
                    for x in prs:
                        if isinstance(x, dict):
                            num = x.get("number") or x.get("id") or x.get("pr_number")
                            if num is not None:
                                cleaned.append(str(num))
                            else:
                                cleaned.append(str(x))
                        elif isinstance(x, (int, float)):
                            cleaned.append(str(int(x)))
                        else:
                            cleaned.append(str(x))
                    out[str(key)] = cleaned
        return out
    return {}

def ensure_commit_to_prs_orientation(commit_to_prs_norm: Dict[str, List[str]]) -> Dict[str, List[str]]:
    if not isinstance(commit_to_prs_norm, dict):
        return {}
    keys = list(commit_to_prs_norm.keys())
    if not keys:
        return {}
    numeric_keys = sum(1 for k in keys if str(k).isdigit())
    if numeric_keys > len(keys) * 0.6:
        inverted = {}
        for pr_key, commits in commit_to_prs_norm.items():
            for comm in (commits or []):
                cstr = str(comm)
                inverted.setdefault(cstr, []).append(str(pr_key))
        return inverted
    # heuristic invert if values look like SHAs
    sample_vals = []
    for v in list(commit_to_prs_norm.values())[:50]:
        if v and isinstance(v, list):
            sample_vals.extend(v[:5])
    hex_like = sum(1 for s in sample_vals if isinstance(s, str) and len(s) >= 7 and all(ch in "0123456789abcdefABCDEF" for ch in s[:7]))
    if hex_like > max(3, len(sample_vals) * 0.2):
        inverted = {}
        for pr_key, commits in commit_to_prs_norm.items():
            for comm in (commits or []):
                cstr = str(comm)
                inverted.setdefault(cstr, []).append(str(pr_key))
        return inverted
    return commit_to_prs_norm

def find_matching_pr_list_for_sha(sha: str, commit_to_prs_norm: dict, commit_meta: dict) -> List[str]:
    if not sha:
        return []
    sha_s = str(sha)
    lower = sha_s.lower()
    pref = sha_s[:7]
    if sha_s in commit_to_prs_norm:
        return commit_to_prs_norm.get(sha_s) or []
    if lower in commit_to_prs_norm:
        return commit_to_prs_norm.get(lower) or []
    for k, v in commit_to_prs_norm.items():
        ks = str(k)
        if ks.startswith(pref) or ks.lower().startswith(pref.lower()):
            return v or []
    for k, v in commit_to_prs_norm.items():
        ks = str(k)
        if pref in ks or lower in ks:
            return v or []
    for full in commit_meta.keys():
        fulls = str(full)
        if fulls.startswith(pref) or pref in fulls or fulls.lower().startswith(pref.lower()):
            if fulls in commit_to_prs_norm:
                return commit_to_prs_norm.get(fulls) or []
            if fulls.lower() in commit_to_prs_norm:
                return commit_to_prs_norm.get(fulls.lower()) or []
    try:
        for k, v in commit_to_prs_norm.items():
            if isinstance(v, (list, tuple)):
                for item in v:
                    it = str(item)
                    if it == sha_s or it.startswith(pref) or pref in it:
                        if str(k).isdigit():
                            return [str(k)]
                        else:
                            return v or []
    except Exception:
        pass
    return []

def resolve_commit_list_value(v) -> Optional[List]:
    """
    Given a single value from function_commits mapping, try to return a list-like
    of commit SHAs or None.
    Handles shapes seen in your repo: dict with top_commit, dict with blame_summary,
    dict mapping sha->info, lists of shas, etc.
    """
    if v is None:
        return None
    # direct list of strings/objects
    if isinstance(v, list):
        return v
    # if it's a dict, check common keys
    if isinstance(v, dict):
        # top_commit: { "sha": ... }
        tc = v.get("top_commit")
        if isinstance(tc, dict) and tc.get("sha"):
            return [tc.get("sha")]
        if isinstance(tc, str) and HEX_SHA_RE.match(tc):
            return [tc]
        # blame_summary: mapping from sha -> details
        bs = v.get("blame_summary") or v.get("blame") or v.get("blame_summary_map")
        if isinstance(bs, dict) and bs:
            # keys are SHAs
            keys = [k for k in bs.keys() if isinstance(k, str)]
            if keys:
                return keys
        # maybe v directly maps sha->info (no blame_summary key)
        keys = [k for k in v.keys() if isinstance(k, str) and re.search(r"[0-9a-fA-F]{7}", k)]
        if keys:
            return keys
        # other possible structures: 'commits', 'shas', 'top_commits'
        for candidate in ("commits", "shas", "sha_list", "top_commits", "values"):
            if candidate in v:
                return v[candidate]
        # sometimes top_commit is nested under top_commit.sha or top_commit_id
        if v.get("top_commit_sha"):
            return [v.get("top_commit_sha")]
        if v.get("top") and isinstance(v.get("top"), dict) and v["top"].get("sha"):
            return [v["top"].get("sha")]
        # fallback: not recognized
        return None
    # string: try extract sha
    if isinstance(v, str):
        m = re.findall(r"([0-9a-fA-F]{7,40})", v)
        return m if m else None
    # numbers etc -> ignore
    return None

def get_commit_list_for_node(node: Dict, qname: str, function_commits) -> Tuple[Optional[List], str]:
    """
    Robust resolver for various shapes of function_commits.
    Returns (commit_list_or_None, strategy_str).
    """
    if function_commits is None:
        return None, "function_commits_missing"

    # direct dict mapping (common)
    if isinstance(function_commits, dict):
        # 1) exact qname key
        if qname and qname in function_commits:
            v = function_commits[qname]
            resolved = resolve_commit_list_value(v)
            if resolved:
                return resolved, "dict_exact_qname"
            # if value is dict with 'top_commit' etc but resolve returned None, explicitly attempt the dict logic
            if isinstance(v, dict):
                # if dict contains file/lineno/top_commit structure, handle that
                tc = v.get("top_commit")
                if isinstance(tc, dict) and tc.get("sha"):
                    return [tc.get("sha")], "dict_exact_qname_top_commit"
            # else no commits found
        # 2) try node id
        nodeid = node.get("id")
        if nodeid and nodeid in function_commits:
            v = function_commits[nodeid]
            resolved = resolve_commit_list_value(v)
            if resolved:
                return resolved, "dict_exact_nodeid"
        # 3) suffix match (short function name)
        short = qname.split(".")[-1] if qname else None
        if short:
            # endswith
            for k, v in function_commits.items():
                if isinstance(k, str) and k.endswith(short):
                    resolved = resolve_commit_list_value(v)
                    if resolved:
                        return resolved, "dict_suffix_match"
            # contains
            for k, v in function_commits.items():
                if isinstance(k, str) and short in k:
                    resolved = resolve_commit_list_value(v)
                    if resolved:
                        return resolved, "dict_contains_short"
        # 4) maybe value itself is a map of sha->info; try to find any entry whose key references qname
        for k, v in function_commits.items():
            if isinstance(v, dict):
                # if v is a mapping of sha->info, try keys
                keys = [kk for kk in v.keys() if isinstance(kk, str) and re.search(r"[0-9a-fA-F]{7}", kk)]
                if keys:
                    # return these SHAs
                    return keys, "dict_values_are_sha_map"
        return None, "dict_no_match"

    # list-shape function_commits (list of entries)
    if isinstance(function_commits, list):
        # try to find an item that matches qname
        for it in function_commits:
            if not isinstance(it, dict):
                continue
            # common fields where qualified name may be stored
            for ck in ("qualified_name", "qualified", "qname", "function", "id", "name", "key"):
                if ck in it and str(it.get(ck)) == str(qname):
                    # extract commits from item
                    for val_key in ("commits", "shas", "commit_list", "values", "top_commits"):
                        if val_key in it:
                            return resolve_commit_list_value(it[val_key]), f"list_item_match_{ck}_{val_key}"
                    # maybe the item itself is mapping sha->info
                    resolved = resolve_commit_list_value(it)
                    if resolved:
                        return resolved, f"list_item_match_{ck}_fallback"
        # maybe list contains items that mention the short name
        short = qname.split(".")[-1] if qname else None
        if short:
            for it in function_commits:
                if isinstance(it, dict):
                    for v in it.values():
                        if isinstance(v, str) and short in v:
                            if "commits" in it:
                                return resolve_commit_list_value(it["commits"]), "list_fuzzy_commits"
        return None, "list_no_match"

    # unknown type
    return None, "unknown_type"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results-dir", default="results", help="Directory where artifacts are stored")
    p.add_argument("--replace", action="store_true", help="Replace results/nodes.json with enriched file (dangerous; backup first)")
    p.add_argument("--backup-nodes", action="store_true", help="Make a timestamped backup of results/nodes.json before writing outputs")
    args = p.parse_args()

    R = args.results_dir
    nodes_path = os.path.join(R, "nodes.json")
    func_commits_path = os.path.join(R, "function_commits.json")
    commits_path = os.path.join(R, "commits.json")
    commit_to_prs_path = os.path.join(R, "commit_to_prs.json")
    prs_path = os.path.join(R, "pull_requests.json")
    out_nodes = os.path.join(R, "nodes_with_commit.json")
    report_path = os.path.join(R, "kg_check", "commit_enrichment_report.json")
    debug_path = os.path.join(R, "kg_check", "debug_commit_pr_matching.json")

    nodes = load_json(nodes_path)
    if nodes is None:
        print(f"[ERROR] nodes.json not found at {nodes_path}. Run merge step first.")
        return 1
    function_commits = load_json(func_commits_path)
    commits = load_json(commits_path) or []
    commit_to_prs = load_json(commit_to_prs_path) or {}
    prs = load_json(prs_path) or []

    if args.backup_nodes:
        bak = os.path.join(R, f"nodes.json.bak.{int(datetime.datetime.now().timestamp())}")
        write_json(bak, nodes)
        print(f"[INFO] Backed up nodes.json to {bak}")

    commit_meta = build_commit_meta(commits)

    # Build PR metadata
    pr_meta = {}
    if isinstance(prs, list):
        for p in prs:
            if isinstance(p, dict):
                num = p.get("number") or p.get("id") or p.get("pr_number") or p.get("pr")
                if num is None:
                    continue
                title = p.get("title")
                author = None
                user_field = p.get("user") or p.get("author")
                if isinstance(user_field, dict):
                    author = user_field.get("login") or user_field.get("name")
                elif isinstance(user_field, str):
                    author = user_field
                pr_meta[str(num)] = {"title": title, "author": author}
            else:
                try:
                    if isinstance(p, (int, float)) or (isinstance(p, str) and str(p).isdigit()):
                        prnum = str(int(p))
                    else:
                        prnum = str(p)
                    pr_meta[prnum] = {"title": None, "author": None}
                except Exception:
                    pr_meta[str(p)] = {"title": None, "author": None}

    commit_to_prs_norm = normalize_commit_to_prs(commit_to_prs)
    commit_to_prs_norm = ensure_commit_to_prs_orientation(commit_to_prs_norm)

    enriched = 0
    attached_prs = 0
    missing_mapping = 0
    details = []
    debug_nodes_sample = []
    func_commits_sample = {}
    diag_entries = []

    # sample function_commits structure for debug
    if function_commits is not None:
        try:
            if isinstance(function_commits, dict):
                func_commits_sample = {k: (function_commits[k] if isinstance(function_commits[k], (list,dict)) else str(function_commits[k])) for i,k in enumerate(list(function_commits.keys())[:200])}
            elif isinstance(function_commits, list):
                func_commits_sample = {"type":"list","len": len(function_commits), "first_items": function_commits[:50]}
            else:
                func_commits_sample = {"type": str(type(function_commits)), "repr": str(function_commits)[:500]}
        except Exception as e:
            func_commits_sample = {"err": str(e)}

    for i, node in enumerate(nodes):
        if node.get("type") != "Function":
            continue
        if len(debug_nodes_sample) < 40:
            debug_nodes_sample.append({"id": node.get("id"), "qualified_name": node.get("qualified_name"), "attrs_keys": list((node.get("attrs") or {}).keys())})
        qname = node.get("qualified_name") or (node.get("id") and node.get("id").split(":",1)[-1])
        commit_list, strategy = get_commit_list_for_node(node, qname, function_commits)
        diag = {"id": node.get("id"), "qualified_name": qname, "strategy": strategy}
        if not commit_list:
            missing_mapping += 1
            diag["status"] = "no_commits_found"
            diag_entries.append(diag)
            details.append({"id": node["id"], "status": "no_commits_found"})
            continue
        shas = normalize_commit_list(commit_list)
        diag["raw_commit_list_sample"] = (commit_list[:5] if isinstance(commit_list, list) else str(commit_list)[:200])
        diag["normalized_shas_sample"] = shas[:10]
        if not shas:
            diag["status"] = "no_valid_shas_after_normalize"
            diag_entries.append(diag)
            details.append({"id": node["id"], "status": "no_valid_shas_after_normalize"})
            continue
        top = pick_top_commit(shas, commit_meta)
        diag["picked_top"] = top
        if not top:
            diag["status"] = "no_top_commit_found"
            diag_entries.append(diag)
            details.append({"id": node["id"], "status": "no_top_commit_found"})
            continue
        attrs = node.setdefault("attrs", {})
        attrs["introduced_by_commit"] = top
        cm = commit_meta.get(top) or commit_meta.get(str(top).lower()) or commit_meta.get(str(top)[:7]) or {}
        if cm.get("message"):
            attrs["introduced_by_commit_message"] = cm.get("message")
        if cm.get("author"):
            attrs["introduced_by_commit_author"] = cm.get("author")
        if cm.get("date"):
            attrs["introduced_by_commit_date"] = cm.get("date")

        pr_list = find_matching_pr_list_for_sha(top, commit_to_prs_norm, commit_meta) if commit_to_prs_norm else []

        if pr_list:
            prnum = pr_list[0]
            attrs["introduced_by_pr"] = prnum
            prinfo = pr_meta.get(str(prnum)) or {}
            if prinfo.get("title"):
                attrs["introduced_by_pr_title"] = prinfo.get("title")
            attached_prs += 1
        attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method": "commit_enrichment", "commit": top, "pr": attrs.get("introduced_by_pr")}]
        enriched += 1
        details.append({"id": node["id"], "commit": top, "pr": attrs.get("introduced_by_pr")})
        diag["status"] = "enriched"
        diag_entries.append(diag)

    write_json(out_nodes, nodes)

    try:
        debug_obj = {
            "nodes_sample": debug_nodes_sample,
            "function_commits_sample": func_commits_sample,
            "diag_entries_sample": diag_entries[:200],
            "commit_to_prs_keys_sample": list(commit_to_prs_norm.keys())[:200] if isinstance(commit_to_prs_norm, dict) else [],
            "commit_meta_keys_sample": list(commit_meta.keys())[:200],
            "stats": {
                "nodes_total": len(nodes),
                "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
                "enriched_functions": enriched,
                "attached_prs": attached_prs,
                "functions_missing_commits_mapping": missing_mapping,
            }
        }
        write_json(debug_path, debug_obj)
    except Exception:
        pass

    report = {
        "nodes_total": len(nodes),
        "functions_total": sum(1 for n in nodes if n.get("type") == "Function"),
        "enriched_functions": enriched,
        "attached_prs": attached_prs,
        "functions_missing_commits_mapping": missing_mapping,
        "sample": details[:200]
    }
    write_json(report_path, report)
    print(f"[INFO] Enrichment complete. Wrote: {out_nodes}")
    print(f"[INFO] Report: {report_path}")
    print(f"[INFO] Summary: {report['enriched_functions']} functions enriched, {report['attached_prs']} PRs attached.")
    if args.replace:
        backup = os.path.join(R, f"nodes.json.replace.bak.{int(datetime.datetime.now().timestamp())}")
        write_json(backup, load_json(nodes_path))
        write_json(nodes_path, nodes)
        print(f"[WARN] Replaced original nodes.json (backup at {backup})")

if __name__ == "__main__":
    main()
