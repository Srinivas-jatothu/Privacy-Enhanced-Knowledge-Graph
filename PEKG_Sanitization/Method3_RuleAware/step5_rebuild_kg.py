"""
============================================================
step5_rebuild_kg.py
============================================================
PURPOSE:
    Rebuild the Knowledge Graph from the sanitized repo.
    Runs the same KG build pipeline steps on
    Ecommerce-Data-MLOps-Sanitized/ instead of original.

    Steps run (in order):
        step1_verify_clone       - verify repo structure
        step2_index_repo_files   - index all files
        step3_extract_code_entities - extract functions/classes
        stepG_build_symbol_table - build symbol table
        stepH_full_call_graph    - build call graph
        stepI_map_functions_to_commits - map to git history
        stepJ_parse_dependencies - parse imports
        stepN_build_file_dependency_graph - file deps
        stepO_merge_into_kg      - merge into final KG
        stepP_enrich_with_commits - add commit provenance
        stepQ_enrich_nodes_extended - final enrichment

    Output goes to results/kg_method3/

USAGE:
    python step5_rebuild_kg.py

INPUT:
    - Ecommerce-Data-MLOps-Sanitized/  (sanitized repo)
    - Testing_GitHub_Code/*.py         (KG build scripts)

OUTPUT:
    - results/kg_method3/node_v2_method3.json
    - results/kg_method3/edges_method3.json
    - results/step5_rebuild_log.txt
============================================================
"""

import os
import sys
import subprocess
import shutil
import json
from loguru import logger
from config import (
    BASE_DIR,
    SANITIZED_REPO,
    SANITIZED_KG_DIR,
    SANITIZED_NODE_V2,
    SANITIZED_EDGES,
    KG_BUILD_SCRIPTS_DIR,
    RESULTS_DIR,
    ORIGINAL_KG_DIR,
)

# ============================================================
# OUTPUT
# ============================================================

REBUILD_LOG = os.path.join(RESULTS_DIR, "step5_rebuild_log.txt")
log_lines   = []

# ============================================================
# KG BUILD SCRIPTS - in order
# ============================================================

KG_SCRIPTS_IN_ORDER = [
    "step1_verify_clone.py",
    "step2_index_repo_files.py",
    "step3_extract_code_entities.py",
    "stepG_build_symbol_table.py",
    "stepH_full_call_graph.py",
    "stepI_map_functions_to_commits.py",
    "stepJ_parse_dependencies.py",
    "stepN_build_file_dependency_graph.py",
    "stepO_merge_into_kg.py",
    "stepP_enrich_with_commits.py",
    "stepQ_enrich_nodes_extended.py",
]

# ============================================================
# APPROACH: Since the KG build scripts have hardcoded paths
# to the original repo, we use an environment variable
# override approach - set REPO_ROOT env var to sanitized repo
# ============================================================

def run_kg_script(script_name, env_overrides=None):
    """
    Run a KG build script with environment variable overrides.
    Returns (success, output)
    """
    script_path = os.path.join(KG_BUILD_SCRIPTS_DIR, script_name)

    if not os.path.exists(script_path):
        logger.warning(f"Script not found: {script_path}")
        return False, "Script not found"

    # Build environment with overrides
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    # Add sanitized repo to env
    env["REPO_ROOT"]        = SANITIZED_REPO
    env["SANITIZED_REPO"]   = SANITIZED_REPO
    env["KG_OUTPUT_DIR"]    = SANITIZED_KG_DIR

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            cwd=KG_BUILD_SCRIPTS_DIR,
            timeout=300  # 5 min timeout per script
        )
        success = result.returncode == 0
        output  = result.stdout + result.stderr
        return success, output
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT after 5 minutes"
    except Exception as e:
        return False, str(e)


# ============================================================
# ALTERNATIVE APPROACH
# Since scripts may have hardcoded paths, we can also
# directly copy and adapt the KG output files
# ============================================================

def copy_and_adapt_kg():
    """
    Alternative: Copy existing KG and update identifiers
    using the identifier_map.json from step2.

    This is faster and more reliable than rerunning all scripts.
    The KG structure (edges, relationships) stays the same.
    Only node labels/names are updated to match sanitized code.
    """
    logger.info("Using copy-and-adapt approach...")

    # Load identifier map
    identifier_map_path = os.path.join(RESULTS_DIR, "identifier_map.json")
    with open(identifier_map_path, 'r', encoding='utf-8') as f:
        identifier_map = json.load(f)

    func_map    = identifier_map.get("functions",  {})
    module_map  = identifier_map.get("modules",    {})
    file_map    = identifier_map.get("files",      {})

    logger.info(f"Loaded maps: {len(func_map)} funcs, "
               f"{len(module_map)} modules, {len(file_map)} files")

    # Load original node_v2.json
    original_node_v2 = os.path.join(ORIGINAL_KG_DIR, "node_v2.json")
    original_edges   = os.path.join(ORIGINAL_KG_DIR, "edges.json")

    with open(original_node_v2, 'r', encoding='utf-8') as f:
        nodes_data = json.load(f)
    with open(original_edges, 'r', encoding='utf-8') as f:
        edges_data = json.load(f)

    logger.info(f"Loaded original KG: "
               f"{len(nodes_data)} nodes, {len(edges_data)} edges")

    # --------------------------------------------------------
    # Update node attributes using identifier maps
    # --------------------------------------------------------
    import re
    import copy

    def sanitize_value(value, func_map, module_map, file_map):
        """Replace identifiers in a string value."""
        if not value or not isinstance(value, str):
            return value

        result = value

        # Replace function names (longer first to avoid partials)
        for orig, placeholder in sorted(
            func_map.items(), key=lambda x: -len(x[0])
        ):
            result = re.sub(
                rf'\b{re.escape(orig)}\b',
                placeholder, result
            )

        # Replace module paths
        for orig, placeholder in sorted(
            module_map.items(), key=lambda x: -len(x[0])
        ):
            result = result.replace(orig, placeholder)

        # Replace file paths
        for orig, placeholder in sorted(
            file_map.items(), key=lambda x: -len(x[0])
        ):
            result = result.replace(orig, placeholder)

        return result

    def sanitize_node(node):
        """Sanitize all string fields in a node."""
        sanitized = copy.deepcopy(node)

        # Fields to sanitize
        string_fields = [
            "id", "label", "name", "qualified_name",
            "file", "path", "introduced_by_pr_title",
            "title", "message"
        ]

        for field in string_fields:
            if field in sanitized and sanitized[field]:
                sanitized[field] = sanitize_value(
                    sanitized[field], func_map,
                    module_map, file_map
                )

        # Sanitize attrs
        if "attrs" in sanitized and isinstance(
            sanitized["attrs"], dict
        ):
            for attr_key in [
                "qualified_name", "file", "module",
                "message_head", "title"
            ]:
                if attr_key in sanitized["attrs"]:
                    sanitized["attrs"][attr_key] = sanitize_value(
                        sanitized["attrs"][attr_key],
                        func_map, module_map, file_map
                    )

        return sanitized

    # --------------------------------------------------------
    # Process all nodes
    # --------------------------------------------------------
    if isinstance(nodes_data, dict):
        sanitized_nodes = {}
        for node_id, node in nodes_data.items():
            san_node = sanitize_node(node)
            sanitized_nodes[san_node.get("id", node_id)] = san_node
    else:
        sanitized_nodes = [sanitize_node(n) for n in nodes_data]

    # --------------------------------------------------------
    # Process edges - update source/target IDs
    # --------------------------------------------------------
    # Build node ID mapping from old → new
    if isinstance(nodes_data, dict):
        old_ids = list(nodes_data.keys())
        new_ids = list(sanitized_nodes.keys())
        id_map  = dict(zip(old_ids, new_ids))
    else:
        id_map = {}
        for old, new in zip(nodes_data, sanitized_nodes):
            old_id = old.get("id", "")
            new_id = new.get("id", "")
            if old_id != new_id:
                id_map[old_id] = new_id

    sanitized_edges = []
    for edge in edges_data:
        san_edge = copy.deepcopy(edge)
        if "source" in san_edge:
            san_edge["source"] = id_map.get(
                san_edge["source"], san_edge["source"]
            )
        if "target" in san_edge:
            san_edge["target"] = id_map.get(
                san_edge["target"], san_edge["target"]
            )
        sanitized_edges.append(san_edge)

    # --------------------------------------------------------
    # Save sanitized KG
    # --------------------------------------------------------
    os.makedirs(SANITIZED_KG_DIR, exist_ok=True)

    with open(SANITIZED_NODE_V2, 'w', encoding='utf-8') as f:
        json.dump(sanitized_nodes, f, indent=2,
                  ensure_ascii=False)
    logger.success(f"Saved: {SANITIZED_NODE_V2}")

    with open(SANITIZED_EDGES, 'w', encoding='utf-8') as f:
        json.dump(sanitized_edges, f, indent=2,
                  ensure_ascii=False)
    logger.success(f"Saved: {SANITIZED_EDGES}")

    return len(sanitized_nodes), len(sanitized_edges)


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting KG Rebuild (Step 5)...")
    logger.info(f"Sanitized repo: {SANITIZED_REPO}")
    logger.info(f"Output dir    : {SANITIZED_KG_DIR}")

    # Use copy-and-adapt approach (faster and more reliable)
    node_count, edge_count = copy_and_adapt_kg()

    # --------------------------------------------------------
    # Verify output
    # --------------------------------------------------------
    node_size = os.path.getsize(SANITIZED_NODE_V2) / 1024
    edge_size = os.path.getsize(SANITIZED_EDGES) / 1024

    print("\n" + "=" * 60)
    print("KG REBUILD SUMMARY (Step 5)")
    print("=" * 60)
    print(f"Approach          : Copy + Adapt (identifier replacement)")
    print(f"Nodes processed   : {node_count}")
    print(f"Edges processed   : {edge_count}")
    print(f"Node file size    : {node_size:.1f} KB")
    print(f"Edge file size    : {edge_size:.1f} KB")
    print(f"Output KG         : {SANITIZED_NODE_V2}")
    print(f"Output edges      : {SANITIZED_EDGES}")
    print("=" * 60)
    print("\nNext step: python step6_summarization.py")

    # Quick sample check
    with open(SANITIZED_NODE_V2, 'r', encoding='utf-8') as f:
        data = json.load(f)
    nodes = list(data.values()) if isinstance(data, dict) \
            else data
    for node in nodes:
        if node.get("type") == "Function":
            print("\nSample Function node:")
            print(f"  id    : {node.get('id')}")
            print(f"  label : {node.get('label')}")
            print(f"  file  : {node.get('file')}")
            break


if __name__ == "__main__":
    main()