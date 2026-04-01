"""
============================================================
step3_identifier_sanitizer.py
============================================================
PURPOSE:
    Sanitize IDENTIFIER and STRUCTURAL leakage fields in the KG.
    This step handles what Step 2 deliberately skipped:

    IDENTIFIER fields:
        - id            : "Function:dags.src.anomaly_code_handler.handle_anomalous_codes"
        - label         : "handle_anomalous_codes"
        - name          : "handle_anomalous_codes"
        - qualified_name: "dags.src.anomaly_code_handler.handle_anomalous_codes"
        - signature     : "(input_pickle_path, output_pickle_path)"
        - docstring     : free text containing internal names
        - text_preview  : document content preview

    STRUCTURAL fields:
        - file          : "dags/src/anomaly_code_handler.py"
        - path          : "dags/src/anomaly_code_handler.py"
        - attrs.file    : "dags/src/anomaly_code_handler.py"
        - attrs.module  : "dags.src.anomaly_code_handler"
        - attrs.qualified_name: same as qualified_name

    REPLACEMENT STRATEGY (Method 2 - regex based, NOT role-aware):
        - Each unique identifier gets a counter-based placeholder
        - func_001, func_002 ... for function names
        - file_001, file_002 ... for file paths
        - mod_001, mod_002  ... for module paths
        - pkg_001, pkg_002  ... for package names
        - Node IDs are updated consistently across all edges

    NOTE: This is Method 2's key weakness - replacements are
    counter-based, NOT role-aware. 'handle_anomalous_codes'
    and 'save_heatmap' both become 'func_001', 'func_002'
    with no indication of their role. This destroys semantic
    meaning for the LLM summarizer.
    Method 3 will fix this with role-aware replacement.

USAGE:
    python step3_identifier_sanitizer.py

INPUT:
    - results/kg_after_literal_sanitization.json  (from step2)
    - edges.json
      Path: ../../Testing_GitHub_Code/results/edges.json

OUTPUT:
    - results/kg_after_identifier_sanitization.json
        KG with all identifier + structural fields sanitized
    - results/identifier_registry.csv
        Mapping: original identifier → placeholder
    - results/identifier_sanitization_summary.txt
        Summary of what was sanitized
============================================================
"""

import json
import os
import csv
import re
import copy
from collections import defaultdict
from loguru import logger

# ============================================================
# CONFIG
# ============================================================

INPUT_KG    = os.path.join(os.path.dirname(__file__), "results", "kg_after_literal_sanitization.json")
EDGES_FILE  = r"C:\Users\jsrin\OneDrive\Desktop\Github\PEKG\Testing_GitHub_Code\results\edges.json"
OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_KG       = os.path.join(OUTPUT_DIR, "kg_after_identifier_sanitization.json")
REGISTRY_FILE   = os.path.join(OUTPUT_DIR, "identifier_registry.csv")
SUMMARY_FILE    = os.path.join(OUTPUT_DIR, "identifier_sanitization_summary.txt")

# ============================================================
# IDENTIFIER REGISTRIES
# Each unique value gets a consistent placeholder
# Same value always maps to same placeholder
# ============================================================

# Separate counters per type for clean readable placeholders
_counters = {
    "func":     1,
    "file":     1,
    "module":   1,
    "package":  1,
    "node":     1,
    "doc":      1,
    "param":    1,
}

# Maps: original_value → placeholder
func_registry   = {}   # function/class names
file_registry   = {}   # file paths
module_registry = {}   # module paths
node_id_registry = {}  # node IDs (used to update edges too)

# All replacements log
identifier_rows = []


def record_identifier(node_id, node_type, field, original, replacement):
    """Record an identifier replacement."""
    identifier_rows.append({
        "node_id":      node_id,
        "node_type":    node_type,
        "field":        field,
        "original":     str(original)[:120],
        "replacement":  str(replacement),
        "category":     "IDENTIFIER"
    })


# ============================================================
# PLACEHOLDER GENERATORS
# ============================================================

def get_func_placeholder(name):
    """Get consistent placeholder for a function/class/label name."""
    key = str(name).strip()
    if key not in func_registry:
        func_registry[key] = f"func_{_counters['func']:03d}"
        _counters['func'] += 1
    return func_registry[key]


def get_file_placeholder(path):
    """
    Get consistent placeholder for a file path.
    Preserves file type (test/dag/deploy/src) as context hint.
    e.g. dags/src/anomaly_code_handler.py → dag_src_file_001.py
    """
    key = str(path).strip()
    if key not in file_registry:
        # Detect file type from path for context preservation
        path_lower = key.lower()
        ext = os.path.splitext(key)[1] if '.' in key else ''

        if 'test' in path_lower:
            prefix = "test_file"
        elif 'dags' in path_lower:
            prefix = "dag_file"
        elif 'gcpdeploy' in path_lower:
            prefix = "deploy_file"
        elif 'src' in path_lower:
            prefix = "src_file"
        elif 'config' in path_lower:
            prefix = "config_file"
        elif 'data' in path_lower:
            prefix = "data_file"
        else:
            prefix = "file"

        placeholder = f"{prefix}_{_counters['file']:03d}{ext}"
        _counters['file'] += 1
        file_registry[key] = placeholder

    return file_registry[key]


def get_module_placeholder(module_path):
    """
    Get consistent placeholder for a module path.
    Preserves depth/structure hint.
    e.g. dags.src.anomaly_code_handler → module_l3_001
         (l3 = 3 levels deep)
    """
    key = str(module_path).strip()
    if key not in module_registry:
        depth = len(key.split('.'))
        placeholder = f"module_l{depth}_{_counters['module']:03d}"
        _counters['module'] += 1
        module_registry[key] = placeholder
    return module_registry[key]


def get_node_id_placeholder(node_id, node_type):
    """
    Get consistent placeholder for a node ID.
    Preserves node type prefix.
    e.g. "Function:dags.src.anomaly.handle" → "Function:func_001"
    """
    key = str(node_id).strip()
    if key not in node_id_registry:
        # Extract the type prefix from the ID
        if ':' in key:
            type_prefix = key.split(':')[0]
            inner = key.split(':', 1)[1]

            # Generate inner placeholder based on type
            if type_prefix == 'Function':
                inner_placeholder = get_func_placeholder(inner)
            elif type_prefix == 'File':
                inner_placeholder = get_file_placeholder(inner)
            elif type_prefix == 'Module':
                inner_placeholder = get_module_placeholder(inner)
            elif type_prefix == 'Package':
                pkg_key = inner.strip()
                if pkg_key not in module_registry:
                    module_registry[pkg_key] = f"pkg_{_counters['package']:03d}"
                    _counters['package'] += 1
                inner_placeholder = module_registry[pkg_key]
            elif type_prefix in ('Commit', 'PullRequest', 'Document'):
                # These IDs already have hashes/numbers replaced in step2
                # Just use the ID as-is (it's already sanitized)
                node_id_registry[key] = key
                return key
            else:
                inner_placeholder = f"node_{_counters['node']:03d}"
                _counters['node'] += 1

            node_id_registry[key] = f"{type_prefix}:{inner_placeholder}"
        else:
            node_id_registry[key] = f"node_{_counters['node']:03d}"
            _counters['node'] += 1

    return node_id_registry[key]


# ============================================================
# SIGNATURE SANITIZER
# Replaces parameter names in function signatures
# e.g. (input_pickle_path, output_pickle_path) → (param_001, param_002)
# ============================================================

param_registry = {}


def sanitize_signature(signature):
    """
    Sanitize a function signature string.
    Replaces parameter names with param_N placeholders.
    """
    if not signature or not isinstance(signature, str):
        return signature

    # Extract content between parentheses
    match = re.match(r'\((.*)\)', signature.strip())
    if not match:
        return signature

    params_str = match.group(1)
    if not params_str.strip():
        return "()"

    # Split by comma, handle default values
    params = [p.strip() for p in params_str.split(',')]
    sanitized_params = []

    for param in params:
        # Handle default values: param_name=default_value
        if '=' in param:
            param_name = param.split('=')[0].strip()
        else:
            param_name = param.strip()

        # Skip *args, **kwargs style
        if param_name.startswith('*') or param_name.startswith('**'):
            prefix = param_name[:2] if param_name.startswith('**') else param_name[:1]
            param_name_clean = param_name.lstrip('*')
        else:
            prefix = ''
            param_name_clean = param_name

        # Skip 'self' and 'cls'
        if param_name_clean in ('self', 'cls'):
            sanitized_params.append(param_name)
            continue

        # Get or create placeholder
        if param_name_clean not in param_registry:
            param_registry[param_name_clean] = f"param_{_counters['param']:03d}"
            _counters['param'] += 1

        placeholder = prefix + param_registry[param_name_clean]

        # Re-add default value placeholder
        if '=' in param:
            placeholder += "=[DEFAULT]"

        sanitized_params.append(placeholder)

    return f"({', '.join(sanitized_params)})"


# ============================================================
# DOCSTRING SANITIZER
# Replaces internal identifiers in docstring text
# ============================================================

def sanitize_docstring(text):
    """
    Sanitize a docstring by replacing internal identifiers.
    Replaces:
        - File paths
        - Module paths (dotted)
        - snake_case function names
        - CamelCase class names
    """
    if not text or not isinstance(text, str):
        return text

    sanitized = text

    # Replace file paths (word/word/word.py pattern)
    def replace_file_path(m):
        original = m.group(0)
        return get_file_placeholder(original)

    sanitized = re.sub(
        r'\b[\w][\w/.-]*/[\w/.-]+\.(py|json|yaml|yml|txt|csv)\b',
        replace_file_path,
        sanitized
    )

    # Replace dotted module paths (3+ levels)
    def replace_module(m):
        original = m.group(0)
        return get_module_placeholder(original)

    sanitized = re.sub(
        r'\b[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2,}\b',
        replace_module,
        sanitized
    )

    return sanitized


# ============================================================
# SANITIZE A SINGLE NODE
# ============================================================

def sanitize_node(node):
    """
    Sanitize all identifier and structural fields in a node.
    Returns sanitized copy of the node.
    """
    sanitized = copy.deepcopy(node)
    node_id   = node.get("id",   "UNKNOWN")
    node_type = node.get("type", "UNKNOWN")

    # --------------------------------------------------------
    # 1. Sanitize node ID (most important - used in edges too)
    # --------------------------------------------------------
    original_id     = node_id
    new_id          = get_node_id_placeholder(original_id, node_type)
    sanitized["id"] = new_id
    if new_id != original_id:
        record_identifier(original_id, node_type, "id", original_id, new_id)

    # --------------------------------------------------------
    # 2. Sanitize label
    # --------------------------------------------------------
    # if "label" in sanitized and sanitized["label"]:
    #     original = sanitized["label"]
    #     replacement = get_func_placeholder(original)
    #     sanitized["label"] = replacement
    #     record_identifier(original_id, node_type, "label", original, replacement)
    # Sanitize label - route by node type
    if "label" in sanitized and sanitized["label"]:
        original = sanitized["label"]
        if node_type == "File":
            replacement = get_file_placeholder(original)
        elif node_type == "Module":
            replacement = get_module_placeholder(original)
        elif node_type == "Package":
            replacement = get_module_placeholder(original)
        else:
            replacement = get_func_placeholder(original)
        sanitized["label"] = replacement
        record_identifier(original_id, node_type, "label", original, replacement)

    # --------------------------------------------------------
    # 3. Sanitize name
    # --------------------------------------------------------
    # Sanitize name - route by node type
    if "name" in sanitized and sanitized["name"]:
        original = sanitized["name"]
        if node_type in ("Module", "Package"):
            replacement = get_module_placeholder(original)
        elif node_type == "File":
            replacement = get_file_placeholder(original)
        else:
            replacement = get_func_placeholder(original)
        sanitized["name"] = replacement
        record_identifier(original_id, node_type, "name", original, replacement)

    # --------------------------------------------------------
    # 4. Sanitize qualified_name
    # --------------------------------------------------------
    if "qualified_name" in sanitized and sanitized["qualified_name"]:
        original = sanitized["qualified_name"]
        replacement = get_module_placeholder(original)
        sanitized["qualified_name"] = replacement
        record_identifier(original_id, node_type, "qualified_name", original, replacement)

    # --------------------------------------------------------
    # 5. Sanitize file path
    # --------------------------------------------------------
    if "file" in sanitized and sanitized["file"]:
        original = sanitized["file"]
        replacement = get_file_placeholder(original)
        sanitized["file"] = replacement
        record_identifier(original_id, node_type, "file", original, replacement)

    # --------------------------------------------------------
    # 6. Sanitize path
    # --------------------------------------------------------
    if "path" in sanitized and sanitized["path"]:
        original = sanitized["path"]
        replacement = get_file_placeholder(original)
        sanitized["path"] = replacement
        record_identifier(original_id, node_type, "path", original, replacement)

    # --------------------------------------------------------
    # 7. Sanitize signature
    # --------------------------------------------------------
    if "signature" in sanitized and sanitized["signature"]:
        original = sanitized["signature"]
        replacement = sanitize_signature(original)
        sanitized["signature"] = replacement
        if replacement != original:
            record_identifier(original_id, node_type, "signature", original, replacement)

    # --------------------------------------------------------
    # 8. Sanitize docstring
    # --------------------------------------------------------
    if "docstring" in sanitized and sanitized["docstring"]:
        original = sanitized["docstring"]
        replacement = sanitize_docstring(original)
        sanitized["docstring"] = replacement
        if replacement != original:
            record_identifier(original_id, node_type, "docstring",
                              original[:80], replacement[:80])

    # --------------------------------------------------------
    # 9. Sanitize text_preview (Document nodes)
    # --------------------------------------------------------
    if "text_preview" in sanitized and sanitized["text_preview"]:
        original = sanitized["text_preview"]
        replacement = sanitize_docstring(original)
        sanitized["text_preview"] = replacement
        if replacement != original:
            record_identifier(original_id, node_type, "text_preview",
                              original[:80], replacement[:80])

    # --------------------------------------------------------
    # 10. Sanitize attrs fields
    # --------------------------------------------------------
    if "attrs" in sanitized and isinstance(sanitized["attrs"], dict):
        attrs = sanitized["attrs"]

        if "qualified_name" in attrs and attrs["qualified_name"]:
            original = attrs["qualified_name"]
            attrs["qualified_name"] = get_module_placeholder(original)
            record_identifier(original_id, node_type, "attrs.qualified_name",
                              original, attrs["qualified_name"])

        if "file" in attrs and attrs["file"]:
            original = attrs["file"]
            attrs["file"] = get_file_placeholder(original)
            record_identifier(original_id, node_type, "attrs.file",
                              original, attrs["file"])

        if "module" in attrs and attrs["module"]:
            original = attrs["module"]
            attrs["module"] = get_module_placeholder(original)
            record_identifier(original_id, node_type, "attrs.module",
                              original, attrs["module"])

    return sanitized


# ============================================================
# SANITIZE EDGES
# Update source/target node IDs to match new sanitized IDs
# ============================================================

def sanitize_edges(edges):
    """
    Update all edge source/target IDs using the node_id_registry.
    Edge types and attributes remain unchanged.
    """
    sanitized_edges = []
    edge_updates = 0

    for edge in edges:
        sanitized_edge = copy.deepcopy(edge)

        # Update source
        if "source" in sanitized_edge:
            original_source = sanitized_edge["source"]
            new_source = node_id_registry.get(original_source, original_source)
            if new_source != original_source:
                sanitized_edge["source"] = new_source
                edge_updates += 1

        # Update target
        if "target" in sanitized_edge:
            original_target = sanitized_edge["target"]
            new_target = node_id_registry.get(original_target, original_target)
            if new_target != original_target:
                sanitized_edge["target"] = new_target
                edge_updates += 1

        sanitized_edges.append(sanitized_edge)

    return sanitized_edges, edge_updates


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("Starting Identifier & Structural Sanitization (Step 3)...")

    # Load KG from step2 output
    logger.info(f"Loading KG from: {INPUT_KG}")
    with open(INPUT_KG, 'r', encoding='utf-8') as f:
        data = json.load(f)

    is_dict = isinstance(data, dict)
    nodes   = list(data.values()) if is_dict else data
    logger.info(f"Total nodes to sanitize: {len(nodes)}")

    # Load edges
    logger.info(f"Loading edges from: {EDGES_FILE}")
    with open(EDGES_FILE, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    logger.info(f"Total edges loaded: {len(edges)}")

    # --------------------------------------------------------
    # Pass 1: Sanitize all nodes
    # (builds node_id_registry needed for edge sanitization)
    # --------------------------------------------------------
    sanitized_nodes = []
    for i, node in enumerate(nodes):
        sanitized = sanitize_node(node)
        sanitized_nodes.append(sanitized)
        if (i + 1) % 200 == 0:
            logger.info(f"  Processed {i+1}/{len(nodes)} nodes...")

    logger.success(f"Node sanitization complete. {len(identifier_rows)} replacements made.")

    # --------------------------------------------------------
    # Pass 2: Sanitize edges using node_id_registry
    # --------------------------------------------------------
    logger.info("Sanitizing edges...")
    sanitized_edges, edge_updates = sanitize_edges(edges)
    logger.success(f"Edge sanitization complete. {edge_updates} edge endpoints updated.")

    # --------------------------------------------------------
    # Save sanitized KG
    # --------------------------------------------------------
    if is_dict:
        output_data = {node["id"]: node for node in sanitized_nodes}
    else:
        output_data = sanitized_nodes

    with open(OUTPUT_KG, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    logger.success(f"Sanitized KG saved: {OUTPUT_KG}")

    # Save sanitized edges alongside KG
    edges_output = os.path.join(OUTPUT_DIR, "edges_sanitized.json")
    with open(edges_output, 'w', encoding='utf-8') as f:
        json.dump(sanitized_edges, f, indent=2, ensure_ascii=False)
    logger.success(f"Sanitized edges saved: {edges_output}")

    # --------------------------------------------------------
    # Save identifier registry
    # --------------------------------------------------------
    registry_fieldnames = ["node_id", "node_type", "field",
                           "original", "replacement", "category"]
    with open(REGISTRY_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=registry_fieldnames)
        writer.writeheader()
        writer.writerows(identifier_rows)
    logger.success(f"Identifier registry saved: {REGISTRY_FILE} ({len(identifier_rows)} entries)")

    # --------------------------------------------------------
    # Summary stats
    # --------------------------------------------------------
    field_counts    = defaultdict(int)
    type_counts     = defaultdict(int)

    for row in identifier_rows:
        field_counts[row["field"]]      += 1
        type_counts[row["node_type"]]   += 1

    summary_lines = [
        "=" * 60,
        "IDENTIFIER & STRUCTURAL SANITIZATION SUMMARY (Step 3)",
        "=" * 60,
        f"Total nodes processed      : {len(nodes)}",
        f"Total identifier replacements: {len(identifier_rows)}",
        f"Edge endpoints updated     : {edge_updates}",
        f"Unique function names masked: {len(func_registry)}",
        f"Unique file paths masked   : {len(file_registry)}",
        f"Unique module paths masked : {len(module_registry)}",
        f"Unique parameters masked   : {len(param_registry)}",
        "",
        "Replacements by Field:",
    ]
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {field:<40} {count:>6}")

    summary_lines += ["", "Replacements by Node Type:"]
    for ntype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        summary_lines.append(f"  {ntype:<30} {count:>6}")

    summary_lines += [
        "",
        "Sample Function Name Mappings (first 10):",
    ]
    for orig, placeholder in list(func_registry.items())[:10]:
        summary_lines.append(f"  {placeholder:<20} ← {orig}")

    summary_lines += [
        "",
        "Sample File Path Mappings (first 10):",
    ]
    for orig, placeholder in list(file_registry.items())[:10]:
        summary_lines.append(f"  {placeholder:<30} ← {orig}")

    summary_lines += [
        "",
        "NOTE (Method 2 Weakness):",
        "  Replacements are counter-based, NOT role-aware.",
        "  'handle_anomalous_codes' and 'save_heatmap' both get",
        "  generic func_N labels with no semantic role preserved.",
        "  This reduces LLM summarization quality.",
        "  Method 3 will address this with role-aware abstraction.",
        "=" * 60,
    ]

    summary_text = "\n".join(summary_lines)
    print("\n" + summary_text)

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary_text)
    logger.success(f"Summary saved: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()