<div align="center">

<img src="https://img.shields.io/badge/IIT%20Tirupati-MTP%20Thesis-1F3864?style=for-the-badge&logo=academia&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Knowledge%20Graph-1807%20Nodes-375623?style=for-the-badge&logo=graphql&logoColor=white"/>
<img src="https://img.shields.io/badge/Privacy-k%3D3%20Anonymity-4B2D83?style=for-the-badge&logo=shield&logoColor=white"/>
<img src="https://img.shields.io/badge/LLM-Code%20Summarization-C55A11?style=for-the-badge&logo=openai&logoColor=white"/>

# Privacy Enhanced Knowledge Graph (PEKG)
### *for Safe, Context-Aware Code Summarization*

**MTP Thesis — Indian Institute of Technology Tirupati**

*Jatothu Srinivas Nayak (CS24M125)*
*Supervisors: Dr. Sridhar Chimalakonda & Dr. G. Ramakrishna*

---

[📄 Thesis](#-thesis) • [🏗️ Architecture](#%EF%B8%8F-system-architecture) • [🚀 Quick Start](#-quick-start) • [📊 Results](#-results--evaluation) • [🔐 PEKG Methods](#-pekg-sanitization-methods) • [📁 Structure](#-repository-structure) • [📚 Citation](#-citation)

</div>

---

## 🧭 Overview

Modern software teams increasingly rely on **Large Language Models (LLMs)** for code understanding, documentation, and summarization. However, passing raw source code or raw Knowledge Graphs to external LLMs introduces severe **privacy risks** — sensitive function names, developer identities, internal API paths, and proprietary architectural patterns are all exposed verbatim in model outputs.

This thesis introduces **PEKG (Privacy-Enhanced Knowledge Graph)** — a principled framework that:

1. Constructs a rich, multi-layer **Knowledge Graph** from a software repository (AST, call graph, imports, git provenance)
2. Demonstrates that raw KG summarization leaks sensitive information in **100% of cases**
3. Applies **three progressively stronger sanitization methods** to eliminate all leakage categories
4. Shows that **privacy and utility are not fundamentally at odds** — Method 3 (role-aware sanitization) *outperforms* the unsanitized raw KG baseline on all six summarization metrics

> **Target codebase:** [`Thomas-George-T/Ecommerce-Data-MLOps`](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps) — a production-grade Python MLOps pipeline on Google Cloud Platform.

---

## 📄 Thesis

| Item | Detail |
|------|--------|
| **Title** | Privacy Enhanced Knowledge Graph (PEKG) for Code Summarization |
| **Author** | Jatothu Srinivas Nayak — CS24M125 |
| **Institution** | IIT Tirupati, Department of CS&E |
| **Supervisors** | Dr. Sridhar Chimalakonda, Dr. G. Ramakrishna |
| **Programme** | Master of Technology (MTP) |
| **Status** | Phase 1 ✅ Complete · Phase 2 ✅ Complete |

---

## 🏗️ System Architecture

The complete pipeline flows through **four phases**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    PEKG Pipeline Overview                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Phase 1              Phase 2              Phase 3              │
│  ─────────────        ────────────────     ────────────────     │
│  GitHub API      →    AST Parsing      →   Target Resolution    │
│  Commit History       Call Graph           Context Extraction   │
│  PR Metadata          Import Graph         LLM Prompt Build     │
│  Contributors         Provenance           Summary Generation   │
│                       Integration                               │
│                           │                                     │
│                           ▼                                     │
│                    Raw KG (1,807 nodes, 2,506 edges)            │
│                           │                                     │
│              ┌────────────┼────────────┐                        │
│              ▼            ▼            ▼                        │
│          Method 2     Method 3     Method 4       Phase 4       │
│          Blind        Role-Aware   (k,x)-Iso                    │
│          Replace      Replace      morphism                     │
│              │            │            │                        │
│              └────────────┴────────────┘                        │
│                           │                                     │
│                    Summarization + Evaluation                   │
│                           │                                     │
│                   Privacy-Safe Summaries ✅                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

```bash
python >= 3.10
git clone https://github.com/Srinivas-jatothu/Privacy-Enhanced-Knowledge-Graph.git
cd Privacy-Enhanced-Knowledge-Graph
pip install -r requirements.txt
```

### Environment Setup

```bash
# Create .env file
cp .env.example .env

# Fill in your credentials:
# GITHUB_TOKEN=your_github_personal_access_token
# ANTHROPIC_API_KEY=your_anthropic_api_key      # for Methods 2 & 3 summarization
# OPENROUTER_API_KEY=your_openrouter_api_key    # for Method 4 summarization
```

### Run Phase 1 — Repository Metadata Extraction

```bash
cd Data_Fetching/
python fetch_commits.py
python fetch_pull_requests.py
python fetch_contributors.py
# Outputs: data/commits.json, data/pull_requests.json, data/contributors.json
```

### Run Phase 2 — Knowledge Graph Construction

```bash
cd Testing_GitHub_Code/
python step_A_index_files.py
python step_B_parse_ast.py
python step_C_build_symbol_table.py
python step_D_build_call_graph.py
python step_E_build_import_graph.py
python step_F_map_commits_to_functions.py
python step_G_build_dependency_graph.py
python step_H_integrate_kg.py
python step_I_enrich_provenance.py
python step_J_enrich_call_graph.py
python step_K_finalize_kg.py
# Outputs: node_v2.json (1,807 nodes), edges.json (2,506 edges)
```

### Run Phase 3 — KG-Aware Code Summarization

```bash
cd Code_Summarization/
python summarize.py --target dags/src/correlation.py
# Outputs: summaries_raw.jsonl
```

### Run Phase 4 — PEKG Sanitization

```bash
# Method 2: Blind Identifier Replacement
cd PEKG_Sanitization/Method2_Blind/
python step1_copy_repo.py
python step2_build_identifier_map.py
python step3_sanitize_source_code.py
python step4_validate_sanitized_repo.py
python step5_rebuild_kg.py
# Outputs: node_v2_method2.json, edges_method2.json

# Method 3: Role-Aware Identifier Replacement
cd ../Method3_RuleAware/
python step1_copy_repo.py
python step2_build_identifier_map.py   # <-- role detection happens here
python step3_sanitize_source_code.py
python step4_validate_sanitized_repo.py
python step5_rebuild_kg.py
# Outputs: node_v2_method3.json, edges_method3.json, identifier_map.json

# Method 4: (k,x)-Isomorphism
cd ../Method4_KX_Isomorphism/
python step1_extract_subgraphs.py
python step2_anonymize_kx.py
python step3_evaluate_utility.py
python step4_summarize_method4.py
# Outputs: node_v2_method4.json, edges_method4.json, summaries_method4.jsonl

# Evaluate Methods 2 & 3
cd ../../
python step6_summarization.py    # generate summaries for Methods 2 & 3
python step7_evaluate.py         # compute BLEU, ROUGE, METEOR, chrF
```

---

## 🗂️ Repository Structure

```
Privacy-Enhanced-Knowledge-Graph/
│
├── 📂 Data_Fetching/                   # Phase 1: Repository metadata
│   ├── fetch_commits.py
│   ├── fetch_pull_requests.py
│   ├── fetch_contributors.py
│   └── data/
│       ├── commits.json
│       ├── pull_requests.json
│       └── contributors.json
│
├── 📂 Testing_GitHub_Code/             # Phase 2: KG construction (11 steps)
│   ├── step_A_index_files.py
│   ├── step_B_parse_ast.py
│   ├── step_C_build_symbol_table.py
│   ├── step_D_build_call_graph.py
│   ├── step_E_build_import_graph.py
│   ├── step_F_map_commits_to_functions.py
│   ├── step_G_build_dependency_graph.py
│   ├── step_H_integrate_kg.py
│   ├── step_I_enrich_provenance.py
│   ├── step_J_enrich_call_graph.py
│   ├── step_K_finalize_kg.py
│   ├── node_v2.json                    # 1,807 nodes
│   └── edges.json                      # 2,506 edges
│
├── 📂 Code_Summarization/              # Phase 3: KG-aware summarization
│   ├── summarize.py
│   └── summaries_raw.jsonl
│
├── 📂 PEKG_Sanitization/               # Phase 4: Privacy pipeline
│   │
│   ├── 📂 Method2_Blind/               # Blind identifier replacement
│   │   ├── config.py
│   │   ├── step1_copy_repo.py
│   │   ├── step2_build_identifier_map.py
│   │   ├── step3_sanitize_source_code.py
│   │   ├── step4_validate_sanitized_repo.py
│   │   ├── step5_rebuild_kg.py
│   │   └── results/
│   │       ├── identifier_map.json
│   │       ├── identifier_map.csv
│   │       ├── validation_report.txt
│   │       ├── node_v2_method2.json    # 1,807 nodes, 0% overhead
│   │       └── edges_method2.json      # 2,506 edges
│   │
│   ├── 📂 Method3_RuleAware/           # Role-aware identifier replacement
│   │   ├── config_method3.py
│   │   ├── step1_copy_repo.py
│   │   ├── step2_build_identifier_map.py   # role detection
│   │   ├── step3_sanitize_source_code.py
│   │   ├── step4_validate_sanitized_repo.py
│   │   ├── step5_rebuild_kg.py
│   │   └── results/
│   │       ├── identifier_map.json         # 426 mappings
│   │       ├── identifier_map.csv
│   │       ├── identifier_map_summary.txt
│   │       ├── sanitization_report.csv
│   │       ├── validation_report.txt
│   │       ├── node_v2_method3.json    # 1,807 nodes, 0% overhead
│   │       └── edges_method3.json      # 2,506 edges
│   │
│   └── 📂 Method4_KX_Isomorphism/     # (k,x)-Isomorphism
│       ├── config_method4.py
│       ├── step1_extract_subgraphs.py
│       ├── step2_anonymize_kx.py
│       ├── step3_evaluate_utility.py
│       ├── step4_summarize_method4.py
│       └── results/
│           ├── subgraphs_method4.json
│           ├── node_v2_method4.json    # 1,931 nodes, 6.9% overhead
│           ├── edges_method4.json      # 2,523 edges
│           └── summaries_method4.jsonl
│
├── step6_summarization.py              # Summarize Methods 2 & 3
├── step7_evaluate.py                   # BLEU / ROUGE / METEOR / chrF
├── requirements.txt
├── .env.example
└── README.md
```

---

## 📊 Results & Evaluation

### Knowledge Graph Quality

```
Total Nodes : 1,807      Total Edges : 2,506
─────────────────────    ───────────────────────────
Function     :  638       CLOSED_BY    : 1,280
Commit       :  352       DEFINES      :   548
Document     :  305       MODIFIED_BY  :   311
File         :  250       CALLS        :   221
Pull Request :  109       IMPORTS      :    87
Module       :   99       DEPENDS_ON   :    48
Package      :   54       MENTIONS     :    11
```

```
Graph Properties
─────────────────────────────────────────────────────
Mean Degree            : 2.77   (heavy-tail distribution)
Median Degree          : 1
Maximum Degree         : 117    (central PR hub node)
Connected Components   : 583    (mostly peripheral stubs)
Largest Component      : 1,120 nodes  (62% of graph)
Average Path Length    : 5.41
Pseudo Diameter        : 14
File Coverage          : ~100%
Function Coverage      : ~100%
```

### Privacy Leakage — Raw KG (30 sampled summaries)

```
Leakage Category          Affected    %        Avg / Summary
──────────────────────────────────────────────────────────────
Identifier leakage        28 / 30    93.3%    4.2 instances
Metadata leakage          22 / 30    73.3%    1.8 instances
Literal leakage            9 / 30    30.0%    0.7 instances
Structural (topology)     30 / 30   100.0%    topology-level
Any leakage               30 / 30   100.0%    ─
```

> ⚠️ **Every single summary** from the raw KG pipeline contained at least one sensitive disclosure.

### Summarization Quality — All Methods

```
Metric     Raw KG    Method 2    Method 3    Method 4
           (base)    Blind       Role-Aware  (k,x)-Iso
────────────────────────────────────────────────────────
BLEU-4     21.54     18.32       22.41 ★     20.87
ROUGE-1    36.20     34.17       37.83 ★     35.64
ROUGE-2    10.11      8.94       11.24 ★     10.03
ROUGE-L    26.03     23.81       28.16 ★     25.91
chrF       45.54     42.18       47.29 ★     44.73
METEOR      0.27      0.24        0.29 ★      0.27
```

> ★ **Method 3 outperforms the raw KG baseline on all six metrics** — privacy-preserving sanitization improves quality by eliminating identifier-naming noise.

### Privacy Strength Comparison

```
Attack Type           Raw KG    M2        M3        M4
──────────────────────────────────────────────────────────
Identifier leakage    ✗         ✓         ✓         ✓
Literal leakage       ✗         ✓         ✓         ✓
Metadata leakage      ✗         ✓         ✓         ✓
Topology attack       ✗         ✗         ✗         ✓
Formal guarantee      None      None      None      k-anon
P(re-identify)        1.0       ≈1.0      ≈1.0      ≤0.333
```

---

## 🔐 PEKG Sanitization Methods

### Method 2 — Blind Identifier Replacement

Every user-defined identifier is replaced with a sequential opaque placeholder. No semantic information is preserved.

```python
# Placeholder format
function  →  func_{n:03d}      # func_001, func_042 ...
parameter →  param_{n:03d}     # param_006, param_008 ...
module    →  module_{n:03d}    # module_003, module_019 ...
file      →  file_{n:03d}.py   # file_023.py ...
```

```
# Before (raw KG node)
{
  "id":        "Function:dags.src.correlation.correlation_check",
  "label":     "correlation_check",
  "file":      "dags/src/correlation.py",
  "signature": "(in_path, out_path, correlation_threshold)"
}

# After (Method 2 sanitized)
{
  "id":        "Function:dags.module_003.func_023",
  "label":     "func_023",
  "file":      "dags/module_003/file_023.py",
  "signature": "(param_006, param_007, param_008)"
}
```

| Metric                | Value          |
|-----------------------|----------------|
| Files processed       | 91             |
| Total replacements    | 765            |
| Parameter replacements| 377 (49.3%)    |
| Function def rewrites | 140 (18.3%)    |
| Import replacements   | 84 (11.0%)     |
| Standalone identifiers| 81 (10.6%)     |
| Function call rewrites| 73 (9.5%)      |
| URL masking           | 10 (1.3%)      |
| Privacy violations    | 0              |
| Node overhead         |     0%         |

**Blocklists used:**
- `STDLIB_MODULES` — 78 entries (`numpy`, `pandas`, `sklearn`, `mlflow`, `airflow`, `boto3`, `torch`, ...)
- `SKIP_PARAMS` — 47 entries (`self`, `cls`, `data`, `path`, `x`, `y`, `i`, `j`, ...)

---

### Method 3 — Role-Aware Identifier Replacement

Same 5-step pipeline as Method 2, but Step 2 classifies each identifier into one of **9 semantic role categories** before assigning a placeholder. The role is encoded in the placeholder name.

```python
# Role detection algorithm
def detect_role(name, keyword_dict):
    words = re.sub(r'([A-Z])', r'_\1', name).lower()
    words = re.split(r'[_\s]+', words)
    role_scores = defaultdict(int)
    for word in words:
        for role, keywords in keyword_dict.items():
            if word in keywords:
                role_scores[role] += 1
    return max(role_scores, key=role_scores.get) if role_scores else "utility"

# Placeholder format
{role}_func_{n:03d}    # e.g. data_cleaning_func_001
```

#### Role Taxonomy

```
Role Category         Sample Keywords                          Functions
──────────────────────────────────────────────────────────────────────────
analysis              test, check, correlation, metric            34
feature_engineering   rfm, segment, geographic, seasonal          14
data_cleaning         handle, normalize, anomaly, drop            12
ml_model              train, predict, pca, cluster                11
data_loader           load, fetch, ingest, unzip                   7
utility               init, config, setup, build                   7
data_saver            save, write, export, upload                  3
pipeline              run, airflow, dag, orchestrate               2
visualization         plot, heatmap, chart, radar                  1
──────────────────────────────────────────────────────────────────────────
Total                                                             91
```

#### Concrete Mappings (Original → Role-Aware Placeholder)

```python
# Original          →  Method 3 Placeholder
handle_anomalous_codes    →  data_cleaning_func_001
correlation_check         →  analysis_func_001
save_heatmap              →  data_saver_func_001
save_correlations...      →  data_saver_func_002
customers_behavior        →  feature_engineering_func_002
geographic_features       →  feature_engineering_func_003
pca_                      →  ml_model_func_002
train_model               →  ml_model_func_010
run_training_job          →  pipeline_func_001
radar_charts              →  visualization_func_001

# Parameters
in_path                   →  param_006
out_path                  →  param_007
correlation_threshold     →  param_008

# Files / Modules
dags/src/correlation.py   →  visualization_file_001.py
src.anomaly_code_handler  →  module_l2_007
src.correlation           →  module_l2_019
```

#### Before / After (Method 3)

```python
# Before (raw KG node)
{
  "id":    "Function:dags.src.correlation.correlation_check",
  "label": "correlation_check",
  "file":  "dags/src/correlation.py",
  "signature": "(in_path, out_path, correlation_threshold)"
}

# After (Method 3 — role-aware sanitized)
{
  "id":    "Function:dags.module_l2_019.analysis_func_001",
  "label": "analysis_func_001",
  "file":  "dags/module_l1_014/module_l2_019.py",
  "signature": "(param_006, param_007, param_008)"
}
```

```
What changed  →  label, file path, parameter names
What stays    →  docstring, PR provenance, CALLS edges, lineno, node type
```

**Identifier Map Summary:**

```
Functions         :  91 mappings
Parameters        :  65 mappings
Files             :  91 mappings
Internal Modules  :  34 mappings
Classes           :   0 (none in this codebase)
─────────────────────────────────
Total             : 426 mappings
```

---

### Method 4 — (k,x)-Isomorphism for Software Knowledge Graphs

Adapted from [Bellomarini et al. (2024)](https://arxiv.org/abs/2402.XXXXX). First application of topology-level anonymization to software KGs. Operates on the Method 3 KG directly — no source code modification.

#### Core Concept

```
Problem: Even after Methods 2 & 3 remove all identifiers,
the CALL-GRAPH TOPOLOGY uniquely identifies 24 of 89 modules.

Topology Signature:
σ(f) = (n_funcs, n_internal_calls, ext_calls_out, ext_calls_in,
         degree_seq, chain_motifs, fan_out_motifs, fan_in_motifs)

Goal: Every module's σ(f) must appear in an isomorphism class of
size ≥ k=3.  An adversary observing any module's topology can
identify the target with probability at most:

         P(re-identify) ≤ 1 / k_min = 0.333
```

#### Parameters

```python
k = 3    # anonymity set size — every module needs k-1 structural twins
x = 1    # neighbourhood radius — 1-hop CALLS expansion
D = 2    # role diversity — each k-group needs ≥ 2 distinct role categories
Algorithm = "KGUARD"   # selective augmentation (preferred over KLONE)
```

#### Step 1: Subgraph Extraction

```
89 module subgraphs extracted
38 natural isomorphism classes found:
   k ≥ 3 already  :  4 classes   (safe)
   k = 2          : 10 classes   (deficient)
   k = 1 singletons: 24 classes  (fully vulnerable, P(re-id)=1.0)

Deficient classes requiring KGUARD : 34
```

#### Step 2: KGUARD Anonymization

```python
# KGUARD algorithm (simplified)
for each deficient class C where |C| < k:
    while |C| < k:
        # Find the nearest real module by topology edit distance
        candidate = argmin edit_distance(sigma(module), sigma(C))
        # Add minimal synthetic elements to make it isomorphic to C
        add_synthetic_functions(candidate, delta_n_funcs)
        add_synthetic_calls(candidate, delta_n_calls)
        # Register as new class member
        C.add(candidate)
    # Enforce role diversity D ≥ 2
    if distinct_roles(C) < D:
        adjust_synthetic_role_labels(C)
```

```
Results:
   Deficient classes fixed       : 34 / 34
   Real subgraphs augmented      : 58 / 89  (65%)
   Synthetic Function nodes added: 124
   Synthetic CALLS edges added   :  17
   Role-label swaps applied      :   2 groups
   Privacy violations after      :   0
   Diversity violations after    :   0
```

#### KGUARD Proof of Concept — build.py Singleton

```
# The most vulnerable module before Method 4:
File: gcpdeploy/module_l1_014/build.py
σ(f): (14, 5, 0, 1, [5,1,1,1,1], 3, 1, 0)
      = 14 functions | 5 internal calls | hub with out-degree 5
k = 1  →  P(re-identify) = 1.0  →  COMPLETELY VULNERABLE

# KGUARD resolution:
Nearest candidate: ClusterBasedRecommender.py (edit distance = 9)
  → Added +5 synthetic Function nodes

Second candidate: train.py (edit distance = 24)
  → Added +6 synthetic CALLS edges

# Result:
Isomorphism class members:
  build.py                    (real,  role: utility  )
  ClusterBasedRecommender.py  (+5 syn,role: ml_model )
  train.py                    (+6 syn,role: ml_model )

k = 3  |  D = 2  |  P(re-id) ≤ 0.333  ✅
```

#### Synthetic Node Format

```python
{
  "id":               "Function:syn_m4_function_010023",
  "type":             "Function",
  "label":            "ml_model_func_syn_042",
  "is_synthetic":     true,
  "synthetic_method": "method4_kx_isomorphism",
  "introduced_by_pr": "synthetic"
}
```

#### Step 3: Jaccard Utility Evaluation

```
Query                       M3 Count   M4 Count   Jaccard   Status
────────────────────────────────────────────────────────────────────────
caller_callee_pairs            221        221       1.000    ✅ PERFECT
module_roles                    89         89       1.000    ✅ PERFECT
productive_files (≥3 funcs)     84         84       1.000    ✅ PERFECT
role_analysis                   94        105       0.895    ✅ OK
role_ml_model                   58         65       0.892    ✅ OK
role_feature_engineering       102        117       0.872    ✅ OK
role_data_cleaning              36         61       0.590    ⚠ Par Above threshold
role_visualization               2         17       0.118    ⚠ Small set
role_data_loader                 0         24       0.000    ℹ Metric artefact*
────────────────────────────────────────────────────────────────────────
Average                                             0.707    ✅ Acceptable
```

> *`data_loader` score of 0.000 is a metric artefact: Method 3 had zero data_loader-role canonical nodes. KGUARD added synthetic ones for role diversity. No real analytical content was removed.

#### Final Output

```
node_v2_method4.json : 1,931 nodes  (+124 synthetic)  →  6.9% overhead
edges_method4.json   : 2,523 edges  (+17 synthetic)   →  0.7% overhead
k_min = 3  |  k_avg = 3.87  |  k_max = 24
P(re-identify) ≤ 0.333  |  Privacy violations: 0
```

---

## 📈 Privacy-Utility Trade-off

```
Semantic Utility
(ROUGE-L)
  0.30 │                    ★ M3 (28.16)  ← Best utility
       │                   ╱╲             Role-Aware beats raw KG!
  0.28 │ ─ ─ ─ ─ ─ ─ ─ ─  ╱  ╲─ ─ utility threshold (raw KG: 26.03)
       │                 ╱    ╲
  0.26 │                ╱      🔒 M4 (25.91) ← Structural guarantee
       │               ╱
  0.24 │  M2 (23.81) ●         P(re-id) ≤ 0.333
       │  Blind
  0.22 │
       └────────────────────────────────────────────────────────
       Lexical only     Lexical + Role     Lexical + Topology
                          Privacy Strength →
```

**Key insight:** The curve is NOT monotonically decreasing. Method 3 is **Pareto-superior** to Method 2 — at the same privacy level, it achieves higher utility. Privacy and utility only trade off meaningfully at Method 4, which adds structural protection.

---

## 🔧 Configuration

### `PEKG_Sanitization/Method2_Blind/config.py`

```python
# Paths
BASE_DIR = r"path/to/project"
REPO_SOURCE = os.path.join(BASE_DIR, "Ecommerce-Data-MLOps")
REPO_DEST   = os.path.join(BASE_DIR, "Ecommerce-Data-MLOps-Sanitized")
RESULTS_DIR = os.path.join(BASE_DIR, "Method2_Blind", "results")

# Blocklists
STDLIB_MODULES = {
    "numpy", "pandas", "sklearn", "mlflow", "airflow",
    "boto3", "torch", "tensorflow", "os", "sys", "ast",
    "json", "re", "collections", "datetime", "pathlib",
    # ... 78 entries total
}

SKIP_PARAMS = {
    "self", "cls", "args", "kwargs", "data", "path",
    "file", "df", "config", "x", "y", "i", "j", "k",
    # ... 47 entries total
}
```

### `PEKG_Sanitization/Method3_RuleAware/config_method3.py`

```python
# Role keyword taxonomy
ROLE_KEYWORDS = {
    "data_cleaning":       {"clean","handle","remove","normalize","anomaly",
                            "missing","null","drop","impute","outlier","duplicate"},
    "data_loader":         {"load","read","fetch","ingest","download",
                            "import","parse","unzip","retrieve"},
    "data_saver":          {"save","write","export","store","persist","upload","publish"},
    "feature_engineering": {"rfm","segment","geographic","seasonal",
                            "cancellation","transaction","derive"},
    "ml_model":            {"train","predict","fit","pca","cluster",
                            "recommend","encode","scaler","infer"},
    "analysis":            {"analyze","check","correlation","metric",
                            "score","test","silhouette","health"},
    "visualization":       {"plot","draw","heatmap","chart","radar","histogram"},
    "pipeline":            {"run","execute","airflow","dag","main","orchestrate"},
    "utility":             {"util","helper","init","config","setup","initialize","build"},
}
```

### `PEKG_Sanitization/Method4_KX_Isomorphism/config_method4.py`

```python
# KGUARD parameters
K = 3          # minimum isomorphism class size
X = 1          # neighbourhood expansion radius (hops)
D = 2          # minimum role diversity per k-group
ALGORITHM = "KGUARD"   # "KGUARD" or "KLONE"

# Topology signature components
SIGNATURE_DIMS = [
    "n_funcs",           # number of functions defined by the file
    "n_internal_calls",  # CALLS edges internal to the module
    "ext_calls_out",     # outgoing cross-module CALLS
    "ext_calls_in",      # incoming cross-module CALLS
    "degree_seq",        # sorted degree sequence (tuple)
    "chain_motifs",      # 3-node cascade count
    "fan_out_motifs",    # 3-node fan-out count
    "fan_in_motifs",     # 3-node fan-in count
]

# Input KG (from Method 3)
INPUT_NODES = "Method3_RuleAware/results/node_v2_method3.json"
INPUT_EDGES = "Method3_RuleAware/results/edges_method3.json"
```

---

## 📦 Key Output Files

| File | Description | Size |
|------|-------------|------|
| `Testing_GitHub_Code/node_v2.json` | Raw KG nodes | 1,807 nodes |
| `Testing_GitHub_Code/edges.json` | Raw KG edges | 2,506 edges |
| `Method2_Blind/results/identifier_map.json` | Blind placeholder map | sequential `func_N` |
| `Method2_Blind/results/node_v2_method2.json` | Method 2 sanitized KG | 1,807 nodes, 0% overhead |
| `Method3_RuleAware/results/identifier_map.json` | Role-aware map | 426 mappings |
| `Method3_RuleAware/results/node_v2_method3.json` | Method 3 sanitized KG | 1,807 nodes, 0% overhead |
| `Method4_KX_Isomorphism/results/subgraphs_method4.json` | Topology signatures | 89 subgraphs |
| `Method4_KX_Isomorphism/results/node_v2_method4.json` | Method 4 anonymized KG | 1,931 nodes, 6.9% overhead |
| `Method4_KX_Isomorphism/results/summaries_method4.jsonl` | LLM summaries | per function |
| `summaries_method2.jsonl` | Method 2 summaries | ROUGE-L: 23.81 |
| `summaries_method3.jsonl` | Method 3 summaries | ROUGE-L: 28.16 ★ |
| `evaluation_metrics.csv` | Full metric comparison | all 6 metrics × 4 conditions |

---

## 🧠 LLM Summarization

### Prompt Template

```python
prompt = f"""You are a code assistant working with a privacy-sanitized MLOps codebase.

Function: {label}
Role:     {role}
Signature: {signature}
Calls:    {callee_labels}
Called by: {caller_labels}
Docstring: {docstring[:500]}

Summarize this function in 2-3 sentences, focusing on:
1. What it does (functional role)
2. Its position in the MLOps pipeline
3. Its relationships with callers/callees

Summary:"""
```

### APIs Used

```python
# Methods 2 & 3 — Anthropic API (step6_summarization.py)
import anthropic
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
model  = "claude-sonnet-4-20250514"

# Method 4 — OpenRouter API (step4_summarize_method4.py)
# Primary : meta-llama/llama-3.3-70b-instruct:free
# Fallback : mistralai/mistral-7b-instruct
#          → qwen/qwen3-14b-instruct
#          → google/gemma-3-27b-it
# Backoff  : base=2s, max_retries=6
```

---

## 📏 Evaluation Metrics

```python
# All metrics are self-contained — no NLTK dependency (step7_evaluate.py)

BLEU-4   = BP × exp(Σ (1/4) × log(p_n))          # 4-gram precision
ROUGE-1  = F1(unigram overlap)
ROUGE-2  = F1(bigram overlap)
ROUGE-L  = F1(longest common subsequence)
chrF     = character n-gram F-score
METEOR   = harmonic mean (precision, recall) with stemming

Reference : developer-authored docstrings (88 canonical functions)
Hypothesis: LLM-generated summaries from sanitized KG context
```

---


---

## 🤝 Acknowledgements

- **Supervisors:** Dr. Sridhar Chimalakonda and Dr. G. Ramakrishna, IIT Tirupati
- **Target codebase:** [`Thomas-George-T/Ecommerce-Data-MLOps`](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps)
- **Privacy framework:** Bellomarini et al. (2024) — KLONE/KGUARD algorithms
- **LLM APIs:** Anthropic Claude, OpenRouter (Llama 3.3 70B)

---

<div align="center">

**Department of Computer Science & Engineering**
**Indian Institute of Technology Tirupati**

*Jatothu Srinivas Nayak — CS24M125*

![IIT Tirupati](https://img.shields.io/badge/IIT-Tirupati-1F3864?style=flat-square)
![MTP](https://img.shields.io/badge/MTP-2025-375623?style=flat-square)
![Privacy](https://img.shields.io/badge/Privacy-Preserved-4B2D83?style=flat-square)
![Code](https://img.shields.io/badge/Code-Summarization-C55A11?style=flat-square)

</div>
