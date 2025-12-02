# import json
# import os
# import re
# from pathlib import Path
# import networkx as nx
# import matplotlib.pyplot as plt
# from matplotlib.gridspec import GridSpec
# import pandas as pd

# # ============ PRIVACY DETECTION ============

# PRIVACY_PATTERNS = {
#     'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
#     'username': r'(user|username|login|uid|author)[\s:=]+[\'"]?([A-Za-z0-9_]+)[\'"]?',
#     'password': r'(password|passwd|pwd|secret|token|apikey|api_key)[\s:=]+[\'"]?([^\s\'\"]+)[\'"]?',
#     'api_key': r'(api[_-]?key|secret[_-]?key|access[_-]?token)[\s:=]+[\'"]?([A-Za-z0-9_\-]+)[\'"]?',
#     'path': r'(\/home\/|\/root\/|C:\\Users\\|\/var\/|\/etc\/)',
#     'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
#     'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
#     'database_url': r'(mysql|postgres|mongodb|redis):[\/\/]?[^\s]+',
#     'aws_key': r'AKIA[0-9A-Z]{16}',
#     'function_name': r'def\s+([a-z_][a-z0-9_]*)\(',
# }

# def detect_privacy_leaks(text, filename=''):
#     """Detect privacy-sensitive information in text"""
#     leaks = {}
    
#     for pattern_name, pattern in PRIVACY_PATTERNS.items():
#         matches = re.finditer(pattern, text, re.IGNORECASE)
#         found = [m.group(0) for m in matches]
#         if found:
#             leaks[pattern_name] = list(set(found))[:5]  # Limit to 5 per type
    
#     return leaks

# def read_file_safe(filepath, max_size=50000):
#     """Read file content safely"""
#     try:
#         if os.path.getsize(filepath) > max_size:
#             with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
#                 return f.read(max_size)
#         with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
#             return f.read()
#     except Exception as e:
#         return f"Error reading file: {str(e)}"

# # ============ KG-BASED CONTEXT RETRIEVAL ============

# def load_kg_data():
#     """Load the KG from JSON"""
#     with open('results/node_v2.json', 'r', encoding='utf-8') as f:
#         nodes = json.load(f)
    
#     with open('results/kg_edges.csv', 'r', encoding='utf-8') as f:
#         csv_lines = f.readlines()
    
#     edges = []
#     for line in csv_lines[1:]:
#         parts = line.strip().split(',', 2)
#         if len(parts) >= 2:
#             edges.append((parts[0].strip(), parts[1].strip(), 
#                          parts[2].strip() if len(parts) > 2 else 'UNKNOWN'))
    
#     return nodes, edges

# def build_graph(nodes, edges):
#     """Build full KG"""
#     G = nx.DiGraph()
    
#     for node in nodes:
#         node_id = node.get('id', '')
#         label = node.get('label', node_id.split(':')[-1] if ':' in node_id else node_id)
#         node_type = node.get('type', 'Unknown')
        
#         # Create a copy of node attrs without 'id', 'label', 'type' to avoid conflicts
#         node_attrs = {k: v for k, v in node.items() if k not in ['id', 'label', 'type']}
        
#         G.add_node(node_id, label=label, type=node_type, **node_attrs)
    
#     for src, tgt, etype in edges:
#         if src in G.nodes() and tgt in G.nodes():  # Only add if both nodes exist
#             G.add_edge(src, tgt, edge_type=etype)
    
#     return G

# def get_node_context(target_id, G, radius=2, max_neighbors=8):
#     """Retrieve neighborhood subgraph for target"""
#     if target_id not in G:
#         return None, []
    
#     neighbors = set([target_id])
#     queue = [(target_id, 0)]
    
#     while queue:
#         node, dist = queue.pop(0)
#         if dist < radius:
#             for nbr in list(G.successors(node))[:max_neighbors//2]:
#                 if nbr not in neighbors:
#                     neighbors.add(nbr)
#                     queue.append((nbr, dist + 1))
            
#             for pred in list(G.predecessors(node))[:max_neighbors//2]:
#                 if pred not in neighbors:
#                     neighbors.add(pred)
#                     queue.append((pred, dist + 1))
    
#     subgraph = G.subgraph(list(neighbors))
#     context_nodes = [G.nodes[n].get('label', n.split(':')[-1]) for n in list(neighbors)[:5]]
    
#     return subgraph, context_nodes

# def generate_summary(filename, file_content, context_nodes):
#     """Generate summary with KG context"""
#     # Extract first 200 chars of meaningful content
#     clean_content = ' '.join(file_content.split('\n')[:10])
    
#     # Determine file type
#     if filename.endswith('.py'):
#         file_type = "Python module"
#         if 'test' in filename:
#             file_type += " (test file)"
#     elif filename.endswith('.json'):
#         file_type = "JSON configuration"
#     elif filename.endswith('.csv'):
#         file_type = "Data file (CSV)"
#     else:
#         file_type = "Code file"
    
#     # Build summary
#     summary = f"The {file_type} '{filename}' "
    
#     if 'def ' in file_content:
#         funcs = re.findall(r'def\s+([a-z_][a-z0-9_]*)\(', file_content)
#         if funcs:
#             summary += f"defines {len(funcs)} function(s): {', '.join(funcs[:3])}. "
    
#     if 'import ' in file_content:
#         imports = re.findall(r'import\s+([a-zA-Z0-9_.]+)', file_content)
#         if imports:
#             summary += f"Imports: {', '.join(set(imports[:3]))}. "
    
#     if context_nodes:
#         summary += f"Related components: {', '.join(context_nodes)}. "
    
#     return summary

# # ============ VISUALIZATION 1: Summary with Privacy Detection ============

# def create_summary_privacy_image(filename, file_content, summary, privacy_leaks, output_path):
#     """Create image showing summary and detected privacy leaks"""
    
#     fig = plt.figure(figsize=(14, 9))
#     gs = GridSpec(2, 2, figure=fig, height_ratios=[0.6, 0.4], hspace=0.4, wspace=0.3)
    
#     # ===== TOP LEFT: File Information =====
#     ax_info = fig.add_subplot(gs[0, 0])
#     ax_info.axis('off')
    
#     y_pos = 0.95
#     ax_info.text(0.5, y_pos, f'File: {filename}', fontsize=12, fontweight='bold',
#                 ha='center', transform=ax_info.transAxes,
#                 bbox=dict(boxstyle='round,pad=0.5', facecolor='#4ECDC4', 
#                          edgecolor='black', linewidth=2))
#     y_pos -= 0.15
    
#     ax_info.text(0.05, y_pos, 'Summary:', fontsize=10, fontweight='bold',
#                 transform=ax_info.transAxes, color='#333333')
#     y_pos -= 0.08
    
#     # Wrap summary text
#     from textwrap import wrap
#     summary_wrapped = '\n'.join(wrap(summary, width=80))
#     ax_info.text(0.05, y_pos, summary_wrapped, fontsize=9, transform=ax_info.transAxes,
#                 color='#555555', verticalalignment='top', family='monospace',
#                 bbox=dict(boxstyle='round', facecolor='#f9f9f9', alpha=0.8, 
#                          edgecolor='gray', linewidth=1))
    
#     # ===== TOP RIGHT: File Statistics =====
#     ax_stats = fig.add_subplot(gs[0, 1])
#     ax_stats.axis('off')
    
#     y_pos = 0.95
#     ax_stats.text(0.5, y_pos, 'File Statistics', fontsize=12, fontweight='bold',
#                  ha='center', transform=ax_stats.transAxes,
#                  bbox=dict(boxstyle='round,pad=0.5', facecolor='#95E1D3', 
#                           edgecolor='black', linewidth=2))
#     y_pos -= 0.12
    
#     file_size = len(file_content)
#     lines = len(file_content.split('\n'))
#     funcs = len(re.findall(r'def\s+', file_content))
#     classes = len(re.findall(r'class\s+', file_content))
#     imports = len(re.findall(r'import\s+', file_content))
    
#     stats = [
#         (f'File Size', f'{file_size} bytes'),
#         (f'Lines of Code', f'{lines}'),
#         (f'Functions', f'{funcs}'),
#         (f'Classes', f'{classes}'),
#         (f'Imports', f'{imports}'),
#     ]
    
#     for stat_name, stat_val in stats:
#         ax_stats.text(0.05, y_pos, f'{stat_name}:', fontsize=9, transform=ax_stats.transAxes,
#                      fontweight='bold', color='#333333')
#         ax_stats.text(0.70, y_pos, stat_val, fontsize=9, transform=ax_stats.transAxes,
#                      color='#FF6B6B', fontweight='bold', family='monospace')
#         y_pos -= 0.08
    
#     # ===== BOTTOM: PRIVACY LEAKAGE DETECTION =====
#     ax_privacy = fig.add_subplot(gs[1, :])
#     ax_privacy.axis('off')
    
#     y_pos = 0.95
#     if privacy_leaks:
#         ax_privacy.text(0.5, y_pos, '⚠️  PRIVACY LEAKAGE DETECTED', fontsize=12, fontweight='bold',
#                        ha='center', transform=ax_privacy.transAxes, color='white',
#                        bbox=dict(boxstyle='round,pad=0.6', facecolor='#FF6B6B', 
#                                 edgecolor='#C92A2A', linewidth=2.5))
#         y_pos -= 0.12
        
#         # Show detected leaks
#         for leak_type, leak_values in sorted(privacy_leaks.items()):
#             ax_privacy.text(0.05, y_pos, f'🔓 {leak_type.upper()}:', fontsize=9.5, 
#                            fontweight='bold', transform=ax_privacy.transAxes,
#                            color='#C92A2A')
#             y_pos -= 0.07
            
#             # Show leaked values (redacted)
#             for leak_val in leak_values:
#                 if len(leak_val) > 40:
#                     leak_val = leak_val[:37] + '...'
#                 redacted = '*' * len(leak_val)
#                 ax_privacy.text(0.10, y_pos, f'Found: [{redacted}]', fontsize=8.5, 
#                                transform=ax_privacy.transAxes, color='#FF6B6B',
#                                family='monospace',
#                                bbox=dict(boxstyle='round', facecolor='#ffe6e6', 
#                                         alpha=0.7, edgecolor='#FF6B6B'))
#                 y_pos -= 0.06
            
#             y_pos -= 0.02
#     else:
#         ax_privacy.text(0.5, y_pos, '✓ No Privacy Leaks Detected', fontsize=11, fontweight='bold',
#                        ha='center', transform=ax_privacy.transAxes, color='white',
#                        bbox=dict(boxstyle='round,pad=0.6', facecolor='#4ECDC4', 
#                                 edgecolor='#0B7A8C', linewidth=2))
#         y_pos -= 0.15
#         ax_privacy.text(0.5, y_pos, 'This file appears safe for sharing', fontsize=9,
#                        ha='center', transform=ax_privacy.transAxes, style='italic',
#                        color='#333333')
    
#     # Footer
#     fig.text(0.5, 0.01, 'Privacy detection scans for emails, credentials, paths, and sensitive identifiers', 
#             ha='center', fontsize=8, style='italic', color='gray')
    
#     plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white',
#                edgecolor='black', pad_inches=0.3)
#     print(f"✓ Saved: {output_path}")
#     plt.close()

# # ============ VISUALIZATION 2: Privacy Comparison Matrix =====

# def create_privacy_report_image(analysis_results, output_path):
#     """Create comprehensive privacy report across multiple files"""
    
#     fig = plt.figure(figsize=(14, 10))
#     gs = GridSpec(2, 1, figure=fig, height_ratios=[0.7, 0.3], hspace=0.4)
    
#     # ===== TOP: Privacy Risk Matrix =====
#     ax_matrix = fig.add_subplot(gs[0])
#     ax_matrix.axis('off')
    
#     # Prepare data
#     file_names = [r['file'] for r in analysis_results]
#     leak_types = set()
#     for r in analysis_results:
#         leak_types.update(r['leaks'].keys())
#     leak_types = sorted(leak_types)
    
#     # Create matrix data
#     matrix_data = []
#     for fname in file_names:
#         row_data = [fname]
#         result = next((r for r in analysis_results if r['file'] == fname), None)
#         if result:
#             for leak_type in leak_types:
#                 count = len(result['leaks'].get(leak_type, []))
#                 row_data.append(str(count) if count > 0 else '—')
#         matrix_data.append(row_data)
    
#     # Create table
#     cols = ['File'] + [lt.upper()[:8] for lt in leak_types]
#     table_data = [cols] + matrix_data
    
#     table = ax_matrix.table(cellText=table_data, cellLoc='center', loc='center',
#                            colWidths=[0.25] + [0.09] * len(leak_types))
#     table.auto_set_font_size(False)
#     table.set_fontsize(8.5)
#     table.scale(1, 2)
    
#     # Color header
#     for i in range(len(cols)):
#         table[(0, i)].set_facecolor('#4ECDC4')
#         table[(0, i)].set_text_props(weight='bold', color='white')
    
#     # Color cells by risk
#     for i in range(1, len(table_data)):
#         table[(i, 0)].set_facecolor('#f0f0f0')
#         for j in range(1, len(cols)):
#             cell_val = table_data[i][j]
#             if cell_val == '—':
#                 table[(i, j)].set_facecolor('#e8f5f3')
#             else:
#                 count = int(cell_val)
#                 if count >= 3:
#                     table[(i, j)].set_facecolor('#FF6B6B')
#                     table[(i, j)].set_text_props(weight='bold', color='white')
#                 elif count >= 1:
#                     table[(i, j)].set_facecolor('#FFB3B3')
#                     table[(i, j)].set_text_props(weight='bold')
    
#     ax_matrix.text(0.5, 1.05, 'Privacy Risk Assessment Matrix - All Files', 
#                   fontsize=13, fontweight='bold', ha='center', 
#                   transform=ax_matrix.transAxes)
    
#     # ===== BOTTOM: Risk Summary =====
#     ax_summary = fig.add_subplot(gs[1])
#     ax_summary.axis('off')
    
#     y_pos = 0.95
#     ax_summary.text(0.05, y_pos, 'Risk Summary:', fontsize=11, fontweight='bold',
#                    transform=ax_summary.transAxes, color='#333333')
#     y_pos -= 0.12
    
#     total_files = len(analysis_results)
#     files_with_leaks = len([r for r in analysis_results if r['leaks']])
#     total_leaks = sum(len(r['leaks']) for r in analysis_results)
    
#     summary_stats = [
#         (f'Total Files Scanned', str(total_files)),
#         (f'Files with Leaks', str(files_with_leaks)),
#         (f'Total Leak Types Found', str(total_leaks)),
#         (f'Risk Level', 'HIGH' if files_with_leaks > total_files * 0.3 else ('MEDIUM' if files_with_leaks > 0 else 'LOW')),
#     ]
    
#     for stat_name, stat_val in summary_stats:
#         color = '#FF6B6B' if 'HIGH' in stat_val or stat_val.isdigit() and int(stat_val) > 0 else '#4ECDC4'
#         ax_summary.text(0.05, y_pos, f'{stat_name}:', fontsize=9.5, transform=ax_summary.transAxes,
#                        fontweight='bold', color='#333333')
#         ax_summary.text(0.50, y_pos, stat_val, fontsize=9.5, transform=ax_summary.transAxes,
#                        color=color, fontweight='bold', family='monospace')
#         y_pos -= 0.08
    
#     # Recommendations
#     y_pos -= 0.05
#     ax_summary.text(0.05, y_pos, '📋 Recommendations:', fontsize=10, fontweight='bold',
#                    transform=ax_summary.transAxes, color='#333333')
#     y_pos -= 0.08
    
#     recs = [
#         "• Redact credentials and API keys before sharing code",
#         "• Use environment variables for sensitive config",
#         "• Implement privacy-aware KG for open-source release",
#     ]
    
#     for rec in recs:
#         ax_summary.text(0.05, y_pos, rec, fontsize=8.5, transform=ax_summary.transAxes,
#                        color='#555555', style='italic')
#         y_pos -= 0.06
    
#     plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white',
#                edgecolor='black', pad_inches=0.3)
#     print(f"✓ Saved: {output_path}")
#     plt.close()

# # ============ MAIN EXECUTION ============

# def main():
#     print("\n" + "="*70)
#     print("PRIVACY-AWARE CODE SUMMARIZER WITH KG CONTEXT")
#     print("="*70)
    
#     # Load KG
#     print("\n[1/4] Loading Knowledge Graph...")
#     nodes, edges = load_kg_data()
#     G = build_graph(nodes, edges)
#     print(f"✓ Loaded {len(G.nodes())} nodes, {len(G.edges())} edges")
    
#     # Scan project files
#     print("\n[2/4] Scanning project files for privacy leaks...")
#     project_root = Path('../../Ecommerce-Data-MLOps')  # Adjust path as needed
    
#     analysis_results = []
    
#     # Sample files to analyze
#     sample_files = [
#         'src/correlation.py',
#         'src/data_loader.py',
#         'config/feature_processing.json',
#         'README.md',
#     ]
    
#     for file_path in sample_files:
#         full_path = project_root / file_path
#         if full_path.exists():
#             print(f"  Analyzing: {file_path}")
            
#             content = read_file_safe(full_path)
#             leaks = detect_privacy_leaks(content, file_path)
            
#             # Get KG context
#             file_node_id = f"File:{file_path}"
#             context = []
#             if file_node_id in G.nodes():
#                 subgraph, context = get_node_context(file_node_id, G)
            
#             # Generate summary
#             summary = generate_summary(file_path, content, context)
            
#             analysis_results.append({
#                 'file': file_path,
#                 'content': content,
#                 'summary': summary,
#                 'leaks': leaks,
#                 'context': context,
#             })
            
#             # Create individual summary image
#             output_img = f'results/privacy_summary_{file_path.replace("/", "_").replace(".", "_")}.png'
#             os.makedirs('results', exist_ok=True)
#             create_summary_privacy_image(file_path, content, summary, leaks, output_img)
#         else:
#             print(f"  ⚠ File not found: {file_path}")
    
#     # Create overall privacy report
#     if analysis_results:
#         print("\n[3/4] Creating privacy report matrix...")
#         create_privacy_report_image(analysis_results, 'results/privacy_report_all_files.png')
    
#         # Print summary to console
#         print("\n[4/4] Summary Report:")
#         print("-" * 70)
#         for result in analysis_results:
#             print(f"\n📄 FILE: {result['file']}")
#             print(f"   Summary: {result['summary'][:100]}...")
#             if result['leaks']:
#                 print(f"   ⚠️  PRIVACY LEAKS FOUND:")
#                 for leak_type, values in result['leaks'].items():
#                     print(f"      - {leak_type}: {len(values)} instance(s)")
#             else:
#                 print(f"   ✓ No privacy leaks detected")
        
#         print("\n" + "="*70)
#         print("✓ Analysis complete! Images saved to results/")
#         print("="*70 + "\n")
#     else:
#         print("\n⚠ No files found to analyze!")

# if __name__ == '__main__':
#     main()


import json
import os
import re
from pathlib import Path
from collections import defaultdict

# Patterns for sensitive data
SENSITIVE_PATTERNS = {
    'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    'github_username': r'(?:github\.com\/|@)([A-Za-z0-9_-]+)',
    'commit_author': r'\"author[\"|\']?\s*:\s*[\"|\']?([^\"|\'\n]+)',
    'commit_message': r'\"message[\"|\']?\s*:\s*[\"|\']?([^\"|\'\n]{20,})',
    'user_path': r'\/home\/[a-zA-Z0-9_]+|C:\\\\Users\\\\[a-zA-Z0-9_]+',
    'email_in_commit': r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+',
    'pr_author': r'\"author[\"|\']?\s*:\s*\{[^}]*\"login[\"|\']?\s*:\s*[\"|\']?([^\"|\'\n]+)',
    'user_credentials': r'(password|token|secret|apikey|api_key)[\s:=]+[\'"]?([^\s\'"]+)',
}

def scan_file_for_pii(filepath):
    """Scan a file for PII and proprietary data"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(100000)  # Read first 100KB
    except Exception as e:
        return None
    
    findings = {}
    
    for pattern_name, pattern in SENSITIVE_PATTERNS.items():
        matches = re.finditer(pattern, content, re.IGNORECASE)
        found = list(set([m.group(0) for m in matches]))
        if found:
            findings[pattern_name] = len(found)
    
    return findings if findings else None

def categorize_files():
    """Categorize files by their sensitivity to data leakage"""
    
    testing_root = Path('.')
    results_dir = testing_root / 'results'
    
    # Files that likely contain PII/proprietary data
    high_risk_files = {
        'commits.json': 'Contains commit author emails, names, and messages',
        'function_commits.json': 'Links functions to commits with author info',
        'function_commits_repaired.json': 'Repaired function-commit mappings with PII',
        'function_commits_repaired_v2.json': 'V2 of repaired function-commit data',
        'manifest.json': 'Repository manifest with file paths and metadata',
        'manifest_enriched.json': 'Enriched manifest with author/commit info',
        'commit_to_prs.json': 'Maps commits to PRs with author information',
        'ci_env_secrets_report.json': 'CI environment variables and secrets',
        'configs_secrets_report.json': 'Configuration files with potential secrets',
        'nodes_with_commit.json': 'KG nodes with commit history',
        'call_graph_enriched.json': 'Call graph with commit enrichment',
    }
    
    # Python files that process PII data
    high_risk_python_files = {
        'stepI_map_functions_to_commits.py': 'Maps functions to commits (author extraction)',
        'stepP_enrich_with_commits.py': 'Enriches KG with commit metadata (PII source)',
        'stepQ_enrich_nodes_extended.py': 'Extended node enrichment with commit info',
        'stepL_parse_configs_and_detect_secrets.py': 'Parses configs and detects secrets',
        'stepK_parse_ci_workflows.py': 'Extracts CI workflow secrets/env vars',
        'stepM_enrich_call_graph_with_jedi.py': 'Enriches call graph (may include metadata)',
        'stepD_merge_artifacts_into_manifest.py': 'Merges artifacts that may contain PII',
        'stepO_merge_into_kg.py': 'Merges all data into KG (includes commit info)',
    }
    
    print("\n" + "="*80)
    print("SENSITIVE FILES ANALYSIS - FILES THAT REVEAL PII & PROPRIETARY DATA")
    print("="*80)
    
    print("\n📊 JSON FILES WITH HIGH RISK (DIRECT DATA):")
    print("-" * 80)
    
    for filename, description in high_risk_files.items():
        filepath = results_dir / filename
        if filepath.exists():
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Count records
                if isinstance(data, list):
                    record_count = len(data)
                elif isinstance(data, dict):
                    record_count = len(data)
                else:
                    record_count = 1
                
                # Scan for PII
                pii_findings = scan_file_for_pii(filepath)
                
                print(f"\n🔴 {filename}")
                print(f"   Description: {description}")
                print(f"   Records/Items: {record_count}")
                print(f"   File Size: {filepath.stat().st_size / 1024:.1f} KB")
                
                if pii_findings:
                    print(f"   PII Found:")
                    for pii_type, count in pii_findings.items():
                        print(f"      - {pii_type}: {count} instances")
                
            except Exception as e:
                print(f"\n🔴 {filename}")
                print(f"   Error reading: {str(e)}")
    
    print("\n\n🐍 PYTHON FILES PROCESSING PII (PROCESSING LOGIC):")
    print("-" * 80)
    
    for filename, description in high_risk_python_files.items():
        filepath = testing_root / filename
        if filepath.exists():
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Count lines
                lines = len(content.split('\n'))
                
                # Check for sensitive operations
                has_commit_parsing = 'commit' in content.lower() and ('author' in content.lower() or 'email' in content.lower())
                has_secret_parsing = 'secret' in content.lower() or 'password' in content.lower()
                has_pii_extraction = 'author' in content.lower() or 'user' in content.lower()
                
                print(f"\n🔴 {filename}")
                print(f"   Description: {description}")
                print(f"   Lines of Code: {lines}")
                
                if has_commit_parsing or has_secret_parsing or has_pii_extraction:
                    print(f"   Risk Operations:")
                    if has_commit_parsing:
                        print(f"      ✗ Extracts commit author/email info")
                    if has_secret_parsing:
                        print(f"      ✗ Parses secrets and credentials")
                    if has_pii_extraction:
                        print(f"      ✗ Extracts user/author information")
                
            except Exception as e:
                print(f"\n🔴 {filename}")
                print(f"   Error reading: {str(e)}")
    
    # Generate recommended files for summarization demo
    print("\n\n" + "="*80)
    print("📋 RECOMMENDED FILES FOR CODE SUMMARIZATION PRIVACY DEMO")
    print("="*80)
    
    demo_files = [
        ('stepI_map_functions_to_commits.py', 'Shows how functions are mapped to commits (reveals author info)'),
        ('stepP_enrich_with_commits.py', 'Demonstrates enrichment with commit metadata (PII source)'),
        ('stepL_parse_configs_and_detect_secrets.py', 'Parses configs revealing secrets/credentials'),
        ('function_commits_repaired_v2.json', 'Dataset: function-commit mappings with author emails'),
        ('commits.json', 'Dataset: raw commit data with author PII'),
        ('ci_env_secrets_report.json', 'Dataset: extracted secrets from CI/CD'),
    ]
    
    print("\nDirect Code Summarization Input (Python files):")
    for py_file, desc in demo_files[:3]:
        if Path(py_file).suffix == '.py':
            print(f"  1️⃣  {py_file}")
            print(f"      → {desc}")
    
    print("\nData Files to Show Privacy Leakage (JSON):")
    for json_file, desc in demo_files[3:]:
        if Path(json_file).suffix == '.json':
            print(f"  2️⃣  {json_file}")
            print(f"      → {desc}")
    
    return demo_files

def create_demo_input_list():
    """Create a JSON file with recommended demo inputs"""
    
    demo_config = {
        "python_files_to_summarize": [
            {
                "filename": "stepI_map_functions_to_commits.py",
                "reason": "Maps functions to commits - reveals author identities",
                "pii_risk": "HIGH",
                "expected_leaks": ["author emails", "usernames", "commit timestamps"],
            },
            {
                "filename": "stepP_enrich_with_commits.py",
                "reason": "Enriches KG with commit data - PII enrichment source",
                "pii_risk": "HIGH",
                "expected_leaks": ["author names", "commit messages", "PR info"],
            },
            {
                "filename": "stepL_parse_configs_and_detect_secrets.py",
                "reason": "Parses configs and extracts secrets",
                "pii_risk": "CRITICAL",
                "expected_leaks": ["API keys", "passwords", "secrets", "environment variables"],
            },
            {
                "filename": "stepK_parse_ci_workflows.py",
                "reason": "Extracts CI/CD configuration",
                "pii_risk": "HIGH",
                "expected_leaks": ["environment secrets", "webhook URLs", "credentials"],
            },
        ],
        "json_files_to_analyze": [
            {
                "filename": "results/commits.json",
                "reason": "Raw commit data with author information",
                "pii_risk": "HIGH",
                "sample_pii": ["author_email", "author_name", "commit_message", "timestamp"]
            },
            {
                "filename": "results/function_commits_repaired_v2.json",
                "reason": "Function-commit mappings with PII",
                "pii_risk": "HIGH",
                "sample_pii": ["function_name", "file_path", "author", "commit_hash"]
            },
            {
                "filename": "results/ci_env_secrets_report.json",
                "reason": "Extracted secrets from CI environment",
                "pii_risk": "CRITICAL",
                "sample_pii": ["API_KEYS", "DATABASE_URLs", "TOKENS", "CREDENTIALS"]
            },
        ]
    }
    
    with open('results/demo_sensitive_files.json', 'w') as f:
        json.dump(demo_config, f, indent=2)
    
    print("\n✓ Created: results/demo_sensitive_files.json")
    
    return demo_config

if __name__ == '__main__':
    print("\n🔍 Analyzing sensitive files in the project...\n")
    
    demo_files = categorize_files()
    create_demo_input_list()
    
    print("\n" + "="*80)
    print("✓ ANALYSIS COMPLETE")
    print("="*80)
    print("\nUse these filenames as input to privacy_aware_summarizer.py:")
    print("  - stepI_map_functions_to_commits.py")
    print("  - stepP_enrich_with_commits.py")
    print("  - stepL_parse_configs_and_detect_secrets.py")
    print("="*80 + "\n")