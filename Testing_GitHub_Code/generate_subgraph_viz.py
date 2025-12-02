# # import json
# # import networkx as nx
# # import matplotlib.pyplot as plt
# # import os

# # # Load KG data with explicit UTF-8 encoding
# # with open('results/node_v2.json', 'r', encoding='utf-8') as f:
# #     nodes = json.load(f)

# # print(f"Total nodes loaded: {len(nodes)}")

# # # Filter for PR 54 or correlation-related nodes
# # pr54_nodes = [n for n in nodes if 
# #               (n.get('introduced_by_pr') == '54' or 
# #                n.get('introduced_by_pr') == 54 or
# #                'correlation' in n.get('id', '').lower() or
# #                'save_heatmap' in n.get('id', '').lower() or
# #                'save_correlations' in n.get('id', '').lower())]

# # print(f"Found {len(pr54_nodes)} nodes related to PR 54/correlation")

# # if not pr54_nodes:
# #     print("⚠ No PR 54 nodes found. Filtering by correlation keywords...")
# #     pr54_nodes = [n for n in nodes if 'correlation' in n.get('id', '').lower()]
# #     print(f"Found {len(pr54_nodes)} correlation-related nodes")

# # if not pr54_nodes:
# #     print("⚠ Still empty. Using first 15 nodes for demo...")
# #     pr54_nodes = nodes[:15]

# # # Use 'id' field instead of 'node_id'
# # pr54_node_ids = {n['id'] for n in pr54_nodes}
# # print(f"Node IDs: {list(pr54_node_ids)[:3]}")  # Debug: show first 3

# # # Load edges
# # with open('results/kg_edges.csv', 'r', encoding='utf-8') as f:
# #     csv_lines = f.readlines()
# #     if len(csv_lines) > 1:
# #         print(f"CSV Header: {csv_lines[0].strip()}")
# #         print(f"CSV Sample: {csv_lines[1].strip()[:100]}")

# # # Parse edges properly
# # edges = []
# # for line in csv_lines[1:]:
# #     parts = line.strip().split(',', 2)  # Split on first 2 commas only
# #     if len(parts) >= 2:
# #         edges.append((parts[0].strip(), parts[1].strip()))

# # print(f"Loaded {len(edges)} edges")

# # # Build subgraph
# # G = nx.DiGraph()
# # for node in pr54_nodes:
# #     node_id = node.get('id', '')
# #     label = node.get('label', node_id.split(':')[-1] if ':' in node_id else node_id)
# #     node_type = node.get('type', 'Unknown')
# #     G.add_node(node_id, label=label, type=node_type)

# # # Add edges within subgraph
# # edge_count = 0
# # for src, tgt in edges:
# #     if src in pr54_node_ids and tgt in pr54_node_ids:
# #         G.add_edge(src, tgt)
# #         edge_count += 1

# # print(f"Subgraph: {len(G.nodes())} nodes, {edge_count} edges")

# # if len(G.nodes()) == 0:
# #     print("⚠ Empty graph. Creating demo visualization...")
# #     G.add_node("Function:correlation_check", label="correlation_check", type="Function")
# #     G.add_node("Function:save_heatmap", label="save_heatmap", type="Function")
# #     G.add_edge("Function:correlation_check", "Function:save_heatmap")

# # # Visualize
# # plt.figure(figsize=(14, 10))
# # pos = nx.spring_layout(G, k=3, iterations=50, seed=42)

# # # Draw nodes by type
# # functions = [n for n in G.nodes() if 'Function' in str(G.nodes[n].get('type', ''))]
# # files = [n for n in G.nodes() if 'File' in str(G.nodes[n].get('type', ''))]
# # others = [n for n in G.nodes() if n not in functions and n not in files]

# # print(f"Node breakdown: {len(functions)} Functions, {len(files)} Files, {len(others)} Others")

# # if functions:
# #     nx.draw_networkx_nodes(G, pos, nodelist=functions, node_color='#FF6B6B', 
# #                            node_size=2000, label='Functions', alpha=0.9)
# # if files:
# #     nx.draw_networkx_nodes(G, pos, nodelist=files, node_color='#4ECDC4', 
# #                            node_size=2500, label='Files', alpha=0.9)
# # if others:
# #     nx.draw_networkx_nodes(G, pos, nodelist=others, node_color='#95E1D3', 
# #                            node_size=1500, label='Other', alpha=0.9)

# # # Draw edges
# # nx.draw_networkx_edges(G, pos, edge_color='#666666', arrows=True, 
# #                        arrowsize=20, arrowstyle='->', width=2, alpha=0.6)

# # # Labels
# # labels = {n: G.nodes[n].get('label', n.split(':')[-1]) for n in G.nodes()}
# # nx.draw_networkx_labels(G, pos, labels, font_size=8, font_weight='bold')

# # plt.title("KG Subgraph: PR 54 (Correlation Analysis Pipeline)", fontsize=14, fontweight='bold')
# # plt.legend(scatterpoints=1, loc='upper left', fontsize=10)
# # plt.axis('off')
# # plt.tight_layout()

# # # Ensure output directory exists
# # os.makedirs('results', exist_ok=True)
# # plt.savefig('results/subgraph_pr54.png', dpi=300, bbox_inches='tight')
# # print("✓ Saved: results/subgraph_pr54.png")
# # plt.close()
# # print("Done!")


# import json
# import networkx as nx
# import matplotlib.pyplot as plt
# import matplotlib.patches as mpatches
# from matplotlib.gridspec import GridSpec
# import pandas as pd
# import os

# # Load KG data
# with open('results/node_v2.json', 'r', encoding='utf-8') as f:
#     nodes = json.load(f)

# # Load edges
# with open('results/kg_edges.csv', 'r', encoding='utf-8') as f:
#     csv_lines = f.readlines()

# edges = []
# for line in csv_lines[1:]:
#     parts = line.strip().split(',', 2)
#     if len(parts) >= 2:
#         edges.append((parts[0].strip(), parts[1].strip()))

# # Filter for PR 54
# pr54_nodes = [n for n in nodes if n.get('introduced_by_pr') == '54']

# print(f"Total PR 54 nodes: {len(pr54_nodes)}")

# # Get top correlated nodes (highest degree)
# pr54_node_ids = {n['id'] for n in pr54_nodes}

# # Build graph
# G = nx.DiGraph()
# for node in pr54_nodes:
#     node_id = node.get('id', '')
#     label = node.get('label', node_id.split(':')[-1] if ':' in node_id else node_id)
#     node_type = node.get('type', 'Unknown')
#     G.add_node(node_id, label=label, type=node_type)

# edge_count = 0
# for src, tgt in edges:
#     if src in pr54_node_ids and tgt in pr54_node_ids:
#         G.add_edge(src, tgt)
#         edge_count += 1

# print(f"Graph: {len(G.nodes())} nodes, {edge_count} edges")

# # Select top 20 nodes by degree for cleaner visualization
# top_nodes = sorted(G.nodes(), key=lambda n: G.degree(n), reverse=True)[:20]
# G_sub = G.subgraph(top_nodes).copy()

# print(f"Subgraph (top 20): {len(G_sub.nodes())} nodes, {len(G_sub.edges())} edges")

# # Create figure with GridSpec for layout
# fig = plt.figure(figsize=(16, 10))
# gs = GridSpec(3, 2, figure=fig, height_ratios=[0.7, 0.15, 0.15], hspace=0.3, wspace=0.25)

# # Main graph
# ax_graph = fig.add_subplot(gs[0, :])

# # Compute layout
# pos = nx.spring_layout(G_sub, k=2, iterations=100, seed=42)

# # Color nodes by type
# functions = [n for n in G_sub.nodes() if 'Function' in str(G_sub.nodes[n].get('type', ''))]
# files = [n for n in G_sub.nodes() if 'File' in str(G_sub.nodes[n].get('type', ''))]
# others = [n for n in G_sub.nodes() if n not in functions and n not in files]

# # Draw nodes
# if functions:
#     nx.draw_networkx_nodes(G_sub, pos, nodelist=functions, node_color='#FF6B6B', 
#                            node_size=1500, label=f'Functions ({len(functions)})', 
#                            ax=ax_graph, alpha=0.9, edgecolors='darkred', linewidths=2)
# if files:
#     nx.draw_networkx_nodes(G_sub, pos, nodelist=files, node_color='#4ECDC4', 
#                            node_size=1800, label=f'Files ({len(files)})', 
#                            ax=ax_graph, alpha=0.9, edgecolors='darkcyan', linewidths=2)
# if others:
#     nx.draw_networkx_nodes(G_sub, pos, nodelist=others, node_color='#95E1D3', 
#                            node_size=1200, label=f'Other ({len(others)})', 
#                            ax=ax_graph, alpha=0.8, edgecolors='gray', linewidths=1.5)

# # Draw edges
# nx.draw_networkx_edges(G_sub, pos, edge_color='#999999', arrows=True, 
#                        arrowsize=15, arrowstyle='->', width=1.5, alpha=0.5, ax=ax_graph)

# # Draw labels
# labels = {n: G_sub.nodes[n].get('label', n.split(':')[-1])[:20] for n in G_sub.nodes()}
# nx.draw_networkx_labels(G_sub, pos, labels, font_size=7, font_weight='bold', ax=ax_graph)

# ax_graph.set_title('KG Subgraph: PR 54 - Correlation Analysis Pipeline (Top 20 Nodes)', 
#                    fontsize=14, fontweight='bold', pad=20)
# ax_graph.legend(scatterpoints=1, loc='upper left', fontsize=10, framealpha=0.95)
# ax_graph.axis('off')

# # Stats table (top left)
# ax_stats = fig.add_subplot(gs[1, 0])
# ax_stats.axis('tight')
# ax_stats.axis('off')

# stats_data = [
#     ['Metric', 'Value'],
#     ['Total Nodes', str(len(G.nodes()))],
#     ['Total Edges', str(len(G.edges()))],
#     ['Functions', str(len([n for n in G.nodes() if 'Function' in str(G.nodes[n].get('type', ''))]))],
#     ['Files', str(len([n for n in G.nodes() if 'File' in str(G.nodes[n].get('type', ''))]))],
#     ['Density', f"{nx.density(G):.4f}"],
#     ['Avg Degree', f"{sum(dict(G.degree()).values()) / len(G.nodes()):.2f}"],
# ]

# table_stats = ax_stats.table(cellText=stats_data, cellLoc='left', loc='center',
#                              colWidths=[0.5, 0.5])
# table_stats.auto_set_font_size(False)
# table_stats.set_fontsize(9)
# table_stats.scale(1, 2)

# # Style header row
# for i in range(2):
#     table_stats[(0, i)].set_facecolor('#4ECDC4')
#     table_stats[(0, i)].set_text_props(weight='bold', color='white')

# # Alternate row colors
# for i in range(1, len(stats_data)):
#     for j in range(2):
#         if i % 2 == 0:
#             table_stats[(i, j)].set_facecolor('#f0f0f0')
#         else:
#             table_stats[(i, j)].set_facecolor('#ffffff')

# ax_stats.text(0.5, 1.15, 'Graph Statistics', ha='center', fontsize=11, fontweight='bold',
#               transform=ax_stats.transAxes)

# # Node degree table (top right)
# ax_nodes = fig.add_subplot(gs[1, 1])
# ax_nodes.axis('tight')
# ax_nodes.axis('off')

# # Top nodes by degree
# top_degree_nodes = sorted(G_sub.nodes(), key=lambda n: G_sub.degree(n), reverse=True)[:8]
# node_data = [['Node', 'In-Degree', 'Out-Degree']]
# for node in top_degree_nodes:
#     label = G_sub.nodes[node].get('label', node.split(':')[-1])[:25]
#     in_deg = G_sub.in_degree(node)
#     out_deg = G_sub.out_degree(node)
#     node_data.append([label, str(in_deg), str(out_deg)])

# table_nodes = ax_nodes.table(cellText=node_data, cellLoc='center', loc='center',
#                              colWidths=[0.5, 0.25, 0.25])
# table_nodes.auto_set_font_size(False)
# table_nodes.set_fontsize(8)
# table_nodes.scale(1, 1.8)

# # Style header
# for i in range(3):
#     table_nodes[(0, i)].set_facecolor('#FF6B6B')
#     table_nodes[(0, i)].set_text_props(weight='bold', color='white')

# for i in range(1, len(node_data)):
#     for j in range(3):
#         if i % 2 == 0:
#             table_nodes[(i, j)].set_facecolor('#ffe6e6')
#         else:
#             table_nodes[(i, j)].set_facecolor('#ffffff')

# ax_nodes.text(0.5, 1.15, 'Top Nodes by Degree', ha='center', fontsize=11, fontweight='bold',
#               transform=ax_nodes.transAxes)

# # Edge type distribution (bottom)
# ax_edge = fig.add_subplot(gs[2, :])
# ax_edge.axis('tight')
# ax_edge.axis('off')

# edge_types = {}
# for line in csv_lines[1:]:
#     parts = line.strip().split(',')
#     if len(parts) >= 3:
#         etype = parts[2] if len(parts) > 2 else 'UNKNOWN'
#         edge_types[etype] = edge_types.get(etype, 0) + 1

# edge_data = [['Edge Type', 'Count']] + [[k, str(v)] for k, v in sorted(edge_types.items(), key=lambda x: x[1], reverse=True)[:8]]

# table_edges = ax_edge.table(cellText=edge_data, cellLoc='center', loc='center',
#                             colWidths=[0.6, 0.4])
# table_edges.auto_set_font_size(False)
# table_edges.set_fontsize(9)
# table_edges.scale(1, 2)

# for i in range(2):
#     table_edges[(0, i)].set_facecolor('#95E1D3')
#     table_edges[(0, i)].set_text_props(weight='bold', color='white')

# for i in range(1, len(edge_data)):
#     for j in range(2):
#         if i % 2 == 0:
#             table_edges[(i, j)].set_facecolor('#e8f5f3')
#         else:
#             table_edges[(i, j)].set_facecolor('#ffffff')

# ax_edge.text(0.5, 1.15, 'Edge Type Distribution (PR 54)', ha='center', fontsize=11, fontweight='bold',
#              transform=ax_edge.transAxes)

# # Save
# os.makedirs('results', exist_ok=True)
# plt.savefig('results/subgraph_pr54_clean.png', dpi=300, bbox_inches='tight', facecolor='white')
# print("✓ Saved: results/subgraph_pr54_clean.png")
# plt.show()



import json
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import os

# Load KG data
with open('results/node_v2.json', 'r', encoding='utf-8') as f:
    nodes = json.load(f)

# Load edges
with open('results/kg_edges.csv', 'r', encoding='utf-8') as f:
    csv_lines = f.readlines()

edges = []
for line in csv_lines[1:]:
    parts = line.strip().split(',', 2)
    if len(parts) >= 2:
        edges.append((parts[0].strip(), parts[1].strip(), parts[2].strip() if len(parts) > 2 else 'UNKNOWN'))

# Filter for PR 54
pr54_nodes = [n for n in nodes if n.get('introduced_by_pr') == '54']
pr54_node_ids = {n['id'] for n in pr54_nodes}

print(f"Total PR 54 nodes: {len(pr54_nodes)}")

# Build full graph first
G_full = nx.DiGraph()
for node in pr54_nodes:
    node_id = node.get('id', '')
    label = node.get('label', node_id.split(':')[-1] if ':' in node_id else node_id)
    node_type = node.get('type', 'Unknown')
    G_full.add_node(node_id, label=label, type=node_type)

edge_count_full = 0
for src, tgt, etype in edges:
    if src in pr54_node_ids and tgt in pr54_node_ids:
        G_full.add_edge(src, tgt, edge_type=etype)
        edge_count_full += 1

print(f"Full Graph: {len(G_full.nodes())} nodes, {edge_count_full} edges")

# Select top 15 nodes by degree for clarity
top_nodes = sorted(G_full.nodes(), key=lambda n: G_full.degree(n), reverse=True)[:15]
G = G_full.subgraph(top_nodes).copy()

print(f"Subgraph (top 15): {len(G.nodes())} nodes, {len(G.edges())} edges")

# Create figure with two-column layout - REDUCED SIZE
fig = plt.figure(figsize=(13, 7))  # Reduced from 18x10
gs = GridSpec(1, 2, figure=fig, width_ratios=[1.2, 0.8], wspace=0.3)

# ============ LEFT COLUMN: GRAPH ============
ax_graph = fig.add_subplot(gs[0, 0])

# Compute layout with better spacing
pos = nx.spring_layout(G, k=2.5, iterations=100, seed=42)  # Reduced k

# Categorize nodes
functions = [n for n in G.nodes() if 'Function' in str(G.nodes[n].get('type', ''))]
files = [n for n in G.nodes() if 'File' in str(G.nodes[n].get('type', ''))]

# Draw edges with labels
nx.draw_networkx_edges(G, pos, edge_color='#CCCCCC', arrows=True, 
                       arrowsize=15, arrowstyle='->', width=2, alpha=0.6, ax=ax_graph,
                       connectionstyle='arc3,rad=0.1')

# Draw nodes with distinction - SMALLER SIZES
if functions:
    nx.draw_networkx_nodes(G, pos, nodelist=functions, node_color='#FF6B6B', 
                           node_size=1800, label=f'Functions ({len(functions)})', 
                           ax=ax_graph, alpha=0.95, edgecolors='#C92A2A', linewidths=1.5)

if files:
    nx.draw_networkx_nodes(G, pos, nodelist=files, node_color='#4ECDC4', 
                           node_size=2200, label=f'Files ({len(files)})', 
                           ax=ax_graph, alpha=0.95, edgecolors='#0B7A8C', linewidths=1.5)

# Draw labels - smaller font
labels = {}
for n in G.nodes():
    label = G.nodes[n].get('label', n.split(':')[-1])
    # Truncate long names
    if len(label) > 20:
        label = label[:17] + '...'
    labels[n] = label

nx.draw_networkx_labels(G, pos, labels, font_size=7.5, font_weight='bold', 
                        font_family='monospace', ax=ax_graph)

ax_graph.set_title('Knowledge Graph: PR 54 - Correlation Analysis Pipeline\n(Top 15 Components)', 
                   fontsize=11, fontweight='bold', pad=10, loc='left')
ax_graph.legend(scatterpoints=1, loc='upper left', fontsize=9, framealpha=0.98, 
                edgecolor='black', fancybox=True)
ax_graph.axis('off')

# Add grid reference
ax_graph.text(0.02, 0.02, 'Node Size: Importance | Color: Type | Arrows: Dependencies', 
              transform=ax_graph.transAxes, fontsize=7.5, style='italic', 
              bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

# ============ RIGHT COLUMN: STATISTICS ============
ax_stats = fig.add_subplot(gs[0, 1])
ax_stats.axis('off')

# Calculate statistics
total_functions = len([n for n in G_full.nodes() if 'Function' in str(G_full.nodes[n].get('type', ''))])
total_files = len([n for n in G_full.nodes() if 'File' in str(G_full.nodes[n].get('type', ''))])
avg_degree = sum(dict(G_full.degree()).values()) / len(G_full.nodes()) if len(G_full.nodes()) > 0 else 0
density = nx.density(G_full)

# Edge type distribution
edge_type_dist = {}
for src, tgt, etype in edges:
    if src in pr54_node_ids and tgt in pr54_node_ids:
        edge_type_dist[etype] = edge_type_dist.get(etype, 0) + 1

# Create stats panels
y_pos = 0.95

# Title
ax_stats.text(0.5, y_pos, 'Graph Statistics', fontsize=12, fontweight='bold',
              ha='center', transform=ax_stats.transAxes,
              bbox=dict(boxstyle='round,pad=0.4', facecolor='#4ECDC4', alpha=0.8, 
                       edgecolor='black', linewidth=1.5))
y_pos -= 0.11

# General metrics
ax_stats.text(0.05, y_pos, 'Overall Metrics', fontsize=9.5, fontweight='bold',
              transform=ax_stats.transAxes, color='#C92A2A')
y_pos -= 0.065

metrics = [
    ('Total Nodes', str(len(G_full.nodes()))),
    ('Total Edges', str(len(G_full.edges()))),
    ('Functions', str(total_functions)),
    ('Files', str(total_files)),
    ('Density', f'{density:.4f}'),
    ('Avg Degree', f'{avg_degree:.2f}'),
]

for metric, value in metrics:
    ax_stats.text(0.05, y_pos, f'{metric}:', fontsize=8.5, transform=ax_stats.transAxes,
                  fontweight='bold', color='#333333')
    ax_stats.text(0.65, y_pos, value, fontsize=8.5, transform=ax_stats.transAxes,
                  color='#FF6B6B', fontweight='bold', family='monospace')
    y_pos -= 0.055

y_pos -= 0.02

# Top nodes by degree
ax_stats.text(0.05, y_pos, 'Top Nodes', fontsize=9.5, fontweight='bold',
              transform=ax_stats.transAxes, color='#C92A2A')
y_pos -= 0.065

top_degree_nodes = sorted(G.nodes(), key=lambda n: G.degree(n), reverse=True)[:5]
for i, node in enumerate(top_degree_nodes, 1):
    label = G.nodes[node].get('label', node.split(':')[-1])[:18]
    degree = G.degree(node)
    ax_stats.text(0.05, y_pos, f'{i}. {label}', fontsize=8, transform=ax_stats.transAxes,
                  color='#333333')
    ax_stats.text(0.80, y_pos, f'd={degree}', fontsize=8, transform=ax_stats.transAxes,
                  color='#FF6B6B', fontweight='bold', family='monospace')
    y_pos -= 0.05

y_pos -= 0.02

# Edge types
ax_stats.text(0.05, y_pos, 'Edge Types', fontsize=9.5, fontweight='bold',
              transform=ax_stats.transAxes, color='#C92A2A')
y_pos -= 0.065

for etype, count in sorted(edge_type_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
    ax_stats.text(0.05, y_pos, f'{etype}', fontsize=8, transform=ax_stats.transAxes,
                  color='#333333')
    ax_stats.text(0.75, y_pos, f'{count}', fontsize=8, transform=ax_stats.transAxes,
                  color='#FF6B6B', fontweight='bold', family='monospace')
    y_pos -= 0.05

# Footer
fig.text(0.5, 0.01, 'Knowledge Graph extracted from codebase via AST analysis and call-graph construction', 
         ha='center', fontsize=7.5, style='italic', color='gray')

# Save
os.makedirs('results', exist_ok=True)
plt.savefig('results/subgraph_pr54_final.png', dpi=300, bbox_inches='tight', facecolor='white',
            edgecolor='black', pad_inches=0.2)
print("✓ Saved: results/subgraph_pr54_final.png")
plt.close()
print("Done! Image size reduced while keeping all 15 components.")