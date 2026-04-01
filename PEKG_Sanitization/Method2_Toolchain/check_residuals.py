# # # import csv
# # # from collections import defaultdict

# # # counts = defaultdict(list)
# # # with open(r'results\final_privacy_scan.csv', encoding='utf-8') as f:
# # #     reader = csv.DictReader(f)
# # #     for row in reader:
# # #         key = f"{row['pattern_type']} | {row['field']}"
# # #         counts[key].append(row['matched_text'][:50])

# # # for k, samples in sorted(counts.items(), key=lambda x: -len(x[1])):
# # #     print(f"{len(samples):>6}  {k}")
# # #     print(f"         sample: {samples[0]}")
# # #     print()


# # import json
# # with open(r'results\kg_after_literal_sanitization.json', encoding='utf-8') as f:
# #     data = json.load(f)

# # # Handle both list and dict
# # nodes = list(data.values()) if isinstance(data, dict) else data

# # for node in nodes:
# #     if node.get('type') == 'PullRequest':
# #         attrs = node.get('attrs', {})
# #         if 'commits' in attrs and attrs['commits']:
# #             print(json.dumps(attrs['commits'][0], indent=2))
# #             break



# import csv
# from collections import defaultdict

# counts = defaultdict(list)
# with open(r'results\final_privacy_scan.csv', encoding='utf-8') as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         key = f"{row['field']}"
#         counts[key].append(f"{row['matched_text'][:40]} | node: {row['node_id'][:40]}")

# for field, samples in sorted(counts.items(), key=lambda x: -len(x[1])):
#     print(f"{len(samples):>6}  field: {field}")
#     for s in samples[:3]:
#         print(f"         {s}")
#     print()


import csv
with open(r'results\identifier_registry.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if 'correlation_check' in row['original']:
            print(f"{row['replacement']:<25} ← {row['original']}")