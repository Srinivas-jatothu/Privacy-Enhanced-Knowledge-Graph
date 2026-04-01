# # # # import csv

# # # # with open(r'results\sensitive_item_registry.csv', encoding='utf-8') as f:
# # # #     reader = csv.DictReader(f)
# # # #     for row in reader:
# # # #         if row['tool'] == 'presidio' and row['replacement'].startswith('CONTRIBUTOR'):
# # # #             print(f"{row['replacement']:<20} | {row['field']:<30} | {row['original']}")



# # # import csv

# # # with open(r'results\sensitive_item_registry.csv', encoding='utf-8') as f:
# # #     reader = csv.DictReader(f)
# # #     seen = set()
# # #     for row in reader:
# # #         if row['tool'] == 'contributor-registry':
# # #             key = f"{row['field']} | {row['original']}"
# # #             if key not in seen:
# # #                 seen.add(key)
# # #                 print(f"{row['replacement']:<20} | {row['field']:<30} | {row['original']}")



# # # save as check_messages.py
# # import csv

# # with open(r'results\sensitive_item_registry.csv', encoding='utf-8') as f:
# #     reader = csv.DictReader(f)
# #     seen = set()
# #     for row in reader:
# #         if row['tool'] == 'presidio':
# #             key = f"{row['field']} | {row['original']}"
# #             if key not in seen:
# #                 seen.add(key)
# #                 print(f"{row['replacement']:<20} | {row['field']:<30} | {row['original']}")




# import json
# with open(r'results\kg_after_literal_sanitization.json', encoding='utf-8') as f:
#     data = json.load(f)
# nodes = list(data.values()) if isinstance(data, dict) else data
# for node in nodes:
#     if node.get('type') == 'Function':
#         print('id          :', node.get('id'))
#         print('label       :', node.get('label'))
#         print('qualified   :', node.get('qualified_name'))
#         print('signature   :', node.get('signature'))
#         print('docstring   :', str(node.get('docstring',''))[:80])
#         print('attrs.module:', node.get('attrs',{}).get('module'))
#         break



import json
with open(r'results\kg_after_identifier_sanitization.json', encoding='utf-8') as f:
    data = json.load(f)
nodes = list(data.values()) if isinstance(data, dict) else data
for node in nodes:
    if node.get('type') == 'Function':
        print(json.dumps(node, indent=2))
        break