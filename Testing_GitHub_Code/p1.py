# import json, sys
# from jsonschema import validate, Draft7Validator, ValidationError

# schema = json.load(open("results/kg_schema.json", encoding="utf-8"))
# nodes = json.load(open("results/nodes.json", encoding="utf-8"))
# edges = json.load(open("results/edges.json", encoding="utf-8"))

# # quick structural checks (node-level)
# node_errors = []
# for n in nodes:
#     try:
#         validate(instance=n, schema=schema["definitions"]["Node"])
#     except ValidationError as e:
#         node_errors.append((n.get("id"), str(e.message)))

# edge_errors = []
# for e in edges:
#     try:
#         validate(instance=e, schema=schema["definitions"]["Edge"])
#     except ValidationError as ex:
#         edge_errors.append((e.get("source"), e.get("target"), str(ex.message)))

# print("Nodes validated:", len(nodes), "errors:", len(node_errors))
# if node_errors:
#     print("Sample node errors:", node_errors[:10])
# print("Edges validated:", len(edges), "errors:", len(edge_errors))
# if edge_errors:
#     print("Sample edge errors:", edge_errors[:10])
# p1_validate.py
import json
from jsonschema import Draft7Validator, RefResolver, ValidationError

schema = json.load(open("results/kg_schema.json", encoding="utf-8"))
nodes = json.load(open("results/nodes.json", encoding="utf-8"))
edges = json.load(open("results/edges.json", encoding="utf-8"))

# create a resolver that knows about the full schema document
resolver = RefResolver.from_schema(schema)

# Create validators for the Node and Edge definitions, providing the resolver
node_validator = Draft7Validator(schema["definitions"]["Node"], resolver=resolver)
edge_validator = Draft7Validator(schema["definitions"]["Edge"], resolver=resolver)

node_errors = []
for n in nodes:
    for err in node_validator.iter_errors(n):
        node_errors.append((n.get("id"), err.message))

edge_errors = []
for e in edges:
    for err in edge_validator.iter_errors(e):
        edge_errors.append((e.get("source"), e.get("target"), err.message))

print("Nodes validated:", len(nodes), "errors:", len(node_errors))
if node_errors:
    print("Sample node errors (up to 10):")
    for item in node_errors[:10]:
        print(item)

print("Edges validated:", len(edges), "errors:", len(edge_errors))
if edge_errors:
    print("Sample edge errors (up to 10):")
    for item in edge_errors[:10]:
        print(item)
