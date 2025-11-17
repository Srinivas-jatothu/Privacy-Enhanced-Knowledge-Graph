import json,os,collections

base="results"

# Load nodes
nodes = json.load(open(os.path.join(base,"nodes.json"), encoding="utf-8"))

# Load symbol table
sym_path=os.path.join(base,"symbol_table.json")
ce1=os.path.join(base,"code_entities_full.json")
ce2=os.path.join(base,"code_entities_enriched.json")

sym = {}
if os.path.exists(sym_path):
    try:
        tmp=json.load(open(sym_path,encoding="utf-8"))
        if isinstance(tmp, dict):
            sym=tmp
    except:
        sym={}

# Load code_entities
ce_list=[]
for p in (ce1, ce2):
    if os.path.exists(p):
        try:
            tmp=json.load(open(p,encoding="utf-8"))
            if isinstance(tmp, list):
                ce_list.extend(tmp)
        except:
            pass

# Find missing functions
missing = [n for n in nodes if n.get("type")=="Function" and not n.get("file")]

print("Functions missing file/lineno:", len(missing))

# Build indexes
ce_by_q={}
ce_short=collections.defaultdict(list)
for ent in ce_list:
    funcs = ent.get("functions") or []
    for f in funcs:
        q = f.get("qualified_name") or f.get("name")
        if q:
            ce_by_q[q]=f
            short=q.split(".")[-1]
            ce_short[short].append((q,f))

sym_short=collections.defaultdict(list)
for q,info in sym.items():
    short=q.split(".")[-1]
    sym_short[short].append((q,info))

# Classification
results=[]
counts=collections.Counter()

for n in missing:
    q = n.get("qualified_name")
    short = q.split(".")[-1] if q else n.get("id","").split(":")[-1]

    # Exact symbol_table match
    if q and q in sym:
        counts["sym_exact"] += 1
        results.append((n["id"], "sym_exact", sym[q].get("file"), sym[q].get("lineno")))
        continue

    # Exact code_entity match
    if q and q in ce_by_q:
        f=ce_by_q[q]
        counts["ce_exact"] += 1
        results.append((n["id"], "ce_exact", f.get("file") or f.get("path"), f.get("lineno")))
        continue

    # Short-name matches
    cand=[]
    cand.extend(sym_short.get(short,[]))
    cand.extend(ce_short.get(short,[]))

    if len(cand)==0:
        counts["no_match"] += 1
        results.append((n["id"], "no_match", None, None))
    elif len(cand)==1:
        q2,info = cand[0]
        fpath = info.get("file") or info.get("path") or info.get("filename") if isinstance(info,dict) else None
        lineno = info.get("lineno") or info.get("line") if isinstance(info,dict) else None
        counts["short_unique"] += 1
        results.append((n["id"], "short_unique", q2, fpath, lineno))
    else:
        counts["ambiguous"] += 1
        results.append((n["id"], "ambiguous", len(cand)))

# Save outputs
out_dir = os.path.join(base, "kg_check")
os.makedirs(out_dir, exist_ok=True)

open(os.path.join(out_dir,"missing_functions.txt"), "w", encoding="utf-8").write(
    "\n".join([r[0] for r in results])
)

open(os.path.join(out_dir,"missing_classification.json"), "w", encoding="utf-8").write(
    json.dumps({"counts":counts, "sample": results[:200]}, indent=2)
)

print("Classification counts:", counts)
print("Wrote results/kg_check/missing_functions.txt and missing_classification.json")
print("Done.")