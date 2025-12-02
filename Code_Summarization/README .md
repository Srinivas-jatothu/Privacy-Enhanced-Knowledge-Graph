# PEKG Code Summarization Pipeline

## Overview

A 9‑step privacy‑enhanced, KG‑driven code summarization pipeline integrating AST, graph context, provenance, and static analysis.

## Steps

1. **Normalize request** → summarizer_request.json
2. **Resolve KG node** → targets_list.jsonl
3. **Fetch metadata** → target_metadata.jsonl
4. **KG neighborhood** → target_graph_context.jsonl
5. **AST mapping** → target_ast_map.jsonl
6. **AST structural extraction** → ast_summaries.jsonl
7. **Code snippet extract** → code_snippets.jsonl
8. **Payload assembly** → generation_payloads.jsonl
9. **LLM summarization** → generation_results.jsonl
10. **Export** → final_summaries/

## Purpose

Combines KG, AST, provenance, and static analysis to create privacy‑preserving high‑quality code summaries.
