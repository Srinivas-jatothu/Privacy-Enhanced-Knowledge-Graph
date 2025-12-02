import os
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

AUTO_SUMMARIES = Path("results/auto_summaries.jsonl")
OUT_TEMPLATE = Path("results/human_summaries_template.jsonl")

def load_jsonl(path: Path):
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except:
                logging.warning(f"Skipping invalid line in {path}")
    return items

def main():
    logging.info("Starting generate_human_template.py")

    if not AUTO_SUMMARIES.exists():
        logging.error(f"Missing file: {AUTO_SUMMARIES}")
        return

    autos = load_jsonl(AUTO_SUMMARIES)
    logging.debug(f"Loaded {len(autos)} auto summaries")

    OUT_TEMPLATE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_TEMPLATE, "w", encoding="utf-8") as out:
        for item in autos:
            node_id = item.get("node_id")
            if not node_id:
                continue
            out.write(json.dumps({"node_id": node_id, "reference": ""}) + "\n")

    logging.info(f"Wrote human-summaries template to: {OUT_TEMPLATE}")

if __name__ == "__main__":
    main()
