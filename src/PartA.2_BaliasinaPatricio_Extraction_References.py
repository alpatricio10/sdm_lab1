# paper_reference_gatherer.py

import csv
import os
import time
import random
from typing import List

import pandas as pd
from semanticscholar import SemanticScholar


# ------------------------------------------------
# STEP 1: Gather main papers (by keywords + venue)
# ------------------------------------------------

def run_step1_initial(
    keywords: List[str],
    limit: int = 20,
    output_csv: str = "step1_papers.csv"
):
    sch = SemanticScholar()
    publication_types = ["JournalArticle", "Conference"]
    rows = []

    for kw in keywords:
        for pub_type in publication_types:
            print(f"[STEP1] Searching for '{kw}' in {pub_type}, limit={limit}...")
            results = sch.search_paper(
                query=kw,
                limit=limit,
                publication_types=[pub_type],
                fields=["paperId"]
            )
            print(f"  Found {results.total} total. We'll process {len(results.items)} items...")

            for paper in results.items:
                pid = paper.paperId
                if not pid:
                    continue
                print(f"  -> Paper {pid}, retrieving ...")

                references_list = []
                try:
                    ref_result = sch.get_paper_references(paper_id=pid)
                    if ref_result.items:
                        for ref_obj in ref_result.items:
                            if ref_obj.paper and ref_obj.paper.paperId:
                                references_list.append(ref_obj.paper.paperId)
                except Exception as e:
                    print(f"    [STEP1] Error retrieving references for {pid}: {e}")

                row = {
                    "paperId": pid,
                    "references": "; ".join(references_list),
                    "keyword": kw,
                    "venueType": pub_type
                }
                rows.append(row)

                time.sleep(random.uniform(0.2, 0.8))

    fieldnames = ["paperId", "references", "keyword", "venueType"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[STEP1] Wrote {len(rows)} rows to {os.path.abspath(output_csv)}")
    return rows


# --------------------------------------------
# STEP 2: Gather second-level references
# --------------------------------------------

def run_step2_expanded(
    input_csv: str = "step1_papers.csv",
    output_csv: str = "step2_papers.csv"
):
    sch = SemanticScholar()

    ref_ids = set()
    ref_ids_list = []

    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            refs_str = row.get("references", "")
            if refs_str.strip():
                splitted = [x.strip() for x in refs_str.split(";") if x.strip()]
                for r in splitted:
                    ref_ids.add(r)
                    ref_ids_list.append(r)

    print(f"[STEP2] Total references (with duplicates): {len(ref_ids_list)}")
    print(f"[STEP2] Found {len(ref_ids)} unique reference IDs from {input_csv}.")

    rows = []
    for i, rid in enumerate(ref_ids, 1):
        print(f"[{i}] -> Gathering references for {rid} ")
        references_list = []
        try:
            ref_result = sch.get_paper_references(paper_id=rid)
            if ref_result.items:
                for ref_obj in ref_result.items:
                    if ref_obj.paper and ref_obj.paper.paperId:
                        references_list.append(ref_obj.paper.paperId)
        except Exception as e:
            print(f"[STEP2] Error retrieving references for {rid}: {e}")

        row = {
            "paperId": rid,
            "references": "; ".join(references_list),
            "keyword": "null",
            "venueType": "null"
        }
        rows.append(row)

        # Optional: Uncomment if rate-limiting needed
        # time.sleep(random.uniform(0.2, 0.8))

    fieldnames = ["paperId", "references", "keyword", "venueType"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[STEP2] Wrote {len(rows)} rows to {os.path.abspath(output_csv)}")
    return rows


def main():
    # Step 1
    step1_rows = run_step1_initial(
        keywords=[
            'data management', 'indexing', 'data modeling', 'big data',
            'data processing', 'data storage', 'data querying',
            'artificial intelligence', 'machine learning', 'ethics',
            'semantic data', 'data warehouse', 'process mining', 'decision support'
        ],
        limit=20,
        output_csv="step1_papers.csv"
    )

    # Step 2
    step2_rows = run_step2_expanded(
        input_csv="step1_papers.csv",
        output_csv="step2_papers.csv"
    )

    # Combine both into one CSV
    df1 = pd.read_csv("step1_papers.csv")
    df1 = df1[df1["references"].notna() & ~df1["references"].str.strip().eq("")]
    df2 = pd.read_csv("step2_papers.csv")
    combined_df = pd.concat([df1, df2], ignore_index=True)
    combined_df.to_csv("papers_combined.csv", index=False)

    print("[FINAL] Combined data saved to papers_combined.csv")


if __name__ == "__main__":
    main()
