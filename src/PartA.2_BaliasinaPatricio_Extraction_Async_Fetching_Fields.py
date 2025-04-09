import asyncio
import csv
import os
import copy

#from collections import defaultdict
from semanticscholar import AsyncSemanticScholar 
from semanticscholar.Paper import Paper as S2Paper

# -------------------------------------------
# A KNOWN KEYWORDS LIST (from old script)  
# -------------------------------------------
TITLE_KEYWORDS = [
    'data management', 'indexing', 'data modeling', 'big data', 
    'data processing', 'data storage', 'data querying', 
    'artificial intelligence', 'machine learning', 'ethics', 
    'semantic data', 'data warehouse', 'process mining', 
    'decision support'
]


# -------------------------------------------
# LOAD PAPER IDs + References + old Keywords 
# -------------------------------------------
def load_papers_and_references_from_csv(
    filename, 
    paper_id_col="paperId", 
    references_col="references",
    keyword_col="keyword"
):
    """
    Now also returns a dict mapping paperId -> old_csv_keyword (if any).
    """
    paper_ids = []
    references_map = {}
    old_keywords_map = {}

    with open(filename, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            pid = row.get(paper_id_col, "").strip()
            if not pid:
                continue

            refs_str = row.get(references_col, "").strip()
            refs_list = []
            if refs_str:
                refs_list = [r.strip() for r in refs_str.split(";") if r.strip()]

            # Grab the original CSV "keyword" (if present)
            old_kw = row.get(keyword_col, "").strip()

            paper_ids.append(pid)
            references_map[pid] = refs_list
            old_keywords_map[pid] = old_kw

    return paper_ids, references_map, old_keywords_map


# -------------------------------------------
# ASYNC BATCH FETCH, but exclude references
# -------------------------------------------
async def fetch_papers_batch(sch, paper_ids_batch, fields, return_not_found=False):
    return await sch.get_papers(
        paper_ids=paper_ids_batch,
        fields=fields,
        return_not_found=return_not_found
    )

async def fetch_all_papers_async(paper_ids, fields=None, concurrency=5, chunk_size=500, return_not_found=False):
    """
    We will NOT ask for "references" in the fields,
    because we want to keep the references from the CSV only.
    """
    if fields is None:
        fields = [
            "title",
            "abstract",
            "year",
            "venue",
            "authors",
            "citationCount",
            "publicationTypes",
            "externalIds",
            "fieldsOfStudy",
            "journal",
            "url",
        ]

    sch = AsyncSemanticScholar()

    all_papers = []
    not_found_ids = []

    sem = asyncio.Semaphore(concurrency)

    async def worker(batch):
        async with sem:
            return await fetch_papers_batch(sch, batch, fields, return_not_found=return_not_found)

    tasks = []
    for i in range(0, len(paper_ids), chunk_size):
        chunk = paper_ids[i : i + chunk_size]
        task = asyncio.create_task(worker(chunk))
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if return_not_found:
        for res in results:
            if isinstance(res, Exception):
                print("[Paper Batch Error]", res)
                continue
            papers_list, nf_list = res
            all_papers.extend(papers_list)
            not_found_ids.extend(nf_list)
        return all_papers, not_found_ids
    else:
        for res in results:
            if isinstance(res, Exception):
                print("[Paper Batch Error]", res)
                continue
            all_papers.extend(res)
        return all_papers


# -------------------------------------------
# AUTHOR BATCH (unchanged)
# -------------------------------------------
async def fetch_authors_batch(sch, author_ids_batch, fields, return_not_found=False):
    return await sch.get_authors(
        author_ids=author_ids_batch,
        fields=fields,
        return_not_found=return_not_found
    )

async def fetch_all_authors_async(author_ids, fields=None, concurrency=10, chunk_size=100, return_not_found=False):
    if fields is None:
        fields = [
            "name",
            "affiliations",
            "homepage",
            "paperCount",
            "citationCount",
            "hIndex",
            "papers"
        ]

    sch = AsyncSemanticScholar()
    all_authors = []
    not_found_ids = []

    sem = asyncio.Semaphore(concurrency)

    async def worker(batch):
        async with sem:
            return await fetch_authors_batch(sch, batch, fields, return_not_found=return_not_found)

    tasks = []
    for i in range(0, len(author_ids), chunk_size):
        chunk = author_ids[i : i + chunk_size]
        task = asyncio.create_task(worker(chunk))
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if return_not_found:
        for res in results:
            if isinstance(res, Exception):
                print("[Author Batch Error]", res)
                continue
            authors_list, nf_list = res
            all_authors.extend(authors_list)
            not_found_ids.extend(nf_list)
        return all_authors, not_found_ids
    else:
        for res in results:
            if isinstance(res, Exception):
                print("[Author Batch Error]", res)
                continue
            all_authors.extend(res)
        return all_authors


# -------------------------------------------
# CSV WRITERS (modified to use _myKeywords)
# -------------------------------------------
def save_papers_to_csv(papers, filename="papers.csv"):
    fieldnames = [
        "paperId",
        "title",
        "abstract",
        "doi",
        "url",
        "citationCount",
        "venue",
        "venueType",
        "year",
        "fieldsOfStudy",
        "pages",
        "references",
        "authors"
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for p in papers:
            pid = p.paperId
            title = p.title
            abstract = p.abstract
            url = p.url
            year = p.year
            venue = p.venue
            citation_count = getattr(p, "citationCount", 0)

            # doi from externalIds
            doi = ""
            if getattr(p, "externalIds", None):
                doi = p.externalIds.get("DOI") or p.externalIds.get("doi") or ""

            # Use _myKeywords instead of p.fieldsOfStudy
            fos = ""
            if getattr(p, "_myKeywords", None):
                fos = "; ".join(sorted(p._myKeywords))

            # references
            ref_str = ""
            if getattr(p, "references", None):
                ref_str = "; ".join(r.paperId for r in p.references if r.paperId)

            # authors
            authors_str = ""
            if getattr(p, "authors", None):
                authors_str = "; ".join(a.authorId or "" for a in p.authors if a.authorId)

            # pages from p.journal if available
            pages = ""
            if getattr(p, "journal", None) and p.journal:
                pages = getattr(p.journal, "pages", "") or ""

            # venueType
            venue_type = "Unknown"
            if getattr(p, "publicationTypes", None):
                if "JournalArticle" in p.publicationTypes:
                    venue_type = "Journal"
                elif "Conference" in p.publicationTypes:
                    venue_type = "Conference"

            row = {
                "paperId": pid,
                "title": title,
                "abstract": abstract,
                "doi": doi,
                "url": url,
                "citationCount": citation_count,
                "venue": venue,
                "venueType": venue_type,
                "year": year,
                "fieldsOfStudy": fos,
                "pages": pages,
                "references": ref_str,
                "authors": authors_str
            }
            writer.writerow(row)

    print(f"[save_papers_to_csv] Wrote {len(papers)} papers to {os.path.abspath(filename)}")


def save_authors_to_csv(authors, filename="authors.csv"):
    """
    Writes out only the columns: authorId, name, affiliations
    """
    fieldnames = [
        "authorId",
        "name",
        "affiliations",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for a in authors:
            row = {
                "authorId": a.authorId,
                "name": a.name or "",
                "affiliations": "",
            }
            if getattr(a, "affiliations", None):
                row["affiliations"] = "; ".join(a.affiliations)

            writer.writerow(row)

    print(f"[save_authors_to_csv] Wrote {len(authors)} authors to {os.path.abspath(filename)}")


def save_venues_to_csv(venues, filename="venues.csv"):
    fieldnames = ["venueType", "name", "volume", "pages"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for v in venues:
            writer.writerow(v)
    print(f"[save_venues_to_csv] Wrote {len(venues)} venues to {os.path.abspath(filename)}")


def save_paper_keywords_csv(papers, filename="paper_keywords.csv"):
    """
    Writes out each paper's final combined keywords 
    from _myKeywords (rather than fieldsOfStudy).
    """
    import csv
    fieldnames = ["paperId", "keyword"]
    count = 0

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for p in papers:
            pid = p.paperId
            if getattr(p, "_myKeywords", None):
                for kw in p._myKeywords:
                    row = {
                        "paperId": pid,
                        "keyword": kw
                    }
                    writer.writerow(row)
                    count += 1

    print(f"[save_paper_keywords_csv] Wrote {count} (paperId, keyword) pairs to {filename}")


# -------------------------------------------
# MAIN
# -------------------------------------------
async def main():
    """
    1) Load paper IDs + references from step1_papers.csv
       + also load any old CSV keyword for each paper.
    2) Fetch everything but references from the S2 API
    3) Overwrite the paper's references with those from step1
    4) Create combined keywords for each paper (old CSV + S2 fieldsOfStudy + title-based)
    5) Inherit the parent's keywords into each of its reference papers
    6) Extract author info & venue info
    7) Fetch authors in parallel
    8) Write out final CSVs
    """

    input_csv = "papers_combined.csv"
    paper_ids, references_map, old_keywords_map = load_papers_and_references_from_csv(
        filename=input_csv, 
        paper_id_col="paperId", 
        references_col="references",
        keyword_col="keyword"  
    )
    print(f"[main] Loaded {len(paper_ids)} paper IDs from {input_csv}.")

    # 2) fetch from API
    all_papers, not_found = await fetch_all_papers_async(
        paper_ids=paper_ids,
        fields=None,  
        concurrency=10,
        chunk_size=500,
        return_not_found=True
    )
    print(f"[main] Fetched {len(all_papers)} papers. Not found: {len(not_found)}")

    # 3) Rebuild with references from CSV
    paper_dict = {}
    for p in all_papers:
        paper_dict[p.paperId] = p

    for pid, old_paper in paper_dict.items():
        old_refs = references_map.get(pid, [])
        
        data_copy = copy.deepcopy(old_paper.raw_data)
        
        data_copy["references"] = []
        for rid in old_refs:
            data_copy["references"].append({"paperId": rid})
    
        new_paper = S2Paper(data_copy)

        # 4) Create combined keywords
        combined_keywords = set()

        # (a) Old CSV keyword
        old_kw = old_keywords_map.get(pid, "").strip()
        if old_kw:
            combined_keywords.add(old_kw)

        # (b) S2 fieldsOfStudy
        if getattr(new_paper, "fieldsOfStudy", None):
            for fs in new_paper.fieldsOfStudy:
                if fs:
                    combined_keywords.add(fs)

        # (c) Check paper title for known keywords
        title_lower = (new_paper.title or "").lower()
        for tk in TITLE_KEYWORDS:
            if tk.lower() in title_lower:
                combined_keywords.add(tk)

        # Store the final set in _myKeywords
        setattr(new_paper, "_myKeywords", combined_keywords)

        paper_dict[pid] = new_paper

    # We now have "final" papers, but before we finalize them,
    # 5) Inherit parent's keywords into each reference
    #    (only a single pass for direct references)
    for pid, paper_obj in paper_dict.items():
        if getattr(paper_obj, "references", None):
            for ref in paper_obj.references:
                rid = ref.paperId
                if rid in paper_dict:
                    # union the parent's keywords into the reference
                    paper_dict[rid]._myKeywords |= paper_obj._myKeywords

    final_papers = list(paper_dict.values())

    # 6) Extract author info & venue info
    author_ids_set = set()
    venues_set = set()

    for p in final_papers:
        if getattr(p, "authors", None):
            for a in p.authors:
                if a.authorId:
                    author_ids_set.add(a.authorId)

        venue_type = "Unknown"
        if getattr(p, "publicationTypes", None):
            pub_types = p.publicationTypes or []
            if "JournalArticle" in pub_types:
                venue_type = "Journal"
            elif "Conference" in pub_types:
                venue_type = "Conference"
        
        name = p.venue or ""
        volume = ""
        pages = ""
        
        if venue_type == "Journal":
            # quick check to see if it actually looks like a conference
            conf_words = ["conference", "workshop", "proceedings", "symposium"]
            if any(cw in name.lower() for cw in conf_words):
                venue_type = "Conference"
        
        if venue_type == "Journal":
            if getattr(p, "journal", None) and p.journal:
                if getattr(p.journal, "name", None):
                    name = p.journal.name
                volume = getattr(p.journal, "volume", "") or ""
                pages = getattr(p.journal, "pages", "") or ""
        
        if name:
            vdict = {
                "venueType": venue_type,
                "name": name,
                "volume": volume,
                "pages": pages
            }
            venues_set.add(tuple(vdict.items()))


    unique_venues = [dict(t) for t in venues_set]
    print(f"[main] Found {len(author_ids_set)} unique authors, {len(unique_venues)} unique venues.")

    # 7) Fetch authors in parallel
    author_ids = list(author_ids_set)
    all_authors, not_found_authors = await fetch_all_authors_async(
        author_ids,
        fields=None,
        concurrency=10,
        chunk_size=100,
        return_not_found=True
    )
    print(f"[main] Fetched {len(all_authors)} authors. Not found = {len(not_found_authors)}")

    # 8) Write final CSVs
    save_papers_to_csv(final_papers, "papers.csv")
    save_authors_to_csv(all_authors, "authors.csv")
    save_venues_to_csv(unique_venues, "venues.csv")
    save_paper_keywords_csv(final_papers, "paper_keywords.csv")

    print("[main] Done.")

if __name__ == "__main__":
    asyncio.run(main())