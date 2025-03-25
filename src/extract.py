# Documentation: https://semanticscholar.readthedocs.io/en/latest/index.html
from semanticscholar import SemanticScholar
import csv
import os
import hashlib
import uuid

# TODO
# make a csv with the ids connected to the keywords
# update it to 100 papers per keywrod
# publication venue

def generate_custom_id():
    unique_str = str(uuid.uuid4())  
    return hashlib.sha1(unique_str.encode()).hexdigest() 

def save_papers_to_csv(papers, filename="papers.csv"):
    fieldnames = ['paperId', 'title', 'abstract', 'year', 'venue', 'url', 'authors', 'keyword', 'citationCount', 'references', 'journal', 'volume', 'isConference']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for paper in papers:
            # Extract lists as a string
            # authors = '; '.join([author.authorId for author in paper.authors]) if hasattr(paper, 'authors') and paper.authors else ''
            author_ids = [author.authorId for author in paper.authors if author.authorId is not None] if hasattr(paper, 'authors') and paper.authors is not None else []
            authors = '; '.join(author_ids) if len(author_ids) > 0 else ''

            # references = '; '.join([reference.paperId for reference in paper.references]) if hasattr(paper, 'references') and paper.references else ''
            reference_ids = [reference.paperId for reference in paper.references if reference.paperId is not None] if hasattr(paper, 'references') and paper.references is not None else []
            references = '; '.join(reference_ids) if len(reference_ids) > 0 else ''

            # Create a row for this paper
            row = {
                'paperId': paper.paperId,
                'title': paper.title,
                'abstract': paper.abstract,
                'year': paper.year,
                'isConference': paper.isConference,
                'venue': paper.venue,
                'url': paper.url,
                'authors': authors,
                'keyword': paper.keyword,
                'references': references,
                'citationCount': paper.citationCount if hasattr(paper, 'citationCount') else 0,
                'journal': paper.journal.name if hasattr(paper, 'journal') and hasattr(paper.journal, 'name') else None,
                'volume': paper.journal.volume if hasattr(paper, 'journal') and hasattr(paper.journal, 'volume') else None
            }
            
            writer.writerow(row)
    
    print(f"Papers saved to {os.path.abspath(filename)}")

def save_authors_to_csv(authors, filename="authors.csv"):
    fieldnames = ['authorId', 'name', 'affiliations', 'homepage', 'paperCount', 'citationCount', 'hIndex', 'papers']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for author in authors:
            paper_ids = [paper.paperId for paper in author.papers if paper.paperId is not None] if hasattr(author, 'papers') and author.papers is not None else []
            papers = '; '.join(paper_ids) if len(paper_ids) > 0 else ''
            
            affiliations = '; '.join(author.affiliations) if hasattr(author, 'affiliations') and author.affiliations is not None else ''
            
            row = {
                'authorId': author.authorId,
                'name': author.name,
                'affiliations': affiliations,
                'homepage': author.homepage if hasattr(author, 'homepage') else '',
                'paperCount': author.paperCount if hasattr(author, 'paperCount') else 0,
                'citationCount': author.citationCount if hasattr(author, 'citationCount') else 0,
                'hIndex': author.hIndex if hasattr(author, 'hIndex') else 0,
                'papers': papers
            }
            
            writer.writerow(row)
    
    print(f"Authors saved to {os.path.abspath(filename)}")

def save_journals_to_csv(journals, filename="journals.csv"):
    fieldnames = ['journalId', 'name', 'pages', 'volume']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for journal in journals:
            row = {
                'journalId': generate_custom_id(),
                'name': journal.name if hasattr(journal, 'name') else '',
                'pages': journal.pages if hasattr(journal, 'pages') else '',
                'volume': journal.volume if hasattr(journal, 'volume') else ''
            }
            
            writer.writerow(row)
    
    print(f"Journals saved to {os.path.abspath(filename)}")

def save_keywords_to_csv(keywords, filename="keywords.csv"):
    fieldnames = ['keyword']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for keyword in keywords:
            row = {
                'keyword': keyword,
            }
            
            writer.writerow(row)
    
    print(f"Keywords saved to {os.path.abspath(filename)}")

def search_and_save_papers(keywords, limit=100):
    sch = SemanticScholar()
    
    all_papers = []
    seen_paper_ids = {}

    all_conferences = []
    seen_conference_ids = {}

    all_journals = []
    seen_journals = {}

    all_authors = []
    seen_author_ids = {}

    # Search papers for each keyword
    for keyword in keywords:
        print(f"Searching for '{keyword}'...")
        
        # Search for Journal Articles
        papers = sch.search_paper(query=keyword, limit=limit, publication_types = ['JournalArticle'])
        for paper in papers.items:
            # Append additional fields
            paper.keyword = keyword
            paper.isConference = False

            # Paper Information
            if paper.paperId not in seen_paper_ids:
                all_papers.append(paper)
                seen_paper_ids[paper.paperId] = True

            # Author Information
            if hasattr(paper, 'authors') and paper.authors is not None:
                for author in paper.authors:
                    if author.authorId not in seen_author_ids:
                        all_authors.append(author)
                        seen_author_ids[author.authorId] = True

            # Journal Information
            journal_key = (
                paper.journal.name if hasattr(paper.journal, 'name') else None,
                paper.journal.volume if hasattr(paper.journal, 'volume') else None
            )

            if journal_key not in seen_journals:
                all_journals.append(paper.journal)
                seen_paper_ids[journal_key] = True


        # Search for Conference Papers
        papers = sch.search_paper(query=keyword, limit=limit, publication_types = ['Conference'])
        for paper in papers.items:
            # Append additional fields
            paper.keyword = keyword
            paper.isConference = True

            # Paper Information
            if paper.paperId not in seen_paper_ids:
                all_papers.append(paper)
                seen_paper_ids[paper.paperId] = True

            # Author Information
            if hasattr(paper, 'authors') and paper.authors is not None:
                for author in paper.authors:
                    if author.authorId not in seen_author_ids:
                        all_authors.append(author)
                        seen_author_ids[author.authorId] = True


    print(f"Total unique papers found: {len(all_papers)}")
    save_papers_to_csv(all_papers)
    save_authors_to_csv(all_authors)
    save_journals_to_csv(all_journals)
    save_keywords_to_csv(keywords)
    
    return all_papers

# Search 100 papers from each keyword
keywords = ['data management', 'indexing']
# keywords = ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying', 'artificial intelligence', 'machine learning', 'ethics', 'semantic data', 'data warehouse', 'process mining', 'decision support']

papers = search_and_save_papers(keywords, limit=100)
