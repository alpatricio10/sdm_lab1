import pandas as pd
import numpy as np
import random
import uuid
import re
from datetime import datetime

# Function to generate random emails based on author names
def generate_email(name):
    name = re.sub(r'[^\w\s]', '', name).lower()
    domains = ['university.edu', 'research.org', 'institute.net', 'lab.com', 'science.io']
    username = name.replace(' ', '.').lower()
    return f"{username}@{random.choice(domains)}"

# Function to generate random DOIs
def generate_doi(paper_id):
    return f"10.{random.randint(1000, 9999)}/{paper_id}"

# Function to generate random ISBN
def generate_isbn():
    return f"978-{random.randint(0, 9)}-{random.randint(10000, 99999)}-{random.randint(100, 999)}-{random.randint(0, 9)}"

# Function to generate random city
def generate_city():
    cities = ["New York", "London", "Tokyo", "Paris", "Berlin", "Beijing", "Sydney", "Toronto", 
              "Singapore", "Zurich", "Amsterdam", "San Francisco", "Seoul", "Montreal", "Barcelona"]
    return random.choice(cities)

# Function to generate keywords based on abstract
def generate_keywords(abstract):
    if pd.isna(abstract) or abstract == "":
        return ["research", "science", "analysis"]
    
    # Simple keyword extraction (in a real scenario, you'd use NLP techniques)
    common_words = ["the", "and", "of", "to", "a", "in", "for", "is", "on", "that", "this", "with"]
    words = abstract.lower().split()
    words = [word for word in words if word not in common_words and len(word) > 4]
    
    # Get unique words and take up to 5 most common
    if len(words) > 0:
        unique_words = list(set(words))
        return random.sample(unique_words, min(5, len(unique_words)))
    else:
        return ["research", "science", "analysis"]

# Function to generate random organization name
def generate_organization():
    prefixes = ["International", "European", "American", "Asian", "World", "Global"]
    subjects = ["Computer Science", "AI", "Machine Learning", "Data Science", "Bioinformatics", 
                "Computational Biology", "Software Engineering", "Information Systems"]
    types = ["Association", "Society", "Institute", "Consortium", "Federation"]
    
    return f"{random.choice(prefixes)} {random.choice(subjects)} {random.choice(types)}"

# Load the data
papers = pd.read_csv('papers.csv')
authors = pd.read_csv('authors.csv')
journals = pd.read_csv('journals.csv')

# Create nodes and relationships dataframes
# 1. Author nodes
author_nodes = pd.DataFrame({
    'authorId': authors['authorId'],
    'name': authors['name'],
    'email': authors['name'].apply(generate_email)
})
author_nodes.to_csv('author_nodes.csv', index=False)

# 2. Paper nodes
paper_nodes = pd.DataFrame({
    'paperId': papers['paperId'],
    'title': papers['title'],
    'abstract': papers['abstract'],
    'pages': np.random.randint(1, 30, size=len(papers)), 
    'doi': papers['paperId'].apply(generate_doi),
    'url': papers['url'],
    'citationCount': papers['citationCount']
})
paper_nodes.to_csv('paper_nodes.csv', index=False)

# 3. Journal nodes (from journals.csv)
journal_nodes = pd.DataFrame({
    'name': journals['name']
})
journal_nodes.to_csv('journal_nodes.csv', index=False)

# 3. Volume nodes (from journals.csv)
# Create volume during relationship loop
volume_nodes = pd.DataFrame({
    'volumeNumber': journals['volume'],
    'year': 2020
})
volume_nodes.to_csv('volume_nodes.csv', index=False)

# 4. Keyword nodes (extracted from abstracts)
all_keywords = []
paper_keyword_rels = []

for index, row in papers.iterrows():
    paper_id = row['paperId']
    keywords = generate_keywords(row['abstract'])
    
    for keyword in keywords:
        all_keywords.append(keyword)
        paper_keyword_rels.append({
            'paperId': paper_id,
            'keyword': keyword
        })

# Create unique keywords
unique_keywords = list(set(all_keywords))
keyword_nodes = pd.DataFrame({
    'name': unique_keywords
})
keyword_nodes.to_csv('keyword_nodes.csv', index=False)

# Create paper-keyword relationships
paper_keyword_df = pd.DataFrame(paper_keyword_rels)
paper_keyword_df.to_csv('paper_keyword_rels.csv', index=False)

# 5. Event nodes (based on venue and year from papers)
events = []
for index, row in papers.iterrows():
    events.append({
        'eventId': f"event_{row['venue']}_{row['year']}",
        'year': row['year'],
        'venue': row['venue'],
        'city': generate_city()
    })

event_nodes = pd.DataFrame(events).drop_duplicates(subset=['eventId'])
event_nodes.to_csv('event_nodes.csv', index=False)

# 6. Organization nodes (parent organizations of events)
organizations = {}
for event in events:
    org_name = generate_organization()
    organizations[event['eventId']] = org_name

org_nodes = pd.DataFrame({'name': list(set(organizations.values()))})
org_nodes.to_csv('organization_nodes.csv', index=False)

# Create relationships

# Volume part of Journal
paper_events = []
for index, row in papers.iterrows():
    event_id = f"event_{row['venue']}_{row['year']}"
    paper_events.append({
        'volumeId': row['paperId'],
        'paperId': event_id
    })

volume_part_of = pd.DataFrame(paper_events)
volume_part_of.to_csv('volume_part_of.csv', index=False)

# 1. Author writes Paper
author_papers = []
# Parse the authors field from papers.csv
for index, row in papers.iterrows():
    paper_id = row['paperId']
    if isinstance(row['authors'], str):
        author_list = row['authors'].split(',')
        # Make one author the corresponding author
        corresponding_author_idx = random.randint(0, len(author_list) - 1)
        
        for i, author_name in enumerate(author_list):
            author_name = author_name.strip()
            # Find the authorId that matches this name
            author_matches = authors[authors['name'].str.contains(author_name, na=False)]
            
            if len(author_matches) > 0:
                author_id = author_matches.iloc[0]['authorId']
                author_papers.append({
                    'authorId': author_id,
                    'paperId': paper_id,
                    'corresponding_author': (i == corresponding_author_idx)
                })
            else:
                # Generate a new authorId if not found
                new_author_id = f"author_{uuid.uuid4()}"
                author_papers.append({
                    'authorId': new_author_id,
                    'paperId': paper_id,
                    'corresponding_author': (i == corresponding_author_idx)
                })

author_writes_paper = pd.DataFrame(author_papers)
author_writes_paper.to_csv('author_writes_paper.csv', index=False)

# 2. Author reviews Paper (random assignment)
reviewer_assignments = []
for paper_id in papers['paperId']:
    # Select 2-4 random reviewers for each paper
    reviewer_count = random.randint(2, 4)
    # Get reviewers different from authors of this paper
    paper_authors = author_writes_paper[author_writes_paper['paperId'] == paper_id]['authorId'].tolist()
    potential_reviewers = authors[~authors['authorId'].isin(paper_authors)]['authorId'].sample(min(reviewer_count, len(authors) - len(paper_authors)))
    
    for reviewer_id in potential_reviewers:
        reviewer_assignments.append({
            'authorId': reviewer_id,
            'paperId': paper_id
        })

author_reviews_paper = pd.DataFrame(reviewer_assignments)
author_reviews_paper.to_csv('author_reviews_paper.csv', index=False)

# 3. Paper published in Journal
paper_journal = []
for index, row in papers.iterrows():
    # Randomly assign papers to journals
    if len(journals) > 0:
        journal = journals.sample(1).iloc[0]
        paper_journal.append({
            'paperId': row['paperId'],
            'journalName': journal['name'],
            'volume': journal['volume'],
            'year': row['year'],
            'isbn': generate_isbn()
        })

paper_published_in = pd.DataFrame(paper_journal)
paper_published_in.to_csv('paper_published_in.csv', index=False)

# 4. Paper cites Paper
paper_citations = []
for index, row in papers.iterrows():
    paper_id = row['paperId']
    # Check if references column exists and has data
    if 'references' in papers.columns and isinstance(row['references'], str):
        references = row['references'].split(',')
        for ref in references:
            ref = ref.strip()
            # Check if referenced paper exists in our dataset
            if ref in papers['paperId'].values:
                paper_citations.append({
                    'sourcePaperId': paper_id,
                    'targetPaperId': ref
                })

paper_cites_paper = pd.DataFrame(paper_citations)
paper_cites_paper.to_csv('paper_cites_paper.csv', index=False)

# 5. Paper presented in Event
paper_events = []
for index, row in papers.iterrows():
    event_id = f"event_{row['venue']}_{row['year']}"
    paper_events.append({
        'paperId': row['paperId'],
        'eventId': event_id
    })

paper_presented_in = pd.DataFrame(paper_events)
paper_presented_in.to_csv('paper_presented_in.csv', index=False)

# 6. Event is part of Organization
event_orgs = []
for event_id, org_name in organizations.items():
    event_orgs.append({
        'eventId': event_id,
        'organizationName': org_name
    })

event_part_of_org = pd.DataFrame(event_orgs)
event_part_of_org.to_csv('event_part_of_org.csv', index=False)

print("Preprocessing complete. The following CSV files have been created:")
print("1. author_nodes.csv - Author nodes")
print("2. paper_nodes.csv - Paper nodes")
print("3. journal_nodes.csv - Journal nodes")
print("4. keyword_nodes.csv - Keyword nodes")
print("5. event_nodes.csv - Event nodes")
print("6. organization_nodes.csv - Organization nodes")
print("7. author_writes_paper.csv - Author writes Paper relationships")
print("8. author_reviews_paper.csv - Author reviews Paper relationships")
print("9. paper_published_in.csv - Paper published in Journal relationships")
print("10. paper_cites_paper.csv - Paper cites Paper relationships")
print("11. paper_keyword_rels.csv - Paper has Keyword relationships")
print("12. paper_presented_in.csv - Paper presented in Event relationships")
print("13. event_part_of_org.csv - Event is part of Organization relationships")