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

def generate_weighted_reviewer_count():
    # Generates a reviewer count with a bias towards 3
    return random.choices([2, 3, 4, 5], weights=[0.1, 0.6, 0.2, 0.1])[0]

# Function to generate random ISBN
def generate_isbn():
    return f"978-{random.randint(0, 9)}-{random.randint(10000, 99999)}-{random.randint(100, 999)}-{random.randint(0, 9)}"

# Function to generate random city and venue
def generate_city_and_venue():
    city_university_pairs = {
        "New York": ["Columbia University", "New York University", "Cornell Tech", "City University of New York"],
        "San Francisco": ["Stanford University", "University of California, San Francisco", "Berkeley", "San Francisco State University"],
        "Boston": ["Harvard University", "Massachusetts Institute of Technology", "Northeastern University", "Boston University"],
        "Chicago": ["University of Chicago", "Northwestern University", "Illinois Institute of Technology", "Loyola University Chicago"],
        "London": ["University of Oxford", "Imperial College London", "University College London", "King's College London"],
        "Paris": ["Sorbonne University", "École Polytechnique", "Paris-Saclay University", "Sciences Po"],
        "Berlin": ["Humboldt University of Berlin", "Technical University of Berlin", "Free University of Berlin", "Charité - Universitätsmedizin Berlin"],
        "Zurich": ["ETH Zurich", "University of Zurich", "Swiss Federal Institute of Technology"],
        "Tokyo": ["University of Tokyo", "Tokyo Institute of Technology", "Keio University", "Waseda University"],
        "Beijing": ["Tsinghua University", "Peking University", "Beijing University of Posts and Telecommunications", "Beijing Normal University"],
        "Singapore": ["National University of Singapore", "Nanyang Technological University", "Singapore Management University"],
        "Seoul": ["Seoul National University", "Korea Advanced Institute of Science and Technology", "Yonsei University"],
        "Toronto": ["University of Toronto", "York University", "Ryerson University", "University of Waterloo"],
        "Sydney": ["University of Sydney", "University of New South Wales", "Macquarie University", "University of Technology Sydney"],
        "Amsterdam": ["University of Amsterdam", "VU Amsterdam", "Delft University of Technology", "Wageningen University"]
    }
    
    city = random.choice(list(city_university_pairs.keys()))
    venue = random.choice(city_university_pairs[city])
    
    return city, venue

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
keywords = pd.read_csv('keywords.csv')

# Create nodes and relationships dataframes
# 1. Author nodes
author_nodes = pd.DataFrame({
    'authorId': authors['authorId'].apply(lambda x: str(x).split('.')[0]),
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
    'journalName': journals['name']
}).drop_duplicates(subset=['journalName'])
journal_nodes.to_csv('journal_nodes.csv', index=False)

# # 3. Volume nodes (from journals.csv)
# # Create volume during relationship loop
# volume_nodes = pd.DataFrame({
#     'volumeNumber': journals['volume'],
#     'year': 2020
# })
# volume_nodes.to_csv('volume_nodes.csv', index=False)

# 4. Keyword nodes 
keyword_nodes = pd.DataFrame({
    'keyword': keywords['keyword']
})
keyword_nodes.to_csv('keyword_nodes.csv', index=False)

# 5. Conference nodes 
conferences = []
for index, row in papers.iterrows():
    city, venue = generate_city_and_venue()
    if row['isConference'] == True:
        conferences.append({
            'conferenceName': row['venue'],
            'year': row['year'],
            'venue': venue,
            'city': city
        })

# drop duplicates somehow
# conference_nodes = pd.DataFrame(events).drop_duplicates(subset=['eventId'])
conference_nodes = pd.DataFrame(conferences)
conference_nodes.to_csv('conference_nodes.csv', index=False)

# 5. Proceedings nodes 
proceedings = []
for index, row in papers.iterrows():
    if row['isConference'] == True:
        proceedings.append({
            'name': row['journal'],
            'year': row['year']
        })

proceeding_nodes = pd.DataFrame(proceedings)
proceeding_nodes.to_csv('proceedings_nodes.csv', index=False)

# Create relationships
# Volume part of Journal
# paper_events = []
# for index, row in papers.iterrows():
#     event_id = f"event_{row['venue']}_{row['year']}"
#     paper_events.append({
#         'volumeId': row['paperId'],
#         'paperId': event_id
#     })

# volume_part_of = pd.DataFrame(paper_events)
# volume_part_of.to_csv('volume_part_of.csv', index=False)

# 1. Author writes Paper
author_papers = []
# Parse the authors field from papers.csv
for index, row in papers.iterrows():
    paperId = row['paperId']
    if isinstance(row['authors'], str):
        author_list = row['authors'].split(';')
        for i, authorId in enumerate(author_list):
            
            author_papers.append({
                'authorId': authorId,
                'paperId': paperId,
                'corresponding_author': (i == 0)
            })

author_writes_paper = pd.DataFrame(author_papers)
author_writes_paper.to_csv('author_writes_paper.csv', index=False)

# 2. Author reviews Paper (random assignment)
reviewer_assignments = []
for paper_id in papers['paperId']:
    reviewer_count = generate_weighted_reviewer_count()

    # Get reviewers different from authors of this paper
    paper_authors = author_writes_paper[author_writes_paper['paperId'] == paper_id]['authorId'].tolist()
    potential_reviewers = authors[~authors['authorId'].isin(paper_authors)]['authorId'].sample(reviewer_count).apply(lambda x: str(x).split('.')[0])
    
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
    if row['isConference'] == False:
        paper_journal.append({
            'paperId': row['paperId'],
            'journalName': row['journal'],
            'volume': row['volume'],
            'year': row['year']
        })

paper_published_in = pd.DataFrame(paper_journal)
paper_published_in.to_csv('paper_published_in.csv', index=False)

# 4. Paper cites Paper
# paper_citations = []
# for index, row in papers.iterrows():
#     paper_id = row['paperId']
#     # Check if references column exists and has data
#     if 'references' in papers.columns and isinstance(row['references'], str):
#         references = row['references'].split(',')
#         for ref in references:
#             ref = ref.strip()
#             # Check if referenced paper exists in our dataset
#             if ref in papers['paperId'].values:
#                 paper_citations.append({
#                     'sourcePaperId': paper_id,
#                     'targetPaperId': ref
#                 })

# paper_cites_paper = pd.DataFrame(paper_citations)
# paper_cites_paper.to_csv('paper_cites_paper.csv', index=False)

# 5. Paper presented in 
paper_presented = []
for index, row in papers.iterrows():
    if row['isConference'] == True:
        paper_presented.append({
            'paperId': row['paperId'],
            'conferenceName': row['venue']
        })

paper_presented_in = pd.DataFrame(paper_presented)
paper_presented_in.to_csv('paper_presented_in.csv', index=False)

# 6. Event is part of Organization
# event_orgs = []
# for event_id, org_name in organizations.items():
#     event_orgs.append({
#         'eventId': event_id,
#         'organizationName': org_name
#     })

# event_part_of_org = pd.DataFrame(event_orgs)
# event_part_of_org.to_csv('event_part_of_org.csv', index=False)

# 3. Paper has keywords
paper_keywords = []
for index, row in papers.iterrows():
    paper_keywords.append({
        'paperId': row['paperId'],
        'keyword': row['keyword'],
    })

paper_has_keyword = pd.DataFrame(paper_keywords)
paper_has_keyword.to_csv('paper_has_keyword.csv', index=False)

# Add published in conference relationship