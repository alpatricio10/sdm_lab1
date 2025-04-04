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

def generate_weighted_reviewer_count():
    # Generates a reviewer count with a bias towards 3
    return random.choices([2, 3, 4, 5], weights=[0.1, 0.6, 0.2, 0.1])[0]

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

# Clean the venue names
def clean_venue_name(name):
    if isinstance(name, str):
        name = name.strip('"')
        name = re.sub(r'\b\d{4}\b|\[\d{4}\]|\b\d{4}\s+\d+(?:st|nd|rd|th)?', '', name)
        name = re.sub(r'Proceedings of( the)?|Proceeding of the|Proceedings\.?', '', name)
        name = re.sub(r'\b(?:\d+(?:st|nd|rd|th)|First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth|Eleventh|Twelfth|Thirteenth)\b', '', name)
        name = re.sub(r'\(Cat\.\s*No\..*?\)', '', name)
        name = re.sub(r', \d{4}(?:\.|$)', '', name)
        name = re.sub(r'(?:- |, )(?:Proceedings|Volume \d+)', '', name)
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'\s*-\s*', ' ', name)
        name = re.sub(r'\s*:\s*', ': ', name)
        name = re.sub(r'\s*,\s*', ', ', name)
        name = re.sub(r'^\/', '', name)
        name = name.strip(' .,;:-"')
    return name

# Clean text e.g. abstract and title
def clean_text(text):
    if isinstance(text, str):
        text = re.sub(r'[^A-Za-z0-9\s.,-:]', '', text)  
        text = re.sub(r'\s+', ' ', text).strip()  
    return text

def convert_int(value):
    try:
        return int(value)
    except Exception:
        return value

# Load the data
papers = pd.read_csv('final_output/papers.csv')
authors = pd.read_csv('final_output/authors.csv')
venues = pd.read_csv('final_output/venues.csv')
keywords = pd.read_csv('final_output/paper_keywords.csv')

# Clean data
papers['venue'] = papers['venue'].apply(clean_venue_name)
papers['title'] = papers['title'].apply(clean_text)
papers['abstract'] = papers['abstract'].apply(clean_text)
papers['pages'] = papers['pages'].apply(clean_text)
venues['name'] = venues['name'].apply(clean_venue_name)

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
    'doi': papers['doi'],
    'url': papers['url'],
    'citationCount': papers['citationCount']
})
paper_nodes.to_csv('paper_nodes.csv', index=False)

# 3. Journal nodes 
journals = []
for index, row in venues.iterrows():
    if row['venueType'] == 'JournalArticle':
        journals.append({
            'journalName': row['name'],
        })

journal_nodes = pd.DataFrame(journals).drop_duplicates(subset=['journalName'])
journal_nodes.to_csv('journal_nodes.csv', index=False)

# 4. Keyword nodes 
keyword_nodes = pd.DataFrame({
    'keyword': keywords['keyword']
}).drop_duplicates(subset=['keyword'])
keyword_nodes.to_csv('keyword_nodes.csv', index=False)

# 5. Proceedings nodes 
proceedings = []
for index, row in papers.iterrows():
    city, venue = generate_city_and_venue()
    if row['venueType'] == 'Conference':
        proceedings.append({
            'proceedingId': uuid.uuid4(),
            'conferenceName': row['venue'],
            'year': convert_int(row['year']),
            'venue': venue,
            'city': city
        })

proceedings_nodes = pd.DataFrame(proceedings).drop_duplicates(subset=['conferenceName', 'year'])
proceedings_nodes.sort_values(by=['conferenceName', 'year'], inplace=True)
proceedings_nodes['edition'] = proceedings_nodes.groupby('conferenceName')['year'].transform(lambda x: x - x.iloc[0] + 1).astype(int)
proceedings_nodes.to_csv('proceedings_nodes.csv', index=False)

# 6. Conferences nodes 
conferences = []
for index, row in venues.iterrows():
    if row['venueType'] == 'Conference':
        conferences.append({
            'conferenceName': row['name']
        })

conference_nodes = pd.DataFrame(conferences).drop_duplicates(subset=['conferenceName'])
conference_nodes.to_csv('conferences_nodes.csv', index=False)

# 1. Author writes Paper
author_papers = []
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
    if row['venueType'] == 'JournalArticle':
        paper_journal.append({
            'paperId': row['paperId'],
            'journalName': row['venue'],
            'volume': random.randint(1, 50),
            'year': convert_int(row['year'])
        })

paper_published_in = pd.DataFrame(paper_journal)
paper_published_in.to_csv('paper_published_in.csv', index=False)

# 4. Paper cites Paper
paper_citations = []
for index, row in papers.iterrows():
    paper_id = row['paperId']
    if isinstance(row['references'], str):
        references = row['references'].split(';')
        for ref in references:
            ref = ref.strip()
            if ref in papers['paperId'].values:
                paper_citations.append({
                    'sourcePaperId': paper_id,
                    'targetPaperId': ref
                })

paper_cites_paper = pd.DataFrame(paper_citations)
paper_cites_paper.to_csv('paper_cites_paper.csv', index=False)

# 5. Paper presented in 
paper_presented_in = []
for index, row in papers.iterrows():
    if row['venueType'] == 'Conference':
        proceeding = proceedings_nodes[(proceedings_nodes['conferenceName'] == row['venue']) & (proceedings_nodes['year'] == row['year'])]
        if not proceeding.empty:
            proceedingId = proceeding['proceedingId'].iloc[0]  
            paper_presented_in.append({
                'paperId': row['paperId'],
                'proceedingId': proceedingId 
            })

paper_presented_in = pd.DataFrame(paper_presented_in)
paper_presented_in.to_csv('paper_presented_in.csv', index=False)

# 6. Proceedings is part of Conference
proceeding_part_of_conf = proceedings_nodes[['proceedingId', 'conferenceName']].copy()
proceeding_part_of_conf.to_csv('proceeding_part_of.csv', index=False)

# 7. Paper has keywords
paper_has_keyword = pd.DataFrame({
    'paperId': keywords['paperId'],
    'keyword': keywords['keyword'],
})
paper_has_keyword.to_csv('paper_has_keyword.csv', index=False)