from neo4j import GraphDatabase
import pandas as pd
import random
import os

# Neo4j connection params
uri = "bolt://localhost:7687"  
username = "neo4j"  
password = "password"  

# Generate random affiliation if it does not exist
def generate_affiliation(text):
    organizations = [
        "Columbia University", "New York University", "Cornell Tech", "City University of New York",
        "Stanford University", "University of California, San Francisco", "Berkeley", "San Francisco State University",
        "Harvard University", "Massachusetts Institute of Technology", "Northeastern University", "Boston University",
        "University of Chicago", "Northwestern University", "Illinois Institute of Technology", "Loyola University Chicago",
        "University of Oxford", "Imperial College London", "University College London", "King's College London",
        "Sorbonne University", "École Polytechnique", "Paris-Saclay University", "Sciences Po",
        "Humboldt University of Berlin", "Technical University of Berlin", "Free University of Berlin", "Charité - Universitätsmedizin Berlin",
        "ETH Zurich", "University of Zurich", "Swiss Federal Institute of Technology",
        "University of Tokyo", "Tokyo Institute of Technology", "Keio University", "Waseda University",
        "Tsinghua University", "Peking University", "Beijing University of Posts and Telecommunications", "Beijing Normal University",
        "National University of Singapore", "Nanyang Technological University", "Singapore Management University",
        "Seoul National University", "Korea Advanced Institute of Science and Technology", "Yonsei University",
        "University of Toronto", "York University", "Ryerson University", "University of Waterloo",
        "University of Sydney", "University of New South Wales", "Macquarie University", "University of Technology Sydney",
        "University of Amsterdam", "VU Amsterdam", "Delft University of Technology", "Wageningen University",
        "Google", "Microsoft", "Amazon", "Apple", "Meta", "IBM", "Intel", "NVIDIA", "Tesla", "SpaceX",
        "Netflix", "Salesforce", "Oracle", "SAP", "Accenture", "Deloitte", "PwC", "EY", "Tata Consultancy Services", "Infosys"
    ]

    if isinstance(text, str) and not pd.isna(text) and text != '':
        return text

    affiliation = random.choice(organizations)
    return affiliation

# Create Affiliation Nodes
def create_affiliation_nodes(tx, affiliations_list):
    query = """
        UNWIND $affiliations AS affiliation
        CREATE (a:Affiliation {affiliationName: affiliation})
    """
    tx.run(query, affiliations=affiliations_list)


def create_author_affiliation_edge(tx, authorId, affiliationName):
    query = """
        MATCH (a:Author {authorId: $authorId})
        MATCH (af:Affiliation {affiliationName: $affiliationName})
        CREATE (a)-[:IS_FROM]->(af)
    """
    tx.run(query, authorId=authorId, affiliationName=affiliationName)

def update_reviews(tx):
    # Find all papers
    papers = tx.run("MATCH (p:Paper) RETURN p").data()

    for paper_record in papers:
        paper = paper_record['p']
        paperId = paper['paperId']

        # Find all reviewers for the current paper
        reviewers_result = tx.run("""
            MATCH (a:Author)-[r:REVIEWS]->(p:Paper)
            WHERE p.paperId = $paperId
            RETURN a, r
        """, paperId=paperId)
        reviewers = reviewers_result.data()

        num_reviewers = len(reviewers)

        if num_reviewers > 0:
            # Minimum number of approvals needed for a majority
            min_approvals = (num_reviewers // 2) + 1
            decisions = ['True'] * min_approvals + ['False'] * (num_reviewers - min_approvals)
            random.shuffle(decisions)

            for i, reviewer_record in enumerate(reviewers):
                reviewer = reviewer_record['a']

                # Assign properties
                decision = decisions[i]
                comment = "This is a random comment"  # set as a fixed comment

                # Update the properties
                tx.run("""
                    MATCH (a:Author)-[r:REVIEWS]->(p:Paper)
                    WHERE a.authorId = $reviewerId AND p.paperId = $paperId
                    SET r.decision = $decision, r.comment = $comment
                    RETURN r
                """, reviewerId=str(reviewer['authorId']), paperId=paperId, decision=decision, comment=comment)

# Load data
authors = pd.read_csv('final_output/authors.csv')

# Generate affiliations for authors
authors['affiliations'] = authors['affiliations'].apply(generate_affiliation)

with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    affiliations = authors[['affiliations']].drop_duplicates().reset_index(drop=True)
    affiliation_names = affiliations['affiliations'].tolist()

    with driver.session() as session:
        # Clean Up Affiliations
        session.run("MATCH p=()-[r:IS_FROM]->() DETACH DELETE r")
        session.run("MATCH (af:Affiliation) DETACH DELETE af")
        
        # Create Affiliation Nodes
        session.execute_write(create_affiliation_nodes, affiliation_names)

        # Connect Authors to Affiliations
        for index, row in authors.iterrows():
            authorId = str(row['authorId'])
            affiliationName = str(row['affiliations'])
            session.execute_write(create_author_affiliation_edge, authorId, affiliationName)

        # Add Review Details
        session.execute_write(update_reviews)
