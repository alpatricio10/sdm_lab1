from neo4j import GraphDatabase
import os

# Neo4j connection params
uri = "bolt://localhost:7687"  
username = "neo4j"  
password = "password"  

def load_csv_data(driver):
    node_load_queries = [
        # Author Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///author_nodes.csv' AS row
        CREATE (:Author {
            authorId: row.authorId, 
            name: row.name, 
            email: row.email
        })
        """,
        
        # Paper Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_nodes.csv' AS row
        CREATE (:Paper {
            paperId: row.paperId, 
            title: row.title, 
            abstract: row.abstract, 
            pages: toInteger(row.pages), 
            doi: row.doi, 
            url: row.url, 
            citationCount: toInteger(row.citationCount)
        })
        """,
        
        # Journal Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///journal_nodes.csv' AS row
        CREATE (:Journal {
            journalName: row.journalName
        })
        """,
        
        # Keyword Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///keyword_nodes.csv' AS row
        CREATE (:Keyword {
            keyword: row.keyword
        })
        """,

        # Proceedings Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///proceedings_nodes.csv' AS row
        CREATE (:Proceeding {
            proceedingId: row.proceedingId, 
            conferenceName: row.conferenceName, 
            year: toInteger(row.year), 
            venue: row.venue, 
            city: row.city
        })
        """,
        
        # Conference Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///conferences_nodes.csv' AS row
        CREATE (:Conference {
            conferenceName: row.conferenceName
        })
        """
    ]

    relationship_load_queries = [
        # Author WRITES Paper Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///author_writes_paper.csv' AS row
        MATCH (a:Author {authorId: row.authorId})
        MATCH (p:Paper {paperId: row.paperId})
        CREATE (a)-[:WRITES {corresponding_author: row.corresponding_author}]->(p)
        """,
        
        # Author REVIEWS Paper Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///author_reviews_paper.csv' AS row
        MATCH (a:Author {authorId: row.authorId})
        MATCH (p:Paper {paperId: row.paperId})
        CREATE (a)-[:REVIEWS]->(p)
        """,
        
        # Paper PUBLISHED_IN Journal Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_published_in.csv' AS row
        MATCH (p:Paper {paperId: row.paperId})
        MATCH (j:Journal {journalName: row.journalName})
        CREATE (p)-[:PUBLISHED_IN {
            volume: row.volume, 
            year: toInteger(row.year)
        }]->(j)
        """,
        
        # Paper HAS_KEYWORD Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_has_keyword.csv' AS row
        MATCH (p:Paper {paperId: row.paperId})
        MATCH (k:Keyword {keyword: row.keyword})
        CREATE (p)-[:HAS_KEYWORD]->(k)
        """,

        # Paper CITES Paper Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_cites_paper.csv' AS row
        MATCH (p1:Paper {paperId: row.sourcePaperId})
        MATCH (p2:Paper {paperId: row.targetPaperId})
        CREATE (p1)-[:CITES]->(p2)
        """,
        
        # Paper PRESENTED_IN Proceeding Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_presented_in.csv' AS row
        MATCH (p:Paper {paperId: row.paperId})
        MATCH (pr:Proceeding {proceedingId: row.proceedingId})
        CREATE (p)-[:PRESENTED_IN]->(pr)
        """,

        # Proceeding PART_OF Conference Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_part_of.csv' AS row
        MATCH (pr:Proceeding {proceedingId: row.proceedingId})
        MATCH (c:Conference {conferenceName: row.conferenceName})
        CREATE (pr)-[:IS_PART_OF]->(c)
        """
    ]

    with driver.session() as session:
        print("Clearing existing data...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Loading Nodes...")
        for query in node_load_queries:
            session.run(query)

        print("Loading Relationships...")
        for query in relationship_load_queries:
            session.run(query)

def create_indexes(driver):
    index_queries = [
        "CREATE INDEX author_id IF NOT EXISTS FOR (a:Author) ON (a.authorId)",
        "CREATE INDEX paper_id IF NOT EXISTS FOR (p:Paper) ON (p.paperId)",
        "CREATE INDEX journal_name IF NOT EXISTS FOR (j:Journal) ON (j.journalName)",
        "CREATE INDEX keyword IF NOT EXISTS FOR (k:Keyword) ON (k.keyword)",
        "CREATE INDEX conference_name IF NOT EXISTS FOR (c:Conference) ON (c.conferenceName)"
    ]

    print("\nCreating Indexes...")
    with driver.session() as session:
        for query in index_queries:
            session.run(query)

with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    try:
        load_csv_data(driver)
        create_indexes(driver)
    except Exception as e:
        print(f"An error occurred: {e}")