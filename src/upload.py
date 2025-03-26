from py2neo import Graph
import os

# Connect to Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

def load_csv_data():
    # Clear existing data
    graph.run("MATCH (n) DETACH DELETE n")

    # Cypher commands to load nodes and relationships
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
        
        # Conference Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///conference_nodes.csv' AS row
        CREATE (:Conference {
            conferenceName: row.conferenceName, 
            year: toInteger(row.year), 
            venue: row.venue, 
            city: row.city
        })
        """,
        
        # Proceedings Nodes
        """
        LOAD CSV WITH HEADERS FROM 'file:///proceedings_nodes.csv' AS row
        CREATE (:Proceeding {
            name: row.name, 
            year: toInteger(row.year)
        })
        """
    ]

    # Relationship load queries
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
        
        # Paper PRESENTED_IN Conference Relationships
        """
        LOAD CSV WITH HEADERS FROM 'file:///paper_presented_in.csv' AS row
        MATCH (p:Paper {paperId: row.paperId})
        MATCH (c:Conference {conferenceName: row.conferenceName})
        CREATE (p)-[:PRESENTED_IN]->(c)
        """
    ]

    # Execute node loading queries
    print("Loading Nodes...")
    for query in node_load_queries:
        graph.run(query)
        print(f"Executed: {query.split('\n')[0]}")

    # Execute relationship loading queries
    print("\nLoading Relationships...")
    for query in relationship_load_queries:
        graph.run(query)
        print(f"Executed: {query.split('\n')[0]}")

    print("\nData loading complete!")

def create_indexes():
    index_queries = [
        "CREATE INDEX author_id IF NOT EXISTS FOR (a:Author) ON (a.authorId)",
        "CREATE INDEX paper_id IF NOT EXISTS FOR (p:Paper) ON (p.paperId)",
        "CREATE INDEX journal_name IF NOT EXISTS FOR (j:Journal) ON (j.journalName)",
        "CREATE INDEX keyword IF NOT EXISTS FOR (k:Keyword) ON (k.keyword)",
        "CREATE INDEX conference_name IF NOT EXISTS FOR (c:Conference) ON (c.conferenceName)"
    ]

    print("\nCreating Indexes...")
    for query in index_queries:
        graph.run(query)
        print(f"Created index: {query}")

def main():
    try:
        load_csv_data()
        create_indexes()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()