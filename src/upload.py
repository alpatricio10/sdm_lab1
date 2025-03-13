from py2neo import Graph, Node, Relationship
import pandas as pd

# Connect to Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Clear existing data
graph.run("MATCH (n) DETACH DELETE n")

# Load nodes
def load_nodes(file_path, label, properties):
    df = pd.read_csv(file_path)
    
    # Create batch of nodes
    tx = graph.begin()
    count = 0
    
    for _, row in df.iterrows():
        node_props = {prop: row[prop] for prop in properties if prop in row}
        node = Node(label, **node_props)
        tx.create(node)
        
        count += 1
        if count % 1000 == 0:  # Commit in batches of 1000
            graph.commit(tx)
            tx = graph.begin()
    
    graph.commit(tx)
    print(f"Loaded {count} {label} nodes")

# Load relationships
def load_relationships(file_path, start_label, end_label, rel_type, start_key, end_key, properties=None):
    if properties is None:
        properties = []
    
    df = pd.read_csv(file_path)
    
    # Create batch of relationships
    tx = graph.begin()
    count = 0
    
    for _, row in df.iterrows():
        # Get the start and end nodes
        start_node = graph.nodes.match(start_label, **{start_key.split('.')[-1]: row[start_key]}).first()
        end_node = graph.nodes.match(end_label, **{end_key.split('.')[-1]: row[end_key]}).first()
        
        if start_node and end_node:
            # Create relationship properties
            rel_props = {prop: row[prop] for prop in properties if prop in row}
            rel = Relationship(start_node, rel_type, end_node, **rel_props)
            tx.create(rel)
            
            count += 1
            if count % 1000 == 0:  # Commit in batches of 1000
                graph.commit(tx)
                tx = graph.begin()
    
    graph.commit(tx)
    print(f"Loaded {count} {rel_type} relationships")

# Load nodes
load_nodes('author_nodes.csv', 'Author', ['authorId', 'name', 'email'])
load_nodes('paper_nodes.csv', 'Paper', ['paperId', 'title', 'abstract', 'pages', 'doi', 'url', 'citationCount'])
load_nodes('journal_nodes.csv', 'Journal', ['name'])
load_nodes('keyword_nodes.csv', 'Keyword', ['name'])
load_nodes('event_nodes.csv', 'Event', ['eventId', 'year', 'venue', 'city'])
load_nodes('organization_nodes.csv', 'Organization', ['name'])

# Load edges
load_relationships('author_writes_paper.csv', 'Author', 'Paper', 'WRITES', 'authorId', 'paperId', ['corresponding_author'])
load_relationships('author_reviews_paper.csv', 'Author', 'Paper', 'REVIEWS', 'authorId', 'paperId')
load_relationships('paper_published_in.csv', 'Paper', 'Journal', 'PUBLISHED_IN', 'paperId', 'journalName', ['volume', 'year', 'isbn'])
load_relationships('paper_cites_paper.csv', 'Paper', 'Paper', 'CITES', 'sourcePaperId', 'targetPaperId')
load_relationships('paper_keyword_rels.csv', 'Paper', 'Keyword', 'HAS_KEYWORD', 'paperId', 'keyword')
load_relationships('paper_presented_in.csv', 'Paper', 'Event', 'PRESENTED_IN', 'paperId', 'eventId')