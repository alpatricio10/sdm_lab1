from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687" 
username = "neo4j"  
password = "password"  

def clean_rank_graph(tx):
    query = """CALL gds.graph.drop($graphName) YIELD graphName RETURN graphName"""
    tx.run(query, graphName='rankGraph')

def create_rank_graph(tx):
    query = """CALL gds.graph.project('rankGraph',['Paper'],'CITES')"""
    tx.run(query)

def run_pagerank(tx,graph):
    query = """
        CALL gds.pageRank.stream('rankGraph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC, title ASC
        LIMIT 10
    """
    result = tx.run(query, graph=graph)
    print("PageRank results written to Paper nodes.")
    return result.data()

def clean_louvain_graph(tx):
    query = """CALL gds.graph.drop($graphName) YIELD graphName RETURN graphName"""
    tx.run(query, graphName='louvainGraph')

def create_louvain_graph(tx):
    query = """CALL gds.graph.project('louvainGraph',['Paper'],'CITES')"""
    tx.run(query)

def run_louvain(tx):
    query = """
    CALL gds.louvain.stream('louvainGraph')
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).title AS title, communityId
        ORDER BY communityId DESC
        LIMIT 10
    """
    result = tx.run(query)
    print("Louvain Community Detection results written to Keyword nodes.")
    return result.data()

with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    try:
        # Run PageRank
        with driver.session() as session:
            try:
                session.execute_write(clean_rank_graph)
            except Exception as e:
                print("Attempted to drop rank graph, but it does not exist.")
                pass

            session.execute_write(create_rank_graph)
            summary = session.execute_write(run_pagerank, 'rankGraph')
            print(summary)

        print("\n")

        # Run Louvain Community Detection
        with driver.session() as session:
            try:
                session.execute_write(clean_louvain_graph)
            except Exception as e:
                print("Attempted to drop rank graph, but it does not exist.")
                pass

            session.execute_write(create_louvain_graph)
            summary = session.execute_write(run_louvain)
            print(summary)

    except Exception as e:
        print(f"An error occurred: {e}")
