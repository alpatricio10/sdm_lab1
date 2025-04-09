from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687" 
username = "neo4j"  
password = "password" 

def run_query_1(tx):
    query = """
        MATCH (c:Conference)<-[:IS_PART_OF]-(pr:Proceeding)<-[:PRESENTED_IN]-(p:Paper)
        WITH c, pr, p
        ORDER BY c.conferenceName, p.citationCount DESC
        WITH  c.conferenceName AS conferenceName, COLLECT(p)[0..3] AS topPapers
        UNWIND topPapers AS paper
        RETURN conferenceName, paper.title AS paperTitle, paper.citationCount AS citations
        LIMIT 10
    """
    result = tx.run(query)
    return result.data()

def run_query_2(tx):
    query = """
        MATCH (a:Author)-[:WRITES]->(p:Paper)-[:PRESENTED_IN]->(pr:Proceeding)-[:IS_PART_OF]->(c:Conference) WITH c.conferenceName AS conferenceName, a.name AS authorName, COUNT(DISTINCT pr.edition) AS editionCount
        WHERE editionCount >= 4
        RETURN conferenceName, COLLECT(authorName) AS community
        LIMIT 10
    """
    result = tx.run(query)
    return result.data()

def run_query_3(tx):
    query = """
        MATCH (j1:Journal)<-[pub1:PUBLISHED_IN]-(p:Paper)-[cite:CITES]->(p2:Paper)-[pub2:PUBLISHED_IN]->(j2:Journal)
        WITH j2 AS journal, pub1.year AS currentYear, COUNT(cite) AS totalCitations, COUNT(DISTINCT p2) AS totalPublications, pub2
        WHERE pub2.year = currentYear - 1 OR pub2.year = currentYear - 2
        RETURN journal.journalName AS journalName, currentYear AS year, toFloat(totalCitations) / totalPublications AS impactFactor
        ORDER BY impactFactor DESC
        LIMIT 10
    """
    result = tx.run(query)
    return result.data()

def run_query_4(tx):
    query = """
        MATCH (a:Author)-[:WRITES]->(p:Paper)
        WITH a.name AS authorName, p.citationCount AS citationCount
        ORDER BY citationCount DESC
        WITH authorName, COLLECT(citationCount) AS sortedCitationCount
        UNWIND RANGE(1, SIZE(sortedCitationCount)) AS hIndex
        WITH authorName, hIndex, sortedCitationCount[hIndex-1] AS citationCount
        WHERE citationCount >= hIndex
        RETURN authorName, MAX(hIndex) AS hIndex
        ORDER BY hIndex DESC
        LIMIT 10
    """
    result = tx.run(query)
    return result.data()

with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    try:
        with driver.session() as session:
            print("Query 1 Results:\n")
            q1_result = session.execute_write(run_query_1)
            print(q1_result)

        print("\n")

        with driver.session() as session:
            print("Query 2 Results:\n")
            q2_result = session.execute_write(run_query_2)
            print(q2_result)

        print("\n")

        with driver.session() as session:
            print("Query 3 Results:\n")
            q3_result = session.execute_write(run_query_3)
            print(q3_result)

        print("\n")

        with driver.session() as session:
            print("Query 4 Results:\n")
            q4_result = session.execute_write(run_query_4)
            print(q4_result)

    except Exception as e:
        print(f"An error occurred: {e}")
