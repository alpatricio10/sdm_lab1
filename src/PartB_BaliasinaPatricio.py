from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687" 
username = "neo4j"  
password = "password" 

def clear_step(tx):
    query="MATCH (rc:ResearchCommunity) DETACH DELETE rc;"
    tx.run(query)


def run_step_1(tx):
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

def run_step_2(tx):
    query = """
        MATCH (dbComm:ResearchCommunity {name: "Database"})
        MATCH (dbk:Keyword)-[:BELONGS_TO]->(dbComm)
        WITH dbComm, COLLECT(dbk) AS dbKeywords

        CALL {
        //-----------------------
        // PART A: Conferences
        //-----------------------
        WITH dbKeywords, dbComm
        MATCH (c:Conference)
        OPTIONAL MATCH (c)<-[:IS_PART_OF]-(pr:Proceeding)<-[:PRESENTED_IN]-(p:Paper)
        WITH c, COLLECT(DISTINCT p) AS allPapers, dbKeywords, dbComm
        WHERE size(allPapers) > 0

        UNWIND allPapers AS pap
        OPTIONAL MATCH (pap)-[:HAS_KEYWORD]->(k:Keyword)
        WHERE k IN dbKeywords
        WITH c, dbComm, allPapers, pap, COUNT(k) > 0 AS hasAtLeastOneDB
        WITH c, dbComm, allPapers, SUM(CASE WHEN hasAtLeastOneDB THEN 1 ELSE 0 END) AS dbPaperCount

        WHERE (dbPaperCount / toFloat(size(allPapers))) >= 0.9

        MERGE (c)-[:RELATED_TO]->(dbComm)

        RETURN c.conferenceName AS venueName, 
                "Conference"     AS venueType

        UNION

        //-----------------------
        // PART B: Journals
        //-----------------------
        WITH dbKeywords, dbComm
        MATCH (j:Journal)
        OPTIONAL MATCH (j)<-[:PUBLISHED_IN]-(p:Paper)
        WITH j, COLLECT(DISTINCT p) AS allPapers, dbKeywords, dbComm
        WHERE size(allPapers) > 0

        UNWIND allPapers AS pap
        OPTIONAL MATCH (pap)-[:HAS_KEYWORD]->(k:Keyword)
        WHERE k IN dbKeywords
        WITH j, dbComm, allPapers, pap, COUNT(k) > 0 AS hasAtLeastOneDB
        WITH j, dbComm, allPapers, SUM(CASE WHEN hasAtLeastOneDB THEN 1 ELSE 0 END) AS dbPaperCount

        WHERE (dbPaperCount / toFloat(size(allPapers))) >= 0.9

        MERGE (j)-[:RELATED_TO]->(dbComm)

        RETURN j.journalName AS venueName, 
                "Journal"     AS venueType
        }

        RETURN venueName, venueType
        ORDER BY venueType, venueName
        LIMIT 10;
    """
    result = tx.run(query)
    return result.data()

def run_step_3(tx):
    query = """
        MATCH (dbComm:ResearchCommunity {name: "Database"})
        MATCH (p:Paper)
        WHERE
            ( (p)-[:PUBLISHED_IN]->(:Journal)-[:RELATED_TO]->(dbComm) )
        OR ( (p)-[:PRESENTED_IN]->(:Proceeding)-[:IS_PART_OF]->(:Conference)-[:RELATED_TO]->(dbComm) )

        OPTIONAL MATCH (p)<-[:CITES]-(citingPaper:Paper)
        WHERE
            ( (citingPaper)-[:PUBLISHED_IN]->(:Journal)-[:RELATED_TO]->(dbComm) )
        OR ( (citingPaper)-[:PRESENTED_IN]->(:Proceeding)-[:IS_PART_OF]->(:Conference)-[:RELATED_TO]->(dbComm) )

        WITH dbComm, p, COUNT(DISTINCT citingPaper) AS dbCitations
        ORDER BY dbCitations DESC
        LIMIT 100

        MERGE (dbComm)-[:HAS_TOP_PAPER]->(p)
        RETURN p.paperId AS paperId, p.title AS title, dbCitations;
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
