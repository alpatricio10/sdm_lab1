from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

def clear_communities(tx):
    tx.run("""
        MATCH (rc:ResearchCommunity)
        DETACH DELETE rc
    """)

def run_step_1(tx):
    query = """
    MERGE (rc:ResearchCommunity {name: "Database"})
    WITH rc
    UNWIND [
      "data management",
      "indexing",
      "data modeling",
      "big data",
      "data processing",
      "data storage",
      "data querying"
    ] AS dbKw
    MERGE (k:Keyword {keyword: dbKw})
    MERGE (k)-[:BELONGS_TO]->(rc)
    RETURN rc.name AS communityName, collect(k.keyword) AS keywords
    """
    return list(tx.run(query))


def run_step_2(tx):
    query = """
    MATCH (dbComm:ResearchCommunity {name: "Database"})
    MATCH (dbk:Keyword)-[:BELONGS_TO]->(dbComm)
    WITH dbComm, COLLECT(dbk) AS dbKeywords

    CALL {
      // PART A: Conferences
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
      RETURN c.conferenceName AS venueName, "Conference" AS venueType

      UNION

      // PART B: Journals
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
      RETURN j.journalName AS venueName, "Journal" AS venueType
    }
    RETURN venueName, venueType
    ORDER BY venueType, venueName
    LIMIT 10
    """
    return list(tx.run(query))

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

        WITH p, COUNT(DISTINCT citingPaper) AS dbCitations
        ORDER BY dbCitations DESC
        LIMIT 100

        SET p:TopDBPaper
        RETURN p.paperId AS paperId, p.title AS title, dbCitations
        LIMIT 10
    """
    return list(tx.run(query))

def run_step_4(tx):
    query = """
        MATCH (dbComm:ResearchCommunity {name: "Database"})
        MATCH (a:Author)-[:WRITES]->(p:TopDBPaper)
        WITH dbComm, a, COUNT(DISTINCT p) AS topPaperCount
        MERGE (dbComm)-[r:HAS_GOOD_REVIEWER]->(a)
        SET r.topPaperCount = topPaperCount,
            r.isGuru = (topPaperCount >= 2)
        RETURN a.authorId AS authorId,
               a.name AS authorName,
               r.topPaperCount AS relPaperCount,
               r.isGuru AS relIsGuru
        ORDER BY relPaperCount DESC, authorName
        LIMIT 10
    """
    return list(tx.run(query))


# ------------------------------
# MAIN SCRIPT
# ------------------------------
if __name__ == "__main__":
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        session.execute_write(clear_communities)

        print("Step 1 Results:")
        step1_res = session.execute_write(run_step_1)
        print(step1_res)

        print("Step 2 Results:")
        step2_res = session.execute_write(run_step_2)
        print(step2_res)

        print("\nStep 3 Results:")
        step3_res = session.execute_write(run_step_3)
        print(step3_res)

        print("\nStep 4 Results:")
        step4_res = session.execute_write(run_step_4)
        print(step4_res)

    driver.close()
