from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "Neo4jBench2026!"

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

driver.verify_connectivity()
print("Successfully connected to Neo4j!")

with driver.session() as session:
    result = session.run("RETURN 1 AS number")
    record = result.single()
    print("Query Result:", record["number"])

driver.close()