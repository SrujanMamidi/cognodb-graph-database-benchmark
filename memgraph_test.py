from neo4j import GraphDatabase

URI = "bolt://localhost:7688"
USERNAME = ""
PASSWORD = ""

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

driver.verify_connectivity()
print("Successfully connected to Memgraph!")

with driver.session() as session:
    result = session.run("RETURN 1 AS number")
    record = result.single()
    print("Query Result:", record["number"])

driver.close()