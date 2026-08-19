import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()

COGNODB_URI = os.getenv("COGNODB_URI")
COGNODB_USERNAME = os.getenv("COGNODB_USERNAME", "cognodb")
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD")

if not COGNODB_URI or not COGNODB_PASSWORD:
    raise ValueError(
        "COGNODB_URI and COGNODB_PASSWORD must be configured in your .env file."
    )

def main():
    # Initialize Neo4j Bolt driver with credentials from environment
    driver = GraphDatabase.driver(
        COGNODB_URI,
        auth=(COGNODB_USERNAME, COGNODB_PASSWORD),
    )

    try:
        # Verify connection to CognoDB Cloud
        driver.verify_connectivity()
        print("Successfully connected to CognoDB Cloud!")

        # Execute query
        with driver.session() as session:
            result = session.run("RETURN 1 AS number")
            record = result.single()
            if record:
                print(f"Query Result: {record['number']}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()