from falkordb import FalkorDB

db = FalkorDB(host="localhost", port=6379)

graph = db.select_graph("benchmark")

result = graph.query("RETURN 1 AS number")

print("Successfully connected to FalkorDB!")
print("Query Result:", result.result_set[0][0])