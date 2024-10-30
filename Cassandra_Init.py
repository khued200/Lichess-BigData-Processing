from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Setup connection and authentication (if required)
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'],  port=9042, auth_provider=auth_provider)  # Replace '127.0.0.1' with the container's IP if needed
session = cluster.connect()

# Step 1: Create Keyspace (if it doesn't exist)
keyspace_query = """
CREATE KEYSPACE IF NOT EXISTS chess_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
"""
session.execute(keyspace_query)
print("Keyspace created successfully or already exists.")

# Step 2: Connect to the Keyspace
session.set_keyspace("chess_keyspace")

# Step 3: Create Table
table_query = """
CREATE TABLE IF NOT EXISTS chess_game (
    gameid int,
    date date,
    time time,
    round text,
    white text,
    black text,
    timecontrol text,
    result text,
    whiteelo int,
    blackelo int,
    moves text,
    variant text,
    PRIMARY KEY (gameid)
);
"""
session.execute(table_query)
print("Table created successfully or already exists.")

# Step 4: Close the connection
session.shutdown()
cluster.shutdown()
