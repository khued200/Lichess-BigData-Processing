from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Setup connection and authentication (if required)
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'],  port=9042, auth_provider=auth_provider)  # Replace '127.0.0.1' with the container's IP if needed
session = cluster.connect()

# Tạo keyspace nếu chưa tồn tại
keyspace = "lichess"
create_keyspace_query = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
"""
session.execute(create_keyspace_query)

# Sử dụng keyspace
session.set_keyspace(keyspace)

# Tạo bảng nếu chưa tồn tại
create_table_query = """
CREATE TABLE IF NOT EXISTS games (
    GameID uuid,
    Event text,
    Site text,
    Date text,
    Round text,
    White text,
    Black text,
    Result text,
    UTCDate text,
    UTCTime text,
    WhiteElo text,
    BlackElo text,
    WhiteRatingDiff text,
    BlackRatingDiff text,
    TimeControl text,
    Termination text,
    Variant text,
    Moves text,
    PRIMARY KEY (GameID)
);
"""
session.execute(create_table_query)
print("Table created successfully or already exists.")

# Đóng kết nối
session.shutdown()
cluster.shutdown()
