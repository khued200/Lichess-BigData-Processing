from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Kết nối với Cassandra
cluster = Cluster(['localhost'])  # Thay 'localhost' bằng địa chỉ IP của container nếu cần
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
CREATE TABLE IF NOT EXISTS your_table (
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
    PRIMARY KEY (Event, Date) 
);
"""
session.execute(SimpleStatement(create_table_query))

# Đóng kết nối
session.shutdown()
cluster.shutdown()
