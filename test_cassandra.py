from cassandra.cluster import Cluster

# Kết nối đến Cassandra
cluster = Cluster(['localhost'])  # Địa chỉ của Cassandra cluster (ví dụ: 'localhost' nếu Cassandra chạy trên máy cục bộ)
session = cluster.connect('lichess')  # Kết nối đến keyspace 'lichess'

# Dữ liệu mẫu để chèn vào bảng
insert_query = """
    INSERT INTO games (Event, Site, Date, Round, White, Black, Result, UTCDate, UTCTime, 
    WhiteElo, BlackElo, WhiteRatingDiff, BlackRatingDiff, TimeControl, Termination, Variant, Moves, GameID)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# Dữ liệu mẫu
data = (
    'Event Example', 'Site Example', '2024-12-15', '1', 
    'PlayerWhite', 'PlayerBlack', '1-0', '2024-12-15', '14:30', 
    '2400', '2200', '50', '-30', 'Blitz', 'Normal', 'Standard', 'e4 e5...', 'game12345'
)

# Chèn dữ liệu vào bảng
session.execute(insert_query, data)

print("Dữ liệu đã được chèn thành công.")

# Đóng kết nối
cluster.shutdown()
