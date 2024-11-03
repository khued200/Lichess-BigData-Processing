from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4

# Setup connection and authentication
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)  # Replace '127.0.0.1' with the container's IP if needed
session = cluster.connect('lichess')

# Insert sample data into the 'games' table
insert_query = """
INSERT INTO games (GameID, Event, Site, Date, Round, White, Black, Result, UTCDate, UTCTime, WhiteElo, BlackElo, 
                   WhiteRatingDiff, BlackRatingDiff, TimeControl, Termination, Variant, Moves) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# Dữ liệu mẫu
sample_data = [
    (uuid4(), "World Championship", "https://lichess.org", "2024-01-01", "1", "Magnus Carlsen", "Ian Nepomniachtchi", 
     "1-0", "2024-01-01", "12:00:00", "2862", "2789", "+5", "-5", "90+30", "Normal", "Standard", "e4 e5 Nf3 Nc6"),
    (uuid4(), "Candidates Tournament", "https://lichess.org", "2024-02-15", "5", "Fabiano Caruana", "Ding Liren", 
     "0-1", "2024-02-15", "14:30:00", "2820", "2805", "-3", "+3", "60+30", "Resignation", "Standard", "d4 Nf6 c4 e6"),
    (uuid4(), "Online Blitz", "https://lichess.org", "2024-03-10", "3", "Alireza Firouzja", "Hikaru Nakamura", 
     "1/2-1/2", "2024-03-10", "20:15:00", "2750", "2780", "+1", "-1", "3+2", "Time forfeit", "Blitz", "e4 e5 Nf3 Nc6 Bb5")
]

# Chèn dữ liệu mẫu
for record in sample_data:
    session.execute(insert_query, record)

print("Dữ liệu mẫu đã được chèn thành công.")

# Đóng kết nối
session.shutdown()
cluster.shutdown()