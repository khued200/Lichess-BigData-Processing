import chess.pgn
import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time


# Hàm để đọc một số lượng ván cờ từ file PGN
def parse_partial_pgn_to_dataframe(pgn_file, num_games=10):
    games_data = []
    
    # Mở file PGN
    with open(pgn_file, 'r') as pgn:
        # Đếm số lượng ván cờ đã đọc
        game_count = 0
        
        # Đọc từng game
        while game_count < num_games:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break  # Kết thúc tệp nếu không còn ván cờ nào
            
            # display(game.headers)
            
            # Lấy thông tin chung về trận đấu
            game_info = game.headers
            
            # Lấy toàn bộ nước đi của trận đấu
            moves = game.mainline_moves()
            move_list = [str(move) for move in moves]
            
            # Thêm thông tin vào danh sách
            games_data.append({
                # 'Event': game_info.get("Event", ""),
                # 'Site': game_info.get("Site", ""),
                'GameID': game_count+1,
                'Date': game_info.get("Date", ""),
                'Time': game_info.get("UTCTime", ""),
                'Round': game_info.get("Round", ""),
                'White': game_info.get("White", ""),
                'Black': game_info.get("Black", ""),
                'TimeControl':game_info.get("TimeControl", ""),
                'Result': game_info.get("Result", ""),
                'WhiteElo':game_info.get("WhiteElo", ""),
                'BlackElo':game_info.get("BlackElo", ""),
                'Moves': move_list,
                'Variant': game_info.get("Variant", "")
            })
            
            # Tăng biến đếm số lượng ván cờ
            game_count += 1
    
    # Chuyển sang pandas DataFrame
    return pd.DataFrame(games_data)

# Đọc một phần của file PGN (chỉ đọc 5 ván cờ đầu tiên)
pgn_file = 'lichess_db_chess960_rated_2024-09.pgn'
df = parse_partial_pgn_to_dataframe(pgn_file, num_games=1000)

# Hiển thị DataFrame chứa thông tin về các ván cờ đầu tiên
# display(df)
# Tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Địa chỉ của Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=100000*10000, batch_size=16384*100  # Chuyển đổi dữ liệu thành chuỗi JSON
)

# # Địa chỉ của Kafka broker
# bootstrap_servers = 'localhost:9092'
# topic_name = 'chess_demo'

# # Tạo Kafka AdminClient để tạo topic
# admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# # Tạo topic mới
# new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
# admin_client.create_topics(new_topics=[new_topic], validate_only=False)

# Gửi dữ liệu từ DataFrame lên Kafka theo từng khoảng interval 5s
def send_data_in_batches():
    for i in range (min(10000, len(df))):
        message = df.iloc[i].to_dict() # Chuyển từng hàng của DataFrame thành dict
        producer.send('chess-games', value=message)  # Gửi đến topic 'chess_games'
        # if ((i + 1) % 100 == 0) or (i == min(10000, len(df))):  
        #     print(f"Sent {i+1} messages, waiting for 10 seconds...")
        #     time.sleep(10)
        #     producer.flush()

        print(f"Sent {i+1} messages, waiting for 10 seconds...")
        time.sleep(3)
        producer.flush()
        
    # Đảm bảo dữ liệu đã được gửi
    print("Dữ liệu đã được gửi đến Kafka.")
send_data_in_batches()
