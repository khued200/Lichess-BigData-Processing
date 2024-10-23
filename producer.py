from kafka import KafkaProducer
import json
import time
import read_file
import os

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],                           # Địa chỉ Kafka broker
    value_serializer=lambda v : json.dumps(v).encode('utf-8'),      # Chuyển dữ liệu thành dạng JSON
    api_version=(3, 8, 0)                                           # Phiên bản Kafka
)

# Tên của topic
topic_name = 'chess'

data_dir = 'data'
file_name = 'lichess_db_antichess_rated_2024-09.pgn'
pgn_file_path = os.path.join(data_dir, file_name)
games_data = read_file.read_pgn_to_dataframe(pgn_file_path)

# Gửi dữ liệu liên tục
for i in range(10):
    game_data = games_data[i]
    
    producer.send(topic_name, game_data)  # Gửi dữ liệu đến Kafka topic
    # print(f"Sent topic {i}: {game_data}")
    print(f'Sent {i} data')
    time.sleep(0.5)  # Tạm dừng 1 giây giữa mỗi lần gửi

print('Data has been sent')
producer.flush()  # Đảm bảo tất cả tin nhắn được gửi
