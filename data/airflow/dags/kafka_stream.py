from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import chess.pgn
import pandas as pd
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
            
            # Lấy thông tin chung về trận đấu
            game_info = game.headers
            
            # Lấy toàn bộ nước đi của trận đấu
            moves = game.mainline_moves()
            move_list = [str(move) for move in moves]
            
            # Thêm thông tin vào danh sách
            games_data.append({
                'GameID': game_count + 1,
                'Date': game_info.get("Date", ""),
                'Time': game_info.get("UTCTime", ""),
                'Round': game_info.get("Round", ""),
                'White': game_info.get("White", ""),
                'Black': game_info.get("Black", ""),
                'TimeControl': game_info.get("TimeControl", ""),
                'Result': game_info.get("Result", ""),
                'WhiteElo': game_info.get("WhiteElo", ""),
                'BlackElo': game_info.get("BlackElo", ""),
                'Moves': move_list,
                'Variant': game_info.get("Variant", "")
            })
            
            # Tăng biến đếm số lượng ván cờ
            game_count += 1
    
    # Chuyển sang pandas DataFrame
    return pd.DataFrame(games_data)

def send_data_to_kafka(df):
    import logging
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],  # Địa chỉ của Kafka broker
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=1000, batch_size=16384  # Chuyển đổi dữ liệu thành chuỗi JSON
    )
    for i in range(len(df)):
        message = df.iloc[i].to_dict()  # Chuyển từng hàng của DataFrame thành dict
        producer.send('chess-games', value=message)  # Gửi đến topic 'chess-games'
    producer.flush()

    # except Exception as e:
    #     logging.error(f'An error occured: {e}')
    # # Gửi dữ liệu từ DataFrame lên Kafka theo từng khoảng interval 3s
    
        
# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 25),
}

def process_and_send_data():
    # Đọc và xử lý file PGN

    import logging

    pgn_file = '/opt/airflow/lichess_db_chess960_rated_2024-09.pgn'
    num_games = 300
    logging.info("Starting PGN file processing")
    logging.info(f"Reading {num_games} games from {pgn_file}")
    df = parse_partial_pgn_to_dataframe(pgn_file, num_games=num_games)
    # Gửi dữ liệu lên Kafka
    send_data_to_kafka(df)

with DAG(
    'chess_pgn_to_kafka',
    default_args=default_args,
    description='Read PGN file and send chess game data to Kafka',
    schedule=None,  # Set to None to run manually
    catchup=False,
) as dag:
    process_task = PythonOperator(
        task_id='process_and_send_data',
        python_callable=process_and_send_data
    )