from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import chess.pgn
import pandas as pd
import json
import time
from datetime import timedelta
import logging
import os
from airflow.models import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient

# Function to read a specific range of games from the PGN file
def process_date_pgn(pgn_file, start_game, end_game, output_file):
    games_data = []
    with open(pgn_file, 'r') as pgn:
        game_count = 0
        while game_count < end_game:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break  # End of file

            if game_count >= start_game:
                game_info = game.headers
                moves = game.mainline_moves()
                move_list = [str(move) for move in moves]

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

            game_count += 1

    # Save processed data to a CSV file for the next task
    df = pd.DataFrame(games_data)
    df.to_csv(output_file, index=False)
    logging.info(f"Processed games {start_game} to {end_game} and saved to {output_file}")


# Function to send batches of games to Kafka
def send_data_to_kafka(csv_file, batch_size=10, interval=60, broker='kafka:9092'):
    from kafka import KafkaProducer

    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    df = pd.read_csv(csv_file)
    total_rows = len(df)
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=5000,
            acks='all'
        )
    except Exception as e:
        logging.error(f"Failed to connect to Kafka broker {broker}: {e}")
        failed_batch_file = f"{failed_dir}/failed_df_{time.time()}.json"
        df.to_json(failed_batch_file, orient='records')
        logging.info(f"Saved failed batch to {failed_batch_file}")
        raise
    # Let Airflow handle the task failure and retry
    # Load processed data from CSV

    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df.iloc[start_idx:end_idx]

        try:
            for _, row in batch.iterrows():
                message = row.to_dict()
                producer.send('chess-games', value=message).get(timeout=10)  # Đợi kết quả gửi, timeout 10 giây
                producer.flush()
            print(f"Sent batch of {len(batch)} games to Kafka broker {broker}")
        except Exception as e:
            logging.error(f"Kafka error: {e}")
            failed_batch_file = f"{failed_dir}/failed_batch_{time.time()}.json"
            batch.to_json(failed_batch_file, orient='records')
            logging.info(f"Saved failed batch to {failed_batch_file}")
            raise

        if end_idx < total_rows:
            time.sleep(interval)
    
    logging.info(f"Finished sending data from {csv_file} to Kafka broker {broker}")
    # os.remove(csv_file)


# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 25),
}

pgn_file = '/opt/airflow/lichess_db_chess960_rated_2024-09.pgn'
output_dir = '/opt/airflow/output_chunks'
failed_dir = '/opt/airflow/failed_batches'

os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists
os.makedirs(failed_dir, exist_ok=True)  # Ensure failed directory exists

def create_game_ranges(num_games, num_parallel_task):
    game_ranges = []
    games_per_task = num_games // num_parallel_task  # Number of games per task
    remainder = num_games % num_parallel_task       # Remaining games

    start = 0
    for i in range(num_parallel_task):
        end = start + games_per_task
        # Distribute the remainder by adding 1 game to some tasks
        if remainder > 0:
            end += 1
            remainder -= 1
        game_ranges.append((start, end))
        start = end

    return game_ranges

# Example usage:
num_games = 3000
num_parallel_task = 4

game_ranges = create_game_ranges(num_games, num_parallel_task) # Add more ranges as needed

# Define Kafka brokers for each task
brokers = ['kafka-1:9093', 'kafka-2:9094','kafka-3:9095']  # Example brokers for each task



def is_broker_alive_admin(broker):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=[broker]
        )
        # Fetch cluster metadata to confirm broker availability
        metadata = admin_client.describe_cluster()
        print(f"Broker {broker} is alive. Cluster info: {metadata}")
        return True
    except KafkaError as e:
        print(f"Broker {broker} is unavailable: {e}")
        return False

def retry_failed_batches(failed_dir, brokers):
    from kafka import KafkaProducer

    producer = None
    
    # Attempt to connect to one of the available brokers
    for broker in brokers:
        if is_broker_alive_admin(broker):
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                retry_backoff_ms=1000,
                acks='all'
            )
            break

    if not producer:
        raise Exception("All Kafka brokers are unavailable. Could not establish a connection.")

    for file_name in os.listdir(failed_dir):
        if (file_name.startswith("failed_batch_") or file_name.startswith("failed_df_")) and file_name.endswith(".json"):
            file_path = os.path.join(failed_dir, file_name)
            with open(file_path, 'r') as f:
                batch = json.load(f)
                for record in batch:
                    producer.send('chess-games', value=record)
            os.remove(file_path)  # Xóa file sau khi gửi thành công
            logging.info(f"Retried and sent data from {file_path}")


with DAG(
    'multiple_process',
    default_args=default_args,
    description='Process PGN file in chunks and send to Kafka',
    schedule=None,
    catchup=False,
    concurrency=6,  # Maximum number of tasks allowed to run concurrently in this DAG
    max_active_tasks=6,  # Maximum number of active tasks at any point
) as dag:
    process_tasks = []
    send_tasks = []

    for idx, (start_game, end_game) in enumerate(game_ranges):
        output_file = os.path.join(output_dir, f'chess_games_{start_game}_{end_game}.csv')

        # Task to process a chunk of games
        process_task = PythonOperator(
            task_id=f'process_games_{start_game}_to_{end_game}',
            python_callable=process_date_pgn,
            op_kwargs={
                'pgn_file': pgn_file,
                'start_game': start_game,
                'end_game': end_game,
                'output_file': output_file,
            }
            # retries=5,
            # retry_delay=timedelta(seconds=5),
        )
        process_tasks.append(process_task)

        # Assign a broker for each send task
        broker = brokers[idx % len(brokers)]  # Cycle through the brokers list if there are more tasks than brokers

        # Task to send processed games to Kafka
        send_task = PythonOperator(
            task_id=f'send_games_{start_game}_to_{end_game}',
            python_callable=send_data_to_kafka,
            op_kwargs={
                'csv_file': output_file,
                'batch_size': 500,
                'interval': 5,
                'broker': broker,  # Pass the selected broker
            }            
        )
        send_tasks.append(send_task)

        # Set dependencies: process -> send
        process_task >> send_task

    retry_task = PythonOperator(
        task_id='retry_failed_batches',
        python_callable=retry_failed_batches,
        op_kwargs={
            'failed_dir': failed_dir,
            'brokers': brokers  # Chọn broker mới để gửi lại
        },
        trigger_rule=TriggerRule.ALL_DONE
    )
    send_tasks >> retry_task 