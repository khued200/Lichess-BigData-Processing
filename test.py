from stream.producer import send_data_in_batches
from utils.read_file import read_pgn_to_dataframe


df = read_pgn_to_dataframe("./data/lichess_db_antichess_rated_2024-09.pgn",num_games=100 )
send_data_in_batches(df, kafka_server="localhost:29092")

# consume_messages()
