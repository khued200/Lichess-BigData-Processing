import chess.pgn
import pandas as pd
import os

def read_pgn_to_dataframe(file_path):
    games_data = []
    with open(file_path, 'r', encoding='utf-8') as pgn_file:
        for i in range(10):
            game = chess.pgn.read_game(pgn_file)
    
            if game is None:
                break
            # Extract relevant information from the game
            headers = game.headers

            game_info = {
                'Event': headers.get('Event', ''),
                'Site': headers.get('Site', ''),
                'Date': headers.get('Date', ''),
                'Round': headers.get('Round', ''),
                'White': headers.get('White', ''),
                'Black': headers.get('Black', ''),
                'Result': headers.get('Result', ''),
                'UTCDate': headers.get('UTCDate', ''),
                'UTCTime': headers.get('UTCTime', ''),
                'WhiteElo':headers.get('WhiteElo', ''), 
                'BlackElo':headers.get('BlackElo', ''),
                'WhiteRatingDiff': headers.get('WhiteRatingDiff', ''),
                'BlackRatingDiff': headers.get('BlackRatingDiff', ''),
                'TimeControl': headers.get('TimeControl', ''),
                'Termination': headers.get('Termination', ''),
                'Variant':headers.get('Variant', ''),
                'Moves': ' '.join(str(move) for move in game.mainline_moves())
            }
            games_data.append(game_info)  # Separator for readability
        
    return games_data

def main():
    data_dir = 'data'
    file_name = 'lichess_db_antichess_rated_2024-09.pgn'
    pgn_file_path = os.path.join(data_dir, file_name)
    
    games_data = read_pgn_to_dataframe(pgn_file_path)
    print(pd.DataFrame(games_data))
    print(games_data)

if __name__ == "__main__":
    main()
