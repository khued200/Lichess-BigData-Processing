import os
import requests
import zstandard as zstd

def download_file(url, local_filename):
    # Gửi yêu cầu HTTP GET để tải tệp từ URL
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Kiểm tra lỗi kết nối
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def decompress_zst_file(zst_file, output_pgn_file):
    # Giải nén tệp .zst
    with open(zst_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with open(output_pgn_file, 'wb') as decompressed_file:
            dctx.copy_stream(compressed_file, decompressed_file)

def read_pgn_file(pgn_file):
    # Đọc nội dung từ file PGN đã giải nén
    with open(pgn_file, 'r', encoding='utf-8') as file:
        for i in range(5):  # Đọc 5 ván đầu tiên làm ví dụ
            game = file.readline()
            print(game)

# Đường dẫn URL tới file .pgn.zst
url = "https://database.lichess.org/antichess/lichess_db_antichess_rated_2024-09.pgn.zst"

# Tên tệp tạm thời cho tệp đã tải và tệp đã giải nén
zst_file = "lichess_db_antichess_rated_2024-09.pgn.zst"
pgn_file = "lichess_db_antichess_rated_2024-09.pgn"

# Bước 1: Tải file .pgn.zst từ URL
download_file(url, zst_file)

# Bước 2: Giải nén file .zst thành file .pgn
decompress_zst_file(zst_file, pgn_file)

# Bước 3: Đọc và xử lý file .pgn
read_pgn_file(pgn_file)
