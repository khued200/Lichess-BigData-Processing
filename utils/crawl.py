import os
import requests
import zstandard as zstd

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status() 
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def decompress_zst_file(zst_file, output_pgn_file):
    with open(zst_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with open(output_pgn_file, 'wb') as decompressed_file:
            dctx.copy_stream(compressed_file, decompressed_file)

url = "https://database.lichess.org/antichess/lichess_db_antichess_rated_2024-09.pgn.zst"

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

zst_file = os.path.join(data_dir, "lichess_db_antichess_rated_2024-09.pgn.zst")
pgn_file = os.path.join(data_dir, "lichess_db_antichess_rated_2024-09.pgn")

download_file(url, zst_file)

decompress_zst_file(zst_file, pgn_file)

print("Download file successed...")