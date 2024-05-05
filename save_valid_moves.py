import pandas as pd
import polars as pl
import gzip
from tqdm import tqdm

out_dir_path = "C:/Users/aronn/OneDrive/Asztali gép/Rajk/Prog 2/moves_with_valid_id"
m_path = "C:/Users/aronn/OneDrive/Asztali gép/Rajk/Prog 2/moves.csv.gz"
g_path = "C:/Users/aronn/OneDrive/Asztali gép/Rajk/Prog 2/games.csv.gz"
w_file_count = 0
# Create empty df for aggregating valid moves
agg_valid_moves = pl.DataFrame(
    {
        "gameId": "0",
        "moveNo": "0",
        "move": "0",
        "clock": "0",
    }
).clear()


games = pl.read_csv(g_path, columns="gameId")


with gzip.open(m_path, "rt") as m_f:
    reader = pd.read_csv(m_f, chunksize=10_000_000, iterator=True)

    for chunk in tqdm(reader, desc="Processing batches"):
        m_chunk = pl.DataFrame(chunk)

        joined_df = m_chunk.join(games, on="gameId", how="inner")
        # print(joined_df)

        agg_valid_moves = agg_valid_moves.vstack(joined_df)
        # print(agg_valid_moves)

        if len(agg_valid_moves) > 10_000_000:
            parq_path = out_dir_path + f"/file_{w_file_count}.parquet"
            agg_valid_moves.write_parquet(parq_path, compression="lz4")
            agg_valid_moves = agg_valid_moves.clear()
            w_file_count += 1
