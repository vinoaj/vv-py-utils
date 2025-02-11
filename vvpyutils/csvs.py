from pathlib import Path

import pandas as pd


def combine_csvs_to_df(csvs: list[Path], output_path: Path) -> pd.DataFrame:
    combined_df = pd.concat([pd.read_csv(csv) for csv in csvs])
    combined_df.to_csv(output_path, index=False)
    return combined_df
