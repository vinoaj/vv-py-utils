import pandas as pd
from agentsynthpanel.config.env import SQLLITE_URL
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils.bigquery.pipeline.config import JSONL_FOLDER

load_dotenv()


engine = create_engine(SQLLITE_URL)
EXCLUDED_TABLES = ["sqlite_sequence", "alembic_version"]


def export_to_jsonl():
    import json

    jsonl_folder = str(JSONL_FOLDER)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", engine)

    def is_simple_array(x):
        """Check if x is a list/array of simple types (str, int, float, bool, None)"""
        if not isinstance(x, list):
            return False
        simple_types = (str, int, float, bool, type(None))
        return all(isinstance(item, simple_types) for item in x)

    for table in tables["name"]:
        if table not in EXCLUDED_TABLES:
            df = pd.read_sql_table(table, engine)

            # Process JSON columns - stringify complex objects but preserve simple arrays
            for column in df.columns:
                # Check if the column contains dict or list objects
                if df[column].dtype == "object":
                    # Create mask for items that should be stringified
                    # (dicts or complex lists, but not simple arrays)
                    mask = df[column].apply(
                        lambda x: isinstance(x, dict)
                        or (isinstance(x, list) and not is_simple_array(x))
                    )

                    if mask.any():
                        df.loc[mask, column] = df.loc[mask, column].apply(
                            lambda x: json.dumps(x)
                        )

            df.to_json(
                f"{jsonl_folder}/{table}.jsonl",
                orient="records",
                lines=True,
                date_format="iso",
            )


if __name__ == "__main__":
    export_to_jsonl()
