from functools import cached_property
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from pydantic import BaseModel, Field


class DuckUtils(BaseModel):
    """
    Lightweight helper for common DuckDB operations and queries.

    This class provides utilities for DuckDB database management,
    including table inspection and metadata retrieval.

    Examples
    --------
    >>> # In-memory database
    >>> duck = DuckUtils()
    >>> # Persistent database
    >>> duck = DuckUtils(db_file_path=Path('my_db.duckdb'))
    """

    db_file_path: Optional[Path] = Field(
        default=None,
        description="Path to the DuckDB database file. If None, an in-memory database is used.",
    )

    model_config = {"arbitrary_types_allowed": True}

    @cached_property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """
        Lazily initialized DuckDB connection.

        Returns
        -------
        duckdb.DuckDBPyConnection
            A connection to either the file-based database specified by
            db_file_path or to an in-memory database if no path is provided.
        """
        return (
            duckdb.connect(str(self.db_file_path))
            if self.db_file_path
            else duckdb.connect()
        )

    def get_table_metadata(
        self, schema_names: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """
        Get detailed metadata for tables in the specified schemas.

        Parameters
        ----------
        schema_names : list[str] | None
            If None or empty: all schemas with tables are scanned.
            Otherwise: only the specified schemas are scanned (case-sensitive).

        Returns
        -------
        pd.DataFrame
            DataFrame with table metadata from duckdb_tables() system table.
            Empty DataFrame if no tables are found.

        Notes
        -----
        Uses the DuckDB system table function 'duckdb_tables()' to retrieve metadata.
        """
        # Create WHERE clause if schemas are specified
        where_clause = ""
        if schema_names and len(schema_names) > 0:
            placeholders = ", ".join(["?"] * len(schema_names))
            where_clause = f"WHERE schema_name IN ({placeholders})"

        # Construct the complete query
        query = f"""
        SELECT * 
        FROM duckdb_tables() 
        {where_clause}
        ORDER BY schema_name, table_name
        """

        # Execute the query and return a DataFrame
        return self.conn.execute(query, schema_names if schema_names else []).df()

    # ──────────────────────────────────────────────────────────────────────────
    def get_table_row_counts(
        self, schema_names: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """
        Get row counts for tables in the specified schemas.

        Parameters
        ----------
        schema_names : list[str] | None
            If None or empty: all schemas with tables are scanned.
            Otherwise: only the specified schemas are scanned (case-sensitive).

        Returns
        -------
        pd.DataFrame
            DataFrame with columns "Table" (schema.table_name) and "Row Count".
            Empty DataFrame if no tables are found.

        Notes
        -----
        Uses get_table_metadata() and the estimated_size column from duckdb_tables().
        """
        # Create empty DataFrame structure for potential empty returns
        empty_df = pd.DataFrame(columns=["Table", "Row Count"])

        # Get table metadata to identify all tables
        metadata_df = self.get_table_metadata(schema_names)

        # If no tables found, return empty DataFrame
        if metadata_df.empty:
            return empty_df

        # Create result DataFrame directly from metadata
        result_df = pd.DataFrame(
            {
                "Table": metadata_df["schema_name"] + "." + metadata_df["table_name"],
                "Row Count": metadata_df["estimated_size"],
            }
        )

        # Filter out temporary tables to match original behavior
        result_df = result_df[~metadata_df["temporary"]]

        return result_df

    # ──────────────────────────────────────────────────────────────────────────
    def display_table_row_counts(
        self, schema_names: Optional[list[str]] = None
    ) -> None:
        """
        Print formatted table row counts to the console.

        A convenience wrapper around get_table_row_counts() that
        displays results as a Markdown table.

        Parameters
        ----------
        schema_names : list[str] | None
            If None or empty: all schemas with tables are scanned.
            Otherwise: only the specified schemas are scanned (case-sensitive).

        Examples
        --------
        >>> duck = DuckUtils()
        >>> # Display counts for all tables
        >>> duck.display_table_row_counts()
        >>> # Display counts only for 'main' schema
        >>> duck.display_table_row_counts(['main'])
        """
        df = self.get_table_row_counts(schema_names)

        if df.empty:
            schema_desc = (
                "all schemas"
                if not schema_names
                else f"schema(s) {', '.join(schema_names or [])}"
            )
            print(f"ⓘ no tables found in {schema_desc}")
            return

        print(df.to_markdown(index=False, tablefmt="simple_outline"))
