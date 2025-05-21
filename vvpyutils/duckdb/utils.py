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
        Uses information_schema.tables for metadata and COUNT(*) for row counts.
        """
        # Create empty DataFrame structure for potential empty returns
        empty_df = pd.DataFrame(columns=["Table", "Row Count"])

        # If schema_names not provided, get all schemas with base tables
        if not schema_names:
            schema_names = [
                s
                for (s,) in self.conn.execute(
                    """
                    SELECT DISTINCT table_schema
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                    ORDER BY table_schema
                    """
                ).fetchall()
            ]

            if not schema_names:
                return empty_df

        # Get all table row counts
        rows = []
        for schema in schema_names:
            tables = [
                t
                for (t,) in self.conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    [schema],
                ).fetchall()
            ]

            for table in tables:
                count = self.conn.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                ).fetchone()[0]
                rows.append((f"{schema}.{table}", count))

        return (
            empty_df if not rows else pd.DataFrame(rows, columns=["Table", "Row Count"])
        )

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

        print(df.to_markdown(index=False))
