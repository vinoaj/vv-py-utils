import json
from collections.abc import Iterable
from pathlib import Path
from typing import ClassVar, Literal, Optional

import duckdb
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field


class DuckUtils(BaseModel):
    """
    Lightweight helper for common DuckDB operations and queries.

    Provides utilities for DuckDB database management, including table inspection and metadata retrieval.
    """

    conn: Optional[duckdb.DuckDBPyConnection] = None
    db_file_path: Optional[Path] = Field(
        default=None,
        description="Path to the DuckDB database file. If None, an in-memory database is used.",
    )

    _PROMPT_PREFIX: ClassVar[str] = (
        "You have access to the following tables. If you require any data from them, provide the SQL query to extract the data."
    )

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, **data):
        super().__init__(**data)
        if not self.conn:
            self.conn = self._create_conn()

    def _create_conn(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection (in-memory or file-based)."""
        return (
            duckdb.connect(str(self.db_file_path))
            if self.db_file_path
            else duckdb.connect()
        )

    def _ensure_list(self, value: Optional[Iterable[str]]) -> Optional[list[str]]:
        """
        Convert an iterable to a list if it's not None, otherwise return None.

        This is a utility method to standardize parameter handling across the class.

        Parameters
        ----------
        value : Iterable[str] or None
            The iterable to convert to a list. If None, returns None.

        Returns
        -------
        list[str] or None
            The input converted to a list, or None if the input was None.
        """
        return list(value) if value is not None else None

    def get_tables(
        self,
        schema_names: Optional[Iterable[str]] = None,
        table_names: Optional[Iterable[str]] = None,
        *,
        include_columns: bool = False,
        include_constraints: bool = False,
    ) -> pd.DataFrame:
        """
        Get table metadata with flexible detail levels.

        This method provides a unified interface for retrieving table metadata,
        with options for including column details and constraints.

        Parameters
        ----------
        schema_names : Iterable[str] or None
            Iterable of schema names to filter by. If None, all schemas are included.
            Any iterable of strings (list, tuple, set, etc.) is accepted.
        table_names : Iterable[str] or None
            Iterable of table names to filter by. If None, all tables are included.
            Any iterable of strings (list, tuple, set, etc.) is accepted.
        include_columns : bool, default False
            Whether to include column details in the result. If True, returns detailed
            column-level information. If False, returns only table-level information.
        include_constraints : bool, default False
            Whether to include constraint information (primary keys, etc.).
            Only applies when include_columns is True.

        Returns
        -------
        pd.DataFrame
            DataFrame with metadata at the requested detail level.
            Empty DataFrame if no tables are found.

        Notes
        -----
        This method consolidates the functionality of `_catalog` and `get_table_metadata`
        into a single, more flexible interface.
        """
        detail_level = "column" if include_columns else "table"

        return self.get_metadata(
            schema_names=schema_names,
            table_names=table_names,
            detail_level=detail_level,
            include_constraints=include_constraints and include_columns,
        )

    def display_table_row_counts(
        self, schema_names: Optional[Iterable[str]] = None
    ) -> None:
        """
        Print formatted table row counts to the console as a Markdown table.

        Parameters
        ----------
        schema_names : Iterable[str] or None
            If None or empty, all schemas with tables are scanned. Otherwise, only the specified schemas are scanned (case-sensitive).
            Any iterable of strings (list, tuple, set, etc.) is accepted.

        Examples
        --------
        >>> duck = DuckUtils()
        >>> # Display counts for all tables
        >>> duck.display_table_row_counts()
        >>> # Display counts only for 'main' schema
        >>> duck.display_table_row_counts(['main'])
        >>> # Using a tuple or set also works
        >>> duck.display_table_row_counts(('main', 'temp'))
        """
        # Get table metadata using the consolidated method
        raw_metadata_df = self.get_tables(schema_names=schema_names)

        if not raw_metadata_df.empty:
            raw_metadata_df = raw_metadata_df[~raw_metadata_df["temporary"]]

        if raw_metadata_df.empty:
            print(f"ⓘ no tables found in {schema_names}")
            return

        display_df = pd.DataFrame(
            {
                "Table": raw_metadata_df["schema_name"]
                + "."
                + raw_metadata_df["table_name"],
                "Row Count": raw_metadata_df["estimated_size"],
            }
        )

        # Format row counts with thousand separators
        display_df["Row Count"] = display_df["Row Count"].apply(
            lambda x: format(x, ",")
        )

        colalign = ["left", "right"]
        print(
            display_df.to_markdown(
                index=False, tablefmt="simple_outline", colalign=colalign
            )
        )

    def get_llm_prompt(
        self,
        schema_names: Optional[Iterable[str]] = None,
        table_names: Optional[Iterable[str]] = None,
        *,
        include_row_counts: bool = True,
        include_constraints: bool = True,
        max_tables: int | None = None,
        max_columns: int | None = None,
        format_: Literal["markdown", "json"] = "markdown",
        prompt_prefix: Optional[str] = None,
    ) -> str:
        """Build an LLM-friendly catalogue of available tables/columns.

        The output is either GitHub-style Markdown or a JSON blob.  Row counts
        now appear in the header (not per column); per-column *Comment* has
        been added, and PK flags remain optional.
        """

        prefix = prompt_prefix or self._PROMPT_PREFIX

        # ── 1. catalogue scan ────────────────────────────────────────────
        # Get table metadata with columns and constraints using the consolidated method
        df = self.get_tables(
            schema_names=schema_names,
            table_names=table_names,
            include_columns=True,
            include_constraints=include_constraints,
        )

        if df.empty:
            return f"{prefix}\n\n_No tables matched your filters._"

        # ── 2. build output per table ────────────────────────────────────
        result_sections: list[str] = []
        n_total = df.groupby(["schema_name", "table_name"]).ngroups

        for idx, ((schema, table), grp) in enumerate(
            df.groupby(["schema_name", "table_name"]), start=1
        ):
            if max_tables and idx > max_tables:
                result_sections.append(f"*…{n_total - max_tables} more tables omitted*")
                break

            # truncate long column lists
            if max_columns and len(grp) > max_columns:
                grp = grp.head(max_columns).copy()

                # Create a placeholder row more robustly
                if not grp.empty:
                    # Start with a copy of the last row to ensure column structure is identical
                    placeholder = grp.iloc[-1:].copy()

                    # Update placeholder values
                    placeholder["column_index"] = None
                    placeholder["column_name"] = "…"

                    # Set other column values where they exist
                    for col in placeholder.columns:
                        if col == "column_comment":
                            placeholder[col] = "…"
                        elif col == "data_type":
                            placeholder[col] = "…"
                        # Keep existing values for schema-level and table-level fields

                    # Append placeholder row
                    grp = pd.concat([grp, placeholder], ignore_index=True)

            # ── column table ----------------------------------------------------
            cols = pd.DataFrame(
                {
                    "Column": grp["column_name"],
                    "Comment": grp["column_comment"],
                    "Type": grp["data_type"],
                }
            )

            if include_constraints:
                # Check if pk column exists and handle primary key information
                pk_set: set[str] = set()

                if "pk" in grp.columns and len(grp) > 0:
                    pk_raw = grp["pk"].iloc[0]

                    if pk_raw is not None:
                        # Handle both list and numpy.ndarray types
                        if hasattr(pk_raw, "__iter__") and not isinstance(pk_raw, str):
                            # Convert to Python list if it's a NumPy array or other iterable
                            pk_list = list(pk_raw)
                            pk_set = set(pk_list)

                # Add PK column and mark primary key columns with checkmark
                cols.loc[:, "PK"] = ""

                # Mark primary key columns
                for col_name in pk_set:
                    # Convert both to string to ensure matching
                    mask = cols["Column"].astype(str) == str(col_name)
                    if mask.any():
                        cols.loc[mask, "PK"] = "✓"

            # ── header ---------------------------------------------------------
            rows = int(grp["row_count"].iloc[0]) if include_row_counts else 0
            tbl_comment = grp["table_comment"].iloc[0] or ""
            header_md = f"Table: `{schema}.{table}` ({rows:,} rows)"
            if tbl_comment:
                header_md += f"\n{tbl_comment}"

            # ── render ----------------------------------------------------------
            if format_ == "markdown":
                result_sections.append(
                    header_md + "\n" + cols.to_markdown(index=False, tablefmt="github")
                )
            else:
                result_sections.append(
                    json.dumps(
                        {
                            "schema": schema,
                            "table": table,
                            "rows": rows,
                            "comment": tbl_comment,
                            "columns": cols.to_dict(orient="records"),
                        },
                        indent=2,
                    )
                )

        body = "\n\n".join(result_sections)
        return f"{prefix}\n\n{body}"

    def _build_where_clause(
        self, conditions: dict[str, Optional[Iterable[str]]], table_alias: str = ""
    ) -> tuple[str, list]:
        """
        Build a SQL WHERE clause from conditions.

        Parameters
        ----------
        conditions : dict
            Dictionary of column name to iterable of values. If values is None or empty, the condition is ignored.
        table_alias : str, optional
            Table alias to prefix column names with, by default "".

        Returns
        -------
        tuple[str, list]
            A tuple of (where_clause, parameters) for use in SQL queries.
        """
        where_parts = []
        params = []

        prefix = f"{table_alias}." if table_alias else ""

        for col, values in conditions.items():
            values_list = self._ensure_list(values)
            if values_list:
                placeholders = ", ".join(["?"] * len(values_list))
                where_parts.append(f"{prefix}{col} IN ({placeholders})")
                params.extend(values_list)

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        return where_clause, params

    def get_metadata(
        self,
        schema_names: Optional[Iterable[str]] = None,
        table_names: Optional[Iterable[str]] = None,
        *,
        detail_level: Literal["table", "column"] = "table",
        include_constraints: bool = True,
    ) -> pd.DataFrame:
        """
        Get metadata for tables and columns with flexible detail levels.

        This is the core metadata retrieval method that supports both table-level
        and column-level detail, with optional constraints information.

        Parameters
        ----------
        schema_names : Iterable[str] or None
            Iterable of schema names to filter by. If None, all schemas are included.
            Any iterable of strings (list, tuple, set, etc.) is accepted.
        table_names : Iterable[str] or None
            Iterable of table names to filter by. If None, all tables are included.
            Any iterable of strings (list, tuple, set, etc.) is accepted.
        detail_level : Literal["table", "column"], default "table"
            Level of detail to return:
            - "table": Only table-level metadata (faster, less detail)
            - "column": Table and column-level metadata (slower, more detail)
        include_constraints : bool, default False
            Whether to include constraint information (primary keys, etc.)
            Only applies when detail_level is "column".

        Returns
        -------
        pd.DataFrame
            DataFrame with metadata at the requested detail level.
            Empty DataFrame if no tables are found.
        """
        # Convert iterables to lists if they're not None
        schema_list = self._ensure_list(schema_names)
        table_list = self._ensure_list(table_names)

        if detail_level == "table":
            # Table-level metadata (simple query)
            if table_list:
                # If table names are provided, we need a more complex query to filter by both schema and table
                conditions = {"schema_name": schema_list, "table_name": table_list}
                where_clause, params = self._build_where_clause(conditions)
            else:
                # Simple schema filtering if no table names provided
                conditions = {"schema_name": schema_list}
                where_clause, params = self._build_where_clause(conditions)

            query = f"""
            SELECT * 
            FROM duckdb_tables() 
            {where_clause}
            ORDER BY schema_name, table_name
            """

            return self.conn.execute(query, params).df()
        else:
            # Column-level metadata (detailed query)
            conditions = {"schema_name": schema_list, "table_name": table_list}
            where_clause, params = self._build_where_clause(conditions, "ft")

            sql = f"""
                WITH filtered_tables AS (
                    SELECT * FROM duckdb_tables() ft
                    {where_clause}
                )
                SELECT  dc.schema_name,
                        dc.table_name,
                        dc.column_index,
                        dc.column_name,
                        dc.comment                       AS column_comment,
                        dc.data_type,
                        isc.column_default,
                        (isc.column_default IS NOT NULL) AS has_default,
                        isc.is_nullable = 'YES'          AS is_nullable,
                        ft.estimated_size                AS row_count,
                        ft.comment                       AS table_comment
                FROM filtered_tables ft
                JOIN duckdb_columns() dc
                    USING (schema_name, table_name)
                JOIN information_schema.columns isc
                    ON  isc.table_schema = dc.schema_name
                    AND isc.table_name   = dc.table_name
                    AND isc.column_name  = dc.column_name
                ORDER BY dc.schema_name, dc.table_name, dc.column_index
            """

            result_df = self.conn.execute(sql, params).df()

            # Add constraint information if requested
            if include_constraints and not result_df.empty:
                # First get the primary key information
                pk_df = self.conn.execute(
                    """
                    SELECT schema_name,
                           table_name,
                           constraint_column_names AS pk
                    FROM duckdb_constraints()
                    WHERE constraint_type = 'PRIMARY KEY'
                    """
                ).df()

                # Debug the constraint information
                # Merge with main dataframe
                result_df = result_df.merge(
                    pk_df, on=["schema_name", "table_name"], how="left"
                )

            return result_df
