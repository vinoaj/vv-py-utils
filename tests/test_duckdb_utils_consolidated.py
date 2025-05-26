"""
Consolidated tests for the DuckDB utilities functionality.

This file consolidates tests from:
- test_duckdb_utils.py
- test_duckdb_utils_refactored.py
- test_duckdb_utils_views.py
"""

import duckdb
import pandas as pd
import pytest

from vvpyutils.duckdb.utils import DuckUtils

#################################################
# Fixtures - Basic DB Setup
#################################################


@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database file path."""
    db_path = tmp_path / "test_db.duckdb"
    return db_path


@pytest.fixture
def setup_test_data(temp_db_path):
    """Set up a test database with known schemas and tables."""
    # Create a connection to populate the database
    conn = duckdb.connect(str(temp_db_path))

    # Create schemas and tables with test data
    conn.execute("CREATE SCHEMA test_schema1")
    conn.execute("CREATE SCHEMA test_schema2")
    conn.execute("CREATE SCHEMA empty_schema")

    # Create schema with special characters
    conn.execute('CREATE SCHEMA "special-schema"')

    # Create tables in test_schema1
    conn.execute("""
        CREATE TABLE test_schema1.table1 (id INTEGER, name VARCHAR)
    """)
    conn.execute("""
        INSERT INTO test_schema1.table1 VALUES 
        (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
    """)

    conn.execute("""
        CREATE TABLE test_schema1.table2 (id INTEGER, value DOUBLE)
    """)
    conn.execute("""
        INSERT INTO test_schema1.table2 VALUES 
        (1, 10.5), (2, 20.5)
    """)

    # Create tables in test_schema2
    conn.execute("""
        CREATE TABLE test_schema2.table3 (id INTEGER, active BOOLEAN)
    """)
    conn.execute("""
        INSERT INTO test_schema2.table3 VALUES 
        (1, TRUE), (2, FALSE), (3, TRUE), (4, TRUE), (5, FALSE)
    """)

    # Create table with dashes in name
    conn.execute("""
        CREATE TABLE "special-schema"."table-with-dashes" (id INTEGER, notes VARCHAR)
    """)
    conn.execute("""
        INSERT INTO "special-schema"."table-with-dashes" VALUES
        (1, 'Note 1'), (2, 'Note 2'), (3, 'Note 3'), (4, 'Note 4')
    """)

    conn.close()
    return temp_db_path


#################################################
# Fixtures - Tables and Views
#################################################


@pytest.fixture
def temp_db_with_tables_and_views(tmp_path):
    """Create a temporary database with tables and views for testing."""
    # Create test database file
    db_path = tmp_path / "test_db.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create test schema and tables
    conn.execute("CREATE SCHEMA test_schema")
    conn.execute("""
        CREATE TABLE test_schema.test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR
        )
    """)
    conn.execute("INSERT INTO test_schema.test_table VALUES (1, 'Test')")

    # Create a view
    conn.execute("""
        CREATE VIEW test_schema.test_view AS
        SELECT * FROM test_schema.test_table
    """)

    conn.close()
    return db_path


@pytest.fixture
def temp_db_with_views(tmp_path):
    """Set up a test database with tables and views for testing."""
    db_path = tmp_path / "test_db_views.duckdb"

    # Create a connection and populate with test data
    conn = duckdb.connect(str(db_path))

    # Create schemas
    conn.execute("CREATE SCHEMA test_schema")

    # Create tables
    conn.execute("""
        CREATE TABLE test_schema.users (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO test_schema.users VALUES 
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (3, 'Charlie', 'charlie@example.com')
    """)

    conn.execute("""
        CREATE TABLE test_schema.orders (
            id INTEGER,
            user_id INTEGER,
            amount DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO test_schema.orders VALUES 
        (101, 1, 25.50),
        (102, 1, 15.75),
        (103, 2, 45.25),
        (104, 3, 10.00)
    """)

    # Create views
    conn.execute("""
        CREATE VIEW test_schema.user_summary AS
        SELECT id, name, email FROM test_schema.users
    """)

    conn.execute("""
        CREATE VIEW test_schema.order_details AS
        SELECT o.id as order_id, u.name as user_name, o.amount
        FROM test_schema.orders o
        JOIN test_schema.users u ON o.user_id = u.id
    """)

    # Create a schema with only views
    conn.execute("CREATE SCHEMA views_only_schema")
    conn.execute("""
        CREATE VIEW views_only_schema.all_users AS
        SELECT * FROM test_schema.users
    """)

    conn.close()

    return db_path


#################################################
# Connection Tests
#################################################


def test_in_memory_connection():
    """Test DuckUtils with in-memory connection."""
    duck_utils = DuckUtils()
    # Verify connection is successful
    result = duck_utils.conn.execute("SELECT 1").fetchone()[0]
    assert result == 1


def test_file_connection(setup_test_data):
    """Test DuckUtils with a file-based connection."""
    db_path = setup_test_data
    duck_utils = DuckUtils(db_file_path=db_path)

    # Verify connection is successful and can access test data
    result = duck_utils.conn.execute(
        "SELECT COUNT(*) FROM test_schema1.table1"
    ).fetchone()[0]
    assert result == 3


#################################################
# Helper Method Tests
#################################################


def test_build_entity_conditions():
    """Test the _build_entity_conditions helper method."""
    duck_utils = DuckUtils()

    # Test table conditions with schema and table filters
    table_conditions = duck_utils._build_entity_conditions(
        "table", ["schema1", "schema2"], ["table1"]
    )
    assert "schema_name" in table_conditions
    assert "table_name" in table_conditions
    assert table_conditions["schema_name"] == ["schema1", "schema2"]
    assert table_conditions["table_name"] == ["table1"]

    # Test view conditions with both filters
    view_conditions = duck_utils._build_entity_conditions(
        "view", ["schema1"], ["view1", "view2"]
    )
    assert "schema_name" in view_conditions
    assert "view_name" in view_conditions
    assert view_conditions["schema_name"] == ["schema1"]
    assert view_conditions["view_name"] == ["view1", "view2"]

    # Test with only schema filter
    schema_only = duck_utils._build_entity_conditions("table", ["schema1"], None)
    assert "schema_name" in schema_only
    assert "table_name" not in schema_only
    assert schema_only["schema_name"] == ["schema1"]

    # Test with only table filter
    table_only = duck_utils._build_entity_conditions("view", None, ["view1"])
    assert "schema_name" not in table_only
    assert "view_name" in table_only
    assert table_only["view_name"] == ["view1"]

    # Test with no filters
    no_filters = duck_utils._build_entity_conditions("table", None, None)
    assert len(no_filters) == 0


def test_filter_system_views():
    """Test the _filter_system_views helper method."""
    duck_utils = DuckUtils()

    # Create a test DataFrame with system and user views
    df = pd.DataFrame(
        {
            "schema_name": [
                "information_schema",
                "test_schema",
                "pg_catalog",
                "user_schema",
                "main",
            ],
            "table_name": ["view1", "table1", "view2", "view3", "view4"],
            "object_type": ["view", "table", "view", "view", "view"],
        }
    )

    # Filter system views
    filtered_df = duck_utils._filter_system_views(df)

    # Check that system schemas are removed for views but tables remain
    assert len(filtered_df) == 2  # Should keep the table and user view
    assert (
        "information_schema"
        not in filtered_df[filtered_df["object_type"] == "view"]["schema_name"].values
    )
    assert (
        "pg_catalog"
        not in filtered_df[filtered_df["object_type"] == "view"]["schema_name"].values
    )
    assert (
        "main"
        not in filtered_df[filtered_df["object_type"] == "view"]["schema_name"].values
    )
    assert "test_schema" in filtered_df["schema_name"].values  # Should keep the table
    assert (
        "user_schema" in filtered_df["schema_name"].values
    )  # Should keep the user view

    # Test with empty DataFrame
    empty_df = pd.DataFrame(columns=["schema_name", "table_name", "object_type"])
    assert duck_utils._filter_system_views(empty_df).empty


def test_combine_and_process_results():
    """Test the _combine_and_process_results helper method."""
    duck_utils = DuckUtils()

    # Create test DataFrames for tables and views
    tables_df = pd.DataFrame(
        {
            "schema_name": ["schema1", "schema2"],
            "table_name": ["table1", "table2"],
            "object_type": ["table", "table"],
            "column_index": [1, 1],
        }
    )

    views_df = pd.DataFrame(
        {
            "schema_name": ["schema1", "information_schema"],
            "table_name": ["view1", "system_view"],
            "object_type": ["view", "view"],
            "column_index": [1, 1],
        }
    )

    # Test combining with views and including system views
    combined_df = duck_utils._combine_and_process_results(
        tables_df, views_df, include_system_views=True
    )
    assert len(combined_df) == 4  # All tables and views
    schemas = set(combined_df["schema_name"])
    assert "schema1" in schemas
    assert "schema2" in schemas
    assert "information_schema" in schemas

    # Test combining with views but excluding system views
    filtered_df = duck_utils._combine_and_process_results(
        tables_df, views_df, include_system_views=False
    )
    assert len(filtered_df) == 3  # All tables and non-system views
    schemas = set(filtered_df["schema_name"])
    assert "schema1" in schemas
    assert "schema2" in schemas
    assert "information_schema" not in schemas

    # Test without views
    tables_only_df = duck_utils._combine_and_process_results(
        tables_df, None, include_system_views=True
    )
    assert len(tables_only_df) == 2  # Only tables
    assert all(tables_only_df["object_type"] == "table")

    # Test sorting
    sorted_df = duck_utils._combine_and_process_results(
        tables_df, views_df, sort_by=["table_name"]
    )
    assert sorted_df.iloc[0]["table_name"] == "system_view"
    assert sorted_df.iloc[1]["table_name"] == "table1"
    assert sorted_df.iloc[2]["table_name"] == "table2"
    assert sorted_df.iloc[3]["table_name"] == "view1"


def test_build_where_clause_single_condition():
    """Test building WHERE clause with a single condition."""
    duck_utils = DuckUtils()
    where_clause, params = duck_utils._build_where_clause({"column": ["value"]})
    assert where_clause == "WHERE column IN (?)"
    assert params == ["value"]


def test_build_where_clause_multiple_conditions():
    """Test building WHERE clause with multiple conditions."""
    duck_utils = DuckUtils()
    where_clause, params = duck_utils._build_where_clause(
        {"col1": ["val1", "val2"], "col2": ["val3"]}
    )
    assert "col1 IN (?, ?)" in where_clause
    assert "col2 IN (?)" in where_clause
    assert "AND" in where_clause
    assert set(params) == {"val1", "val2", "val3"}


def test_build_where_clause_with_alias():
    """Test building WHERE clause with table alias."""
    duck_utils = DuckUtils()
    where_clause, params = duck_utils._build_where_clause({"column": ["value"]}, "t")
    assert where_clause == "WHERE t.column IN (?)"
    assert params == ["value"]


def test_build_where_clause_empty_conditions():
    """Test building WHERE clause with empty conditions."""
    duck_utils = DuckUtils()
    where_clause, params = duck_utils._build_where_clause({})
    assert where_clause == ""
    assert params == []


def test_ensure_list_conversion():
    """Test _ensure_list utility method."""
    duck_utils = DuckUtils()

    # Test with list input
    assert duck_utils._ensure_list(["a", "b"]) == ["a", "b"]

    # Test with tuple input
    assert duck_utils._ensure_list(("a", "b")) == ["a", "b"]

    # Test with set input
    assert set(duck_utils._ensure_list({"a", "b"})) == {"a", "b"}

    # Test with None input
    assert duck_utils._ensure_list(None) is None


#################################################
# Table Row Count Display Tests
#################################################


def test_display_all_schemas(setup_test_data, capsys):
    """Test displaying all schemas and their table counts."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method that prints to console
    duck_utils.display_table_row_counts()

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify all schemas are included in the output
    assert "test_schema1.table1" in output
    assert "test_schema1.table2" in output
    assert "test_schema2.table3" in output
    assert "special-schema.table-with-dashes" in output

    # Verify row counts are displayed correctly
    assert "3" in output  # table1 has 3 rows
    assert "2" in output  # table2 has 2 rows
    assert "5" in output  # table3 has 5 rows
    assert "4" in output  # table-with-dashes has 4 rows


def test_display_specific_schemas(setup_test_data, capsys):
    """Test displaying only specified schemas."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with specific schemas
    duck_utils.display_table_row_counts(["test_schema1"])

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify only test_schema1 tables are included
    assert "test_schema1.table1" in output
    assert "test_schema1.table2" in output
    assert "test_schema2.table3" not in output
    assert "special-schema.table-with-dashes" not in output


def test_display_empty_schema(setup_test_data, capsys):
    """Test displaying an empty schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with an empty schema
    duck_utils.display_table_row_counts(["empty_schema"])

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify appropriate message is displayed
    assert "ⓘ no tables found in ['empty_schema']" in output


def test_display_nonexistent_schema(setup_test_data, capsys):
    """Test displaying a nonexistent schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with a nonexistent schema
    duck_utils.display_table_row_counts(["nonexistent_schema"])

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify appropriate message is displayed
    assert "ⓘ no tables found in ['nonexistent_schema']" in output


def test_empty_database(tmp_path, capsys):
    """Test with a completely empty database."""
    # Create an empty database file
    empty_db_path = tmp_path / "empty_db.duckdb"
    duck_utils = DuckUtils(db_file_path=empty_db_path)

    # Call the display method
    duck_utils.display_table_row_counts()

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify appropriate message is displayed
    assert "ⓘ no tables found in None" in output


#################################################
# Metadata Retrieval Tests - Basic
#################################################


def test_get_all_schemas_metadata(setup_test_data):
    """Test getting metadata for all schemas."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method that returns a DataFrame
    df = duck_utils.get_tables()

    # Verify DataFrame contains the essential columns
    essential_columns = ["schema_name", "table_name", "estimated_size", "temporary"]
    for col in essential_columns:
        assert col in df.columns

    # Verify all schemas are included
    schema_tables = [
        f"{row['schema_name']}.{row['table_name']}" for _, row in df.iterrows()
    ]
    assert "test_schema1.table1" in schema_tables
    assert "test_schema1.table2" in schema_tables
    assert "test_schema2.table3" in schema_tables
    assert "special-schema.table-with-dashes" in schema_tables

    # Verify row counts are correct
    table_counts = {}
    for _, row in df.iterrows():
        schema_table = f"{row['schema_name']}.{row['table_name']}"
        table_counts[schema_table] = row["estimated_size"]

    assert table_counts["test_schema1.table1"] == 3
    assert table_counts["test_schema1.table2"] == 2
    assert table_counts["test_schema2.table3"] == 5
    assert table_counts["special-schema.table-with-dashes"] == 4


def test_get_specific_schemas_metadata(setup_test_data):
    """Test getting metadata for specific schemas."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with specific schemas
    df = duck_utils.get_tables(schema_names=["test_schema1"])

    # Verify DataFrame contains the essential columns
    essential_columns = ["schema_name", "table_name", "estimated_size", "temporary"]
    for col in essential_columns:
        assert col in df.columns

    # Verify only test_schema1 tables are included
    schema_tables = [
        f"{row['schema_name']}.{row['table_name']}" for _, row in df.iterrows()
    ]
    assert "test_schema1.table1" in schema_tables
    assert "test_schema1.table2" in schema_tables
    assert "test_schema2.table3" not in schema_tables
    assert "special-schema.table-with-dashes" not in schema_tables

    # Verify row counts are correct
    table_counts = {}
    for _, row in df.iterrows():
        schema_table = f"{row['schema_name']}.{row['table_name']}"
        table_counts[schema_table] = row["estimated_size"]

    assert table_counts["test_schema1.table1"] == 3
    assert table_counts["test_schema1.table2"] == 2


def test_get_empty_schema_metadata(setup_test_data):
    """Test getting metadata for an empty schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with an empty schema
    df = duck_utils.get_tables(schema_names=["empty_schema"])

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns


def test_get_nonexistent_schema_metadata(setup_test_data):
    """Test getting metadata for a nonexistent schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with a nonexistent schema
    df = duck_utils.get_tables(schema_names=["nonexistent_schema"])

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns


def test_get_metadata_empty_database(tmp_path):
    """Test getting metadata with a completely empty database."""
    # Create an empty database file
    empty_db_path = tmp_path / "empty_db.duckdb"
    duck_utils = DuckUtils(db_file_path=empty_db_path)

    # Call the method with include_views=False to exclude system views
    df = duck_utils.get_tables(include_views=False)

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns


#################################################
# View-Related Tests
#################################################


def test_get_metadata_with_views(temp_db_with_views):
    """Test that get_metadata includes views when include_views=True."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Get all tables and views (default behavior), excluding system views
    df = duck_utils.get_metadata(detail_level="table", include_system_views=False)

    # Verify that tables and views are included
    objects = set(zip(df["schema_name"], df["table_name"]))

    # Tables should be included
    assert ("test_schema", "users") in objects
    assert ("test_schema", "orders") in objects

    # Views should be included
    assert ("test_schema", "user_summary") in objects
    assert ("test_schema", "order_details") in objects
    assert ("views_only_schema", "all_users") in objects

    # Check that object_type column is present and has correct values
    assert "object_type" in df.columns
    assert "table" in df["object_type"].values
    assert "view" in df["object_type"].values

    # Check tables specifically
    tables_only = df[df["object_type"] == "table"]
    assert len(tables_only) == 2  # users and orders

    # Check user-created views specifically
    # Filter out system views from information_schema, main, pg_catalog
    user_views = df[
        (df["object_type"] == "view")
        & (~df["schema_name"].isin(["information_schema", "main", "pg_catalog"]))
    ]
    assert len(user_views) == 3  # user_summary, order_details, and all_users


def test_get_metadata_without_views(temp_db_with_views):
    """Test that get_metadata excludes views when include_views=False."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Get only tables, no views
    df = duck_utils.get_metadata(detail_level="table", include_views=False)

    # Verify that only tables are included
    objects = set(zip(df["schema_name"], df["table_name"]))

    # Tables should be included
    assert ("test_schema", "users") in objects
    assert ("test_schema", "orders") in objects

    # Views should not be included
    assert ("test_schema", "user_summary") not in objects
    assert ("test_schema", "order_details") not in objects
    assert ("views_only_schema", "all_users") not in objects

    # All records should have object_type = "table"
    assert all(df["object_type"] == "table")


def test_get_tables_with_views(temp_db_with_views):
    """Test that get_tables passes the include_views parameter correctly."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Get all tables and views (default)
    df_with_views = duck_utils.get_tables()

    # Get only tables
    df_without_views = duck_utils.get_tables(include_views=False)

    # Should have more rows with views included
    assert len(df_with_views) > len(df_without_views)

    # Check that views_only_schema is present when views are included
    schemas_with_views = df_with_views["schema_name"].unique()
    assert "views_only_schema" in schemas_with_views

    # Check that views_only_schema is not present when views are excluded
    schemas_without_views = df_without_views["schema_name"].unique()
    assert "views_only_schema" not in schemas_without_views


def test_get_metadata_with_column_detail_and_schema_filter(
    temp_db_with_tables_and_views,
):
    """Test get_metadata with column-level detail and schema filtering.
    This specifically tests the fix for the double prefixing bug."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_tables_and_views)

    # This combination previously caused the "t.t.schema_name" bug
    df = duck_utils.get_metadata(schema_names=["test_schema"], detail_level="column")

    # Verify the query worked and returned expected data
    assert not df.empty
    assert "test_schema" in df["schema_name"].values
    assert "column_name" in df.columns
    assert "data_type" in df.columns

    # Check that both table and view data is returned
    assert "test_table" in df["table_name"].values
    assert "test_view" in df["table_name"].values

    # Check column data
    table_columns = df[df["table_name"] == "test_table"]["column_name"].tolist()
    assert "id" in table_columns
    assert "name" in table_columns


def test_get_metadata_complex_filters(temp_db_with_tables_and_views):
    """Test get_metadata with combined schema and table filters."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_tables_and_views)

    # Filter by both schema and table name
    df = duck_utils.get_metadata(
        schema_names=["test_schema"], table_names=["test_table"], detail_level="table"
    )

    # Verify only the test_table is returned, not the view
    assert len(df) == 1
    assert df.iloc[0]["schema_name"] == "test_schema"
    assert df.iloc[0]["table_name"] == "test_table"
    assert df.iloc[0]["object_type"] == "table"

    # Now try to get only the view
    df_view = duck_utils.get_metadata(
        schema_names=["test_schema"], table_names=["test_view"], detail_level="table"
    )

    # Verify only the view is returned
    assert len(df_view) == 1
    assert df_view.iloc[0]["schema_name"] == "test_schema"
    assert df_view.iloc[0]["table_name"] == "test_view"
    assert df_view.iloc[0]["object_type"] == "view"


#################################################
# Column-Level Tests
#################################################


def test_get_tables_with_columns(setup_test_data):
    """Test getting table metadata with column details."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get tables with column details
    df = duck_utils.get_tables(include_columns=True)

    # Verify DataFrame contains table and column information
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "column_name" in df.columns
    assert "data_type" in df.columns
    assert "is_nullable" in df.columns

    # Verify we have the expected columns for a specific table
    table1_columns = df[
        (df["schema_name"] == "test_schema1") & (df["table_name"] == "table1")
    ]["column_name"].tolist()

    assert "id" in table1_columns
    assert "name" in table1_columns


def test_get_column_metadata_with_views(temp_db_with_views):
    """Test column-level metadata retrieval for both tables and views."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Get column metadata for all tables and views
    df = duck_utils.get_metadata(detail_level="column")

    # Check that we have columns from both tables and views
    user_columns = df[
        (df["schema_name"] == "test_schema") & (df["table_name"] == "users")
    ]["column_name"].tolist()
    view_columns = df[
        (df["schema_name"] == "test_schema") & (df["table_name"] == "user_summary")
    ]["column_name"].tolist()
    order_detail_columns = df[
        (df["schema_name"] == "test_schema") & (df["table_name"] == "order_details")
    ]["column_name"].tolist()

    # Verify table columns
    assert "id" in user_columns
    assert "name" in user_columns
    assert "email" in user_columns

    # Verify view columns
    assert "id" in view_columns
    assert "name" in view_columns
    assert "email" in view_columns

    # Verify more complex view columns
    assert "order_id" in order_detail_columns
    assert "user_name" in order_detail_columns
    assert "amount" in order_detail_columns


def test_get_column_metadata_without_views(temp_db_with_views):
    """Test column-level metadata retrieval for tables only."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Get column metadata for tables only
    df = duck_utils.get_metadata(detail_level="column", include_views=False)

    # Get all unique table names
    tables = set(df["table_name"])

    # Check that only actual tables are included
    assert "users" in tables
    assert "orders" in tables

    # Check that views are excluded
    assert "user_summary" not in tables
    assert "order_details" not in tables
    assert "all_users" not in tables


def test_get_tables_filter_by_table_names(setup_test_data):
    """Test filtering tables by name with schema filters."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get specific tables from specific schemas
    df = duck_utils.get_tables(
        schema_names=["test_schema1", "test_schema2"], table_names=["table1", "table3"]
    )

    # Should return exactly 2 rows (one for each table)
    assert len(df) == 2

    # Check specific table presence
    tables = set(df["table_name"])
    assert "table1" in tables
    assert "table3" in tables
    assert "table2" not in tables


def test_get_tables_filter_by_table_names_only(setup_test_data):
    """Test filtering tables by name without schema filter."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get specific tables from any schema
    df = duck_utils.get_tables(table_names=["table1"])

    # Should return exactly 1 row
    assert len(df) == 1

    # Check specific table presence
    tables = set(df["table_name"])
    assert "table1" in tables
    assert "table2" not in tables
    assert "table3" not in tables


#################################################
# LLM Prompt Tests
#################################################


def test_llm_prompt_includes_views(temp_db_with_views):
    """Test that LLM prompt includes views by default."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Generate LLM prompt
    prompt = duck_utils.get_llm_prompt()

    # Verify tables are included
    assert "users" in prompt
    assert "orders" in prompt

    # Verify views are included
    assert "user_summary" in prompt
    assert "order_details" in prompt
    assert "all_users" in prompt


def test_llm_prompt_excludes_views(temp_db_with_views):
    """Test that LLM prompt can exclude views when requested."""
    duck_utils = DuckUtils(db_file_path=temp_db_with_views)

    # Generate LLM prompt specifically without views
    prompt = duck_utils.get_llm_prompt(include_views=False)

    # Verify tables are included
    assert "users" in prompt
    assert "orders" in prompt

    # Verify views are excluded
    assert "user_summary" not in prompt
    assert "order_details" not in prompt
    assert "all_users" not in prompt


def test_get_llm_prompt_markdown_format(setup_test_data):
    """Test LLM prompt generation in markdown format."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate markdown format prompt
    prompt = duck_utils.get_llm_prompt(format_="markdown")

    # Check markdown formatting
    assert "Table: `test_schema1.table1`" in prompt
    # The column header might vary based on version, just check for table format
    assert "|" in prompt and "-" in prompt


def test_get_llm_prompt_json_format(setup_test_data):
    """Test LLM prompt generation in JSON format."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate JSON format prompt
    prompt = duck_utils.get_llm_prompt(format_="json")

    # Check JSON formatting
    assert "schema" in prompt
    assert "table" in prompt
    assert "columns" in prompt


def test_get_llm_prompt_with_custom_prefix(setup_test_data):
    """Test LLM prompt with custom prefix."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt with custom prefix
    custom_prefix = "This is a custom prompt prefix."
    prompt = duck_utils.get_llm_prompt(prompt_prefix=custom_prefix)

    # Check custom prefix
    assert prompt.startswith(custom_prefix)


def test_get_llm_prompt_with_limited_tables(setup_test_data):
    """Test LLM prompt with limited number of tables."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt with limited tables
    prompt = duck_utils.get_llm_prompt(max_tables=1)

    # Should indicate more tables are omitted
    assert "more tables omitted" in prompt


def test_get_llm_prompt_with_limited_columns(setup_test_data):
    """Test LLM prompt with limited number of columns."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt with limited columns
    prompt = duck_utils.get_llm_prompt(max_columns=1)

    # Should show truncation indication
    assert "…" in prompt


def test_get_llm_prompt_with_constraints(setup_test_data):
    """Test LLM prompt with constraint information."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt with constraints
    prompt = duck_utils.get_llm_prompt(include_constraints=True)

    # Should include PK column
    assert "PK" in prompt


def test_get_llm_prompt_without_constraints(setup_test_data):
    """Test LLM prompt without constraint information."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt without constraints
    prompt = duck_utils.get_llm_prompt(include_constraints=False)

    # Count occurrences of "PK" in the prompt - should be 0
    pk_count = prompt.count("| PK |")
    assert pk_count == 0


def test_get_llm_prompt_empty_result(setup_test_data):
    """Test LLM prompt with no matching tables."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Generate prompt with non-matching filter
    prompt = duck_utils.get_llm_prompt(table_names=["nonexistent_table"])

    # Should include default message
    assert "No tables matched your filters" in prompt


#################################################
# Integration and Edge Case Tests
#################################################


def test_integration_filter_and_format(setup_test_data):
    """Integration test combining filtering with formatting."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Combine multiple parameters
    prompt = duck_utils.get_llm_prompt(
        schema_names=["test_schema1"],
        include_row_counts=True,
        include_constraints=True,
        format_="markdown",
    )

    # Verify correct schema filtering
    assert "test_schema1.table1" in prompt
    assert "test_schema2.table3" not in prompt

    # Verify row counts
    assert "(3 rows)" in prompt or "(3," in prompt

    # Verify markdown formatting
    assert "|" in prompt


def test_special_characters_in_identifiers(setup_test_data):
    """Test handling of special characters in schema and table names."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get table with special characters
    df = duck_utils.get_tables(
        schema_names=["special-schema"], table_names=["table-with-dashes"]
    )

    # Should return exactly 1 row
    assert len(df) == 1
    assert df.iloc[0]["schema_name"] == "special-schema"
    assert df.iloc[0]["table_name"] == "table-with-dashes"


def test_with_and_without_row_counts(setup_test_data):
    """Test LLM prompt with and without row counts."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # With row counts
    with_counts = duck_utils.get_llm_prompt(include_row_counts=True)
    assert "(3," in with_counts or "(3 rows)" in with_counts

    # Without row counts
    without_counts = duck_utils.get_llm_prompt(include_row_counts=False)
    assert "(0," in without_counts or "(0 rows)" in without_counts


def test_connection_reuse():
    """Test reusing the same connection."""
    # Create connection
    duck_utils1 = DuckUtils()
    conn = duck_utils1.conn

    # Create a new instance with same connection
    duck_utils2 = DuckUtils(conn=conn)

    # Both should use the same connection object
    assert duck_utils1.conn is duck_utils2.conn

    # Execute a query through both
    result1 = duck_utils1.conn.execute("SELECT 1").fetchone()[0]
    result2 = duck_utils2.conn.execute("SELECT 1").fetchone()[0]

    assert result1 == 1
    assert result2 == 1


def test_get_metadata_directly(setup_test_data):
    """Test direct use of the get_metadata method."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get metadata directly
    df = duck_utils.get_metadata(schema_names=["test_schema1"], detail_level="column")

    # Verify correct columns returned
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "column_name" in df.columns
    assert "data_type" in df.columns

    # Verify filtered correctly
    schemas = set(df["schema_name"])
    assert "test_schema1" in schemas
    assert "test_schema2" not in schemas


if __name__ == "__main__":
    pytest.main([__file__])
