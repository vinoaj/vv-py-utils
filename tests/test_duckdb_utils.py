import duckdb
import pytest

from vvpyutils.duckdb.utils import DuckUtils


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
        (1, TRUE), (2, FALSE), (3, TRUE), (4, FALSE), (5, TRUE)
    """)

    # Create an empty schema
    conn.execute("CREATE SCHEMA empty_schema")

    # Special characters in schema/table names
    conn.execute('CREATE SCHEMA "special-schema"')
    conn.execute("""
        CREATE TABLE "special-schema"."table-with-dashes" (id INTEGER)
    """)
    conn.execute("""
        INSERT INTO "special-schema"."table-with-dashes" VALUES 
        (1), (2), (3), (4)
    """)

    conn.close()

    return temp_db_path


# Tests for connection initialization
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


# Tests for display_table_row_counts
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


# Tests for table-level metadata retrieval
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
        table_name = f"{row['schema_name']}.{row['table_name']}"
        table_counts[table_name] = row["estimated_size"]

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
        table_name = f"{row['schema_name']}.{row['table_name']}"
        table_counts[table_name] = row["estimated_size"]

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

    # Call the method
    df = duck_utils.get_tables()

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns


# Tests for column-level metadata retrieval
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


def test_get_tables_with_constraints(setup_test_data):
    """Test getting table metadata with constraints."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get tables with column details and constraints
    df = duck_utils.get_tables(include_columns=True, include_constraints=True)

    # Verify constraints column exists
    assert "pk" in df.columns or df.columns.isin(["pk"]).any()


# Tests for filtering tables by name
def test_get_tables_filter_by_table_names(setup_test_data):
    """Test filtering tables by table names."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Should be able to filter by table name without needing column-level detail
    df = duck_utils.get_tables(
        schema_names=["test_schema1", "test_schema2"], table_names=["table1", "table3"]
    )

    # Verify only requested tables are returned
    table_names = df["table_name"].unique().tolist()
    assert "table1" in table_names
    assert "table3" in table_names
    assert "table2" not in table_names


def test_get_tables_filter_by_table_names_only(setup_test_data):
    """Test filtering tables by table names only without specifying schemas."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Should be able to filter by table name only
    df = duck_utils.get_tables(table_names=["table1"])

    # Verify only the requested table is returned
    table_names = df["table_name"].unique().tolist()
    assert len(table_names) == 1
    assert "table1" in table_names
    assert "table2" not in table_names
    assert "table3" not in table_names


# Tests for utility methods
def test_ensure_list_conversion():
    """Test the _ensure_list utility method."""
    duck_utils = DuckUtils()

    # Test with various iterable types
    assert duck_utils._ensure_list(["a", "b"]) == ["a", "b"]
    assert duck_utils._ensure_list(("a", "b")) == ["a", "b"]

    # For sets, the order is not guaranteed, so we need to check differently
    set_result = duck_utils._ensure_list({"a", "b"})
    assert isinstance(set_result, list)
    assert set(set_result) == {"a", "b"}

    assert duck_utils._ensure_list(None) is None


# Tests for direct use of the get_metadata method
def test_get_metadata_directly(setup_test_data):
    """Test the get_metadata method directly."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Test table-level metadata
    table_df = duck_utils.get_metadata(detail_level="table")
    assert "schema_name" in table_df.columns
    assert "table_name" in table_df.columns
    assert "estimated_size" in table_df.columns

    # Test column-level metadata
    column_df = duck_utils.get_metadata(detail_level="column")
    assert "schema_name" in column_df.columns
    assert "table_name" in column_df.columns
    assert "column_name" in column_df.columns
    assert "data_type" in column_df.columns

    # Test with constraints
    constraints_df = duck_utils.get_metadata(
        detail_level="column", include_constraints=True
    )
    assert "pk" in constraints_df.columns or constraints_df.columns.isin(["pk"]).any()


# Tests for the get_llm_prompt method
def test_get_llm_prompt_markdown_format(setup_test_data):
    """Test generating an LLM prompt in markdown format."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    prompt = duck_utils.get_llm_prompt()

    # Verify the default prefix is included
    assert duck_utils._PROMPT_PREFIX in prompt

    # Verify markdown formatting elements are present
    assert "Table: `" in prompt
    assert "| Column " in prompt
    assert "| Type " in prompt

    # Verify all test schemas are included
    assert "test_schema1.table1" in prompt
    assert "test_schema1.table2" in prompt
    assert "test_schema2.table3" in prompt
    assert "special-schema.table-with-dashes" in prompt


def test_get_llm_prompt_json_format(setup_test_data):
    """Test generating an LLM prompt in JSON format."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    prompt = duck_utils.get_llm_prompt(format_="json")

    # Verify the prefix is included
    assert duck_utils._PROMPT_PREFIX in prompt

    # Verify JSON formatting elements
    assert '"schema":' in prompt
    assert '"table":' in prompt
    assert '"columns":' in prompt
    assert '"Column":' in prompt
    assert '"Type":' in prompt


def test_get_llm_prompt_with_custom_prefix(setup_test_data):
    """Test generating an LLM prompt with a custom prefix."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)
    custom_prefix = "Custom prefix for LLM prompt"

    prompt = duck_utils.get_llm_prompt(prompt_prefix=custom_prefix)

    # Verify the custom prefix is included instead of the default
    assert custom_prefix in prompt
    assert duck_utils._PROMPT_PREFIX not in prompt


def test_get_llm_prompt_with_limited_tables(setup_test_data):
    """Test generating an LLM prompt with limited number of tables."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Limit to just 1 table
    prompt = duck_utils.get_llm_prompt(max_tables=1)

    # Verify the tables limit message is included
    assert "more tables omitted" in prompt

    # Count the number of "Table:" occurrences to ensure limit is respected
    assert prompt.count("Table: `") == 1


def test_get_llm_prompt_with_limited_columns(setup_test_data):
    """Test generating an LLM prompt with limited number of columns."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Create a table with many columns for testing
    conn = duckdb.connect(str(setup_test_data))
    conn.execute("""
        DROP TABLE IF EXISTS test_schema1.many_columns;
        CREATE TABLE test_schema1.many_columns (
            col1 INTEGER, 
            col2 INTEGER, 
            col3 INTEGER, 
            col4 INTEGER, 
            col5 INTEGER,
            col6 INTEGER, 
            col7 INTEGER, 
            col8 INTEGER, 
            col9 INTEGER, 
            col10 INTEGER
        );
        INSERT INTO test_schema1.many_columns VALUES (1,2,3,4,5,6,7,8,9,10);
    """)
    conn.close()

    try:
        # Limit to just 3 columns - we'll just check the prompt doesn't crash
        prompt = duck_utils.get_llm_prompt(
            schema_names=["test_schema1"], table_names=["many_columns"], max_columns=3
        )

        # Basic verification that the prompt was generated successfully
        assert "test_schema1.many_columns" in prompt

        # Check we have some columns but not all
        col_count = prompt.count("col")
        assert col_count > 0, "No columns found in prompt"
        assert col_count <= 4, (
            "Too many columns in prompt"
        )  # 3 + possible appearance in column header
    except Exception as e:
        pytest.fail(f"Error generating prompt with limited columns: {str(e)}")


def test_get_llm_prompt_without_constraints(setup_test_data):
    """Test generating an LLM prompt without constraints."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Create a table with a primary key
    conn = duckdb.connect(str(setup_test_data))
    conn.execute("""
        CREATE TABLE test_schema1.with_pk (
            id INTEGER PRIMARY KEY, 
            value VARCHAR
        )
    """)
    conn.close()

    # Generate prompt without constraints
    prompt = duck_utils.get_llm_prompt(
        table_names=["with_pk"], include_constraints=False
    )

    # Verify PK column is not included
    assert "| PK " not in prompt


def test_get_llm_prompt_with_constraints(setup_test_data):
    """Test generating an LLM prompt with constraints."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Create a table with a primary key
    conn = duckdb.connect(str(setup_test_data))
    conn.execute("""
        DROP TABLE IF EXISTS test_schema1.with_pk;
        CREATE TABLE test_schema1.with_pk (
            id INTEGER PRIMARY KEY, 
            value VARCHAR
        );
        INSERT INTO test_schema1.with_pk VALUES (1, 'test');
    """)

    # Verify the primary key was created properly
    pk_info = conn.execute("""
        SELECT * FROM duckdb_constraints()
        WHERE schema_name = 'test_schema1'
        AND table_name = 'with_pk'
        AND constraint_type = 'PRIMARY KEY'
    """).fetchall()

    assert len(pk_info) > 0, "Primary key not properly created for test"

    # Print primary key information for debugging
    pk_columns = conn.execute("""
        SELECT constraint_column_names 
        FROM duckdb_constraints() 
        WHERE schema_name = 'test_schema1'
        AND table_name = 'with_pk'
        AND constraint_type = 'PRIMARY KEY'
    """).fetchone()

    print(f"PK columns for test: {pk_columns[0]}")
    conn.close()

    # Generate prompt with constraints
    prompt = duck_utils.get_llm_prompt(
        schema_names=["test_schema1"], table_names=["with_pk"], include_constraints=True
    )

    # Save prompt output for debugging
    print("\nGenerated prompt excerpt:")
    print(prompt[:500] + "..." if len(prompt) > 500 else prompt)

    # Verify PK column is included
    assert "| PK " in prompt

    # Check that the output contains the checkmark somewhere
    assert "✓" in prompt, "Primary key checkmark not found in the output"


def test_get_llm_prompt_empty_result(setup_test_data):
    """Test generating an LLM prompt when no tables match the filter."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    prompt = duck_utils.get_llm_prompt(table_names=["nonexistent_table"])

    # Verify appropriate message is displayed
    assert "_No tables matched your filters._" in prompt


# Integration tests for combined functionality
def test_integration_filter_and_format(setup_test_data):
    """Test an end-to-end workflow of filtering tables and formatting output."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Get a specific table in JSON format
    prompt = duck_utils.get_llm_prompt(
        schema_names=["test_schema1"], table_names=["table1"], format_="json"
    )

    # Verify JSON formatting and content specificity
    assert "test_schema1" in prompt
    assert "table1" in prompt
    assert '"rows": 3' in prompt  # table1 has 3 rows

    # Verify other tables are not included
    assert "table2" not in prompt
    assert "table3" not in prompt


# Edge case tests
def test_special_characters_in_identifiers(setup_test_data):
    """Test handling of special characters in schema and table names."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Query specifically for the table with dashes in its name
    df = duck_utils.get_tables(
        schema_names=["special-schema"], table_names=["table-with-dashes"]
    )

    # Verify correct handling of special characters
    assert len(df) == 1
    assert df.iloc[0]["schema_name"] == "special-schema"
    assert df.iloc[0]["table_name"] == "table-with-dashes"
    assert df.iloc[0]["estimated_size"] == 4


def test_with_and_without_row_counts(setup_test_data):
    """Test LLM prompt generation with and without row counts."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # With row counts (default)
    prompt_with_counts = duck_utils.get_llm_prompt()
    assert "(3 rows)" in prompt_with_counts  # table1 has 3 rows

    # Without row counts
    prompt_without_counts = duck_utils.get_llm_prompt(include_row_counts=False)
    assert "(0 rows)" in prompt_without_counts  # Should show 0 when disabled
    assert "(3 rows)" not in prompt_without_counts


def test_connection_reuse():
    """Test that the connection is reused across method calls."""
    duck_utils = DuckUtils()

    # Create a test table
    duck_utils.conn.execute("CREATE TABLE test_table (id INTEGER)")
    duck_utils.conn.execute("INSERT INTO test_table VALUES (1), (2), (3)")

    # Verify the connection is the same and data persists
    tables_df = duck_utils.get_tables()
    assert "test_table" in tables_df["table_name"].values

    # Check row count
    row_count = duck_utils.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert row_count == 3


# Tests for the _build_where_clause method
def test_build_where_clause_single_condition():
    """Test building a WHERE clause with a single condition."""
    duck_utils = DuckUtils()

    conditions = {"schema_name": ["test_schema1"]}
    where_clause, params = duck_utils._build_where_clause(conditions)

    assert where_clause == "WHERE schema_name IN (?)"
    assert params == ["test_schema1"]


def test_build_where_clause_multiple_conditions():
    """Test building a WHERE clause with multiple conditions."""
    duck_utils = DuckUtils()

    conditions = {
        "schema_name": ["test_schema1", "test_schema2"],
        "table_name": ["table1"],
    }
    where_clause, params = duck_utils._build_where_clause(conditions)

    assert "schema_name IN (?, ?)" in where_clause
    assert "table_name IN (?)" in where_clause
    assert "AND" in where_clause
    assert set(params) == set(["test_schema1", "test_schema2", "table1"])


def test_build_where_clause_with_alias():
    """Test building a WHERE clause with a table alias."""
    duck_utils = DuckUtils()

    conditions = {"schema_name": ["test_schema1"]}
    where_clause, params = duck_utils._build_where_clause(conditions, table_alias="t")

    assert where_clause == "WHERE t.schema_name IN (?)"
    assert params == ["test_schema1"]


def test_build_where_clause_empty_conditions():
    """Test building a WHERE clause with empty conditions."""
    duck_utils = DuckUtils()

    # Empty conditions dict
    where_clause, params = duck_utils._build_where_clause({})
    assert where_clause == ""
    assert params == []

    # Conditions with None values
    where_clause, params = duck_utils._build_where_clause({"schema_name": None})
    assert where_clause == ""
    assert params == []

    # Conditions with empty lists
    where_clause, params = duck_utils._build_where_clause({"schema_name": []})
    assert where_clause == ""
    assert params == []


if __name__ == "__main__":
    pytest.main([__file__])
