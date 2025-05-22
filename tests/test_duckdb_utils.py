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


if __name__ == "__main__":
    pytest.main([__file__])
