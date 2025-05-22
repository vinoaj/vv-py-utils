from pathlib import Path

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


# Tests for display_schema_table_counts
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
    assert "no tables found in schema(s) empty_schema" in output


def test_display_nonexistent_schema(setup_test_data, capsys):
    """Test displaying a nonexistent schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with a nonexistent schema
    duck_utils.display_table_row_counts(["nonexistent_schema"])

    # Capture the printed output
    captured = capsys.readouterr()
    output = captured.out

    # Verify appropriate message is displayed
    assert "no tables found in schema(s) nonexistent_schema" in output


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
    assert "ⓘ no tables found in all schemas" in output


# Tests for get_table_metadata
def test_get_all_schemas_metadata(setup_test_data):
    """Test getting metadata for all schemas."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method that returns a DataFrame
    df = duck_utils.get_table_metadata()

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
    df = duck_utils.get_table_metadata(["test_schema1"])

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
    df = duck_utils.get_table_metadata(["empty_schema"])

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns


def test_get_nonexistent_schema_metadata(setup_test_data):
    """Test getting metadata for a nonexistent schema."""
    duck_utils = DuckUtils(db_file_path=setup_test_data)

    # Call the method with a nonexistent schema
    df = duck_utils.get_table_metadata(["nonexistent_schema"])

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
    df = duck_utils.get_table_metadata()

    # Verify DataFrame is empty but has correct structure
    assert df.empty
    assert "schema_name" in df.columns
    assert "table_name" in df.columns
    assert "estimated_size" in df.columns
