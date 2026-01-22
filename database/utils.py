"""Database utility functions for common operations with DuckDB."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.datetime_utils import utc_now_metadata

# Type alias for database resources
DatabaseResource = DuckDBResource


def get_next_id(database: DatabaseResource, table_name: str, id_column: str) -> int:
    """Get the next ID for a table by querying max ID.

    Args:
        database: DuckDB database resource
        table_name: Name of the table
        id_column: Name of the ID column

    Returns:
        Next ID to use (max + 1, or 1 if table is empty)
    """
    query = f"SELECT max({id_column}) FROM {table_name}"
    result = database.execute_query(query)

    # Handle pandas DataFrame result from DuckDB
    if hasattr(result, "empty") and not result.empty:
        max_id = result.iloc[0, 0]
        return (max_id + 1) if max_id is not None else 1
    elif hasattr(result, "iloc") and len(result) > 0:
        max_id = result.iloc[0, 0]
        return (max_id + 1) if max_id is not None else 1

    return 1


def get_by_name(
    database: DatabaseResource,
    table_name: str,
    name_column: str,
    name: str,
) -> Optional[Dict[str, Any]]:
    """Get a record by name from any lookup table.

    Args:
        database: DuckDB database resource
        table_name: Name of the table
        name_column: Name of the name column
        name: Name to search for

    Returns:
        Dictionary with record data or None if not found
    """
    query = f"SELECT * FROM {table_name} WHERE {name_column} = ? LIMIT 1"
    result = database.execute_query(query, parameters=[name])
    return query_to_dict(result)


def get_by_id(
    database: DatabaseResource,
    table_name: str,
    id_column: str,
    record_id: int,
) -> Optional[Dict[str, Any]]:
    """Get a record by ID from any lookup table.

    Args:
        database: DuckDB database resource
        table_name: Name of the table
        id_column: Name of the ID column
        record_id: ID to search for

    Returns:
        Dictionary with record data or None if not found
    """
    query = f"SELECT * FROM {table_name} WHERE {id_column} = ? LIMIT 1"
    result = database.execute_query(query, parameters=[record_id])
    return query_to_dict(result)


def execute_update_query(
    database: DatabaseResource,
    table_name: str,
    id_column: str,
    record_id: int,
    update_fields: Dict[str, Any],
    now: Optional[datetime] = None,
) -> None:
    """Execute an UPDATE query using standard SQL UPDATE syntax.

    Args:
        database: DuckDB database resource
        table_name: Name of the table
        id_column: Name of the ID column
        record_id: ID of the record to update
        update_fields: Dictionary of field_name -> value to update
        now: Optional datetime for updated_at field
    """
    if now is None:
        now = utc_now_metadata()

    # Build SET clause with ? placeholders
    set_clauses = []
    params = []

    for field_name, value in update_fields.items():
        if value is None:
            continue
        set_clauses.append(f"{field_name} = ?")
        params.append(value)

    # Add updated_at
    set_clauses.append("updated_at = ?")
    params.append(now)

    if not set_clauses:
        return  # Nothing to update

    # Use standard SQL UPDATE syntax with DuckDB ? placeholders
    query = f"""
    UPDATE {table_name}
    SET {', '.join(set_clauses)}
    WHERE {id_column} = ?
    """
    params.append(record_id)

    database.execute_command(query, parameters=params)


def execute_insert_query(
    database: DatabaseResource,
    table_name: str,
    id_column: str,
    record_id: int,
    insert_fields: Dict[str, Any],
    now: Optional[datetime] = None,
) -> None:
    """Execute an INSERT query using standard SQL INSERT syntax.

    Args:
        database: DuckDB database resource
        table_name: Name of the table
        id_column: Name of the ID column
        record_id: ID to insert
        insert_fields: Dictionary of field_name -> value to insert
        now: Optional datetime for created_at/updated_at fields
    """
    if now is None:
        now = utc_now_metadata()

    # Build columns and values (only include non-None fields)
    columns = [id_column]
    value_placeholders = ["?"]
    params = [record_id]

    for field_name, value in insert_fields.items():
        if value is None:
            continue
        columns.append(field_name)
        value_placeholders.append("?")
        params.append(value)

    columns.extend(["created_at", "updated_at"])
    value_placeholders.extend(["?", "?"])
    params.extend([now, now])

    query = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES ({', '.join(value_placeholders)})
    """

    database.execute_command(query, parameters=params)


def query_to_dict_list(
    result: Any,
) -> List[Dict[str, Any]]:
    """Convert DuckDB query result (pandas DataFrame) to list of dictionaries.

    Args:
        result: DuckDB query result (pandas DataFrame)

    Returns:
        List of dictionaries, one per row
    """
    # Handle pandas DataFrame result from DuckDB
    if hasattr(result, "columns") and hasattr(result, "to_dict"):
        if result.empty:
            return []
        return result.to_dict("records")

    # Fallback: try to convert if it has to_dict method
    if hasattr(result, "to_dict"):
        try:
            return result.to_dict("records")
        except Exception:
            pass

    return []


def query_to_dict(
    result: Any,
) -> Optional[Dict[str, Any]]:
    """Convert DuckDB query result (pandas DataFrame) to single dictionary (first row).

    Args:
        result: DuckDB query result (pandas DataFrame)

    Returns:
        Dictionary with first row data or None if no rows
    """
    # Handle pandas DataFrame result from DuckDB
    if hasattr(result, "columns") and hasattr(result, "empty"):
        if result.empty:
            return None
        # Convert first row to dict
        return result.iloc[0].to_dict()

    # Fallback: try to convert if it has to_dict method
    if hasattr(result, "to_dict"):
        try:
            records = result.to_dict("records")
            return records[0] if records else None
        except Exception:
            pass

    return None
