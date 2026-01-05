"""Database utility functions for common operations."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.datetime_utils import utc_now_metadata


def get_next_id(clickhouse: ClickHouseResource, table_name: str, id_column: str) -> int:
    """Get the next ID for a table by querying max ID.

    Args:
        clickhouse: ClickHouse resource
        table_name: Name of the table
        id_column: Name of the ID column

    Returns:
        Next ID to use (max + 1, or 1 if table is empty)
    """
    query = f"SELECT max({id_column}) FROM {table_name}"
    result = clickhouse.execute_query(query)
    if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
        return result.result_rows[0][0] + 1
    return 1


def get_by_name(
    clickhouse: ClickHouseResource,
    table_name: str,
    name_column: str,
    name: str,
) -> Optional[Dict[str, Any]]:
    """Get a record by name from any lookup table.

    Args:
        clickhouse: ClickHouse resource
        table_name: Name of the table
        name_column: Name of the name column
        name: Name to search for

    Returns:
        Dictionary with record data or None if not found
    """
    query = f"SELECT * FROM {table_name} WHERE {name_column} = {{name:String}} LIMIT 1"
    result = clickhouse.execute_query(query, parameters={"name": name})
    if hasattr(result, "result_rows") and result.result_rows:
        columns = result.column_names
        return dict(zip(columns, result.result_rows[0]))
    return None


def get_by_id(
    clickhouse: ClickHouseResource,
    table_name: str,
    id_column: str,
    record_id: int,
) -> Optional[Dict[str, Any]]:
    """Get a record by ID from any lookup table.

    Args:
        clickhouse: ClickHouse resource
        table_name: Name of the table
        id_column: Name of the ID column
        record_id: ID to search for

    Returns:
        Dictionary with record data or None if not found
    """
    query = f"SELECT * FROM {table_name} WHERE {id_column} = {{id:UInt32}} LIMIT 1"
    result = clickhouse.execute_query(query, parameters={"id": record_id})
    if hasattr(result, "result_rows") and result.result_rows:
        columns = result.column_names
        return dict(zip(columns, result.result_rows[0]))
    return None


def execute_update_query(
    clickhouse: ClickHouseResource,
    table_name: str,
    id_column: str,
    record_id: int,
    update_fields: Dict[str, Any],
    now: Optional[datetime] = None,
) -> None:
    """Execute an UPDATE query using ALTER TABLE.

    Args:
        clickhouse: ClickHouse resource
        table_name: Name of the table
        id_column: Name of the ID column
        record_id: ID of the record to update
        update_fields: Dictionary of field_name -> value to update
        now: Optional datetime for updated_at field
    """
    if now is None:
        now = utc_now_metadata()

    # Build SET clause
    set_clauses = []
    params = {"id": record_id, "now": now}

    for field_name, value in update_fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            set_clauses.append(f"{field_name} = {{field_{field_name}:UInt8}}")
            params[f"field_{field_name}"] = 1 if value else 0
        elif isinstance(value, str):
            set_clauses.append(f"{field_name} = {{field_{field_name}:String}}")
            params[f"field_{field_name}"] = value
        elif isinstance(value, int):
            # Check if it's a UInt8 field (like is_derived)
            if field_name.startswith("is_") or field_name == "is_derived":
                set_clauses.append(f"{field_name} = {{field_{field_name}:UInt8}}")
                params[f"field_{field_name}"] = value
            else:
                set_clauses.append(f"{field_name} = {{field_{field_name}:UInt32}}")
                params[f"field_{field_name}"] = value
        else:
            set_clauses.append(f"{field_name} = {{field_{field_name}:String}}")
            params[f"field_{field_name}"] = str(value) if value is not None else None

    # Add updated_at
    set_clauses.append("updated_at = {now:DateTime64(6)}")

    if not set_clauses:
        return  # Nothing to update

    query = f"""
    ALTER TABLE {table_name}
    UPDATE {', '.join(set_clauses)}
    WHERE {id_column} = {{id:UInt32}}
    """

    clickhouse.execute_command(query, parameters=params)


def execute_insert_query(
    clickhouse: ClickHouseResource,
    table_name: str,
    id_column: str,
    record_id: int,
    insert_fields: Dict[str, Any],
    now: Optional[datetime] = None,
) -> None:
    """Execute an INSERT query.

    Args:
        clickhouse: ClickHouse resource
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
    value_placeholders = ["{id:UInt32}"]
    params = {"id": record_id, "now": now}

    for field_name, value in insert_fields.items():
        if value is None:
            continue
        columns.append(field_name)
        if isinstance(value, bool):
            value_placeholders.append(f"{{field_{field_name}:UInt8}}")
            params[f"field_{field_name}"] = 1 if value else 0
        elif isinstance(value, str):
            value_placeholders.append(f"{{field_{field_name}:String}}")
            params[f"field_{field_name}"] = value
        elif isinstance(value, int):
            # Check if it's a UInt8 field (like is_derived)
            if field_name.startswith("is_") or field_name == "is_derived":
                value_placeholders.append(f"{{field_{field_name}:UInt8}}")
                params[f"field_{field_name}"] = value
            else:
                value_placeholders.append(f"{{field_{field_name}:UInt32}}")
                params[f"field_{field_name}"] = value
        else:
            value_placeholders.append(f"{{field_{field_name}:String}}")
            params[f"field_{field_name}"] = str(value) if value is not None else None

    columns.extend(["created_at", "updated_at"])
    value_placeholders.extend(["{now:DateTime64(6)}", "{now:DateTime64(6)}"])

    query = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES ({', '.join(value_placeholders)})
    """

    clickhouse.execute_command(query, parameters=params)


def query_to_dict_list(
    result: Any,
) -> List[Dict[str, Any]]:
    """Convert ClickHouse query result to list of dictionaries.

    Args:
        result: ClickHouse query result

    Returns:
        List of dictionaries, one per row
    """
    if not hasattr(result, "column_names") or not hasattr(result, "result_rows"):
        return []

    columns = result.column_names
    return [dict(zip(columns, row)) for row in result.result_rows]


def query_to_dict(
    result: Any,
) -> Optional[Dict[str, Any]]:
    """Convert ClickHouse query result to single dictionary (first row).

    Args:
        result: ClickHouse query result

    Returns:
        Dictionary with first row data or None if no rows
    """
    if not hasattr(result, "result_rows") or not result.result_rows:
        return None

    columns = result.column_names
    return dict(zip(columns, result.result_rows[0]))
