"""Dependency graph and calculation log management."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster_quickstart.utils.datetime_utils import utc_now_metadata
from database.models import (
    CalculationLogBase,
    CalculationStatus,
    SeriesDependencyBase,
)
from database.utils import DatabaseResource


class DependencyManager:
    """Manager for dependency graph operations."""

    def __init__(self, database: DatabaseResource):
        """Initialize with database resource (ClickHouse or DuckDB).

        Args:
            database: Database resource instance (ClickHouseResource or DuckDBResource)
        """
        self.database = database

    def create_dependency(self, dependency: SeriesDependencyBase) -> int:
        """Create a new dependency relationship."""
        now = datetime.now()

        # Get next dependency_id
        result = self.database.execute_query("SELECT max(dependency_id) FROM seriesDependencyGraph")
        # Handle both ClickHouse and DuckDB result formats
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_id = result.result_rows[0][0] + 1
        elif hasattr(result, "iloc") and not result.empty:
            max_id = result.iloc[0, 0]
            next_id = (max_id + 1) if max_id is not None else 1
        else:
            next_id = 1

        # Use DuckDB-compatible SQL syntax (DuckDBResource will handle parameter substitution)
        query = """
        INSERT INTO seriesDependencyGraph (
            dependency_id, parent_series_id, child_series_id, weight,
            formula, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        self.database.execute_command(
            query,
            parameters=[
                next_id,
                dependency.parent_series_id,
                dependency.child_series_id,
                dependency.weight or 1.0,
                dependency.formula or "",
                now,
                now,
            ],
        )

        return next_id

    def get_parent_dependencies(self, child_series_id: int) -> List[Dict[str, Any]]:
        """Get all parent dependencies for a child series."""
        query = """
        SELECT * FROM seriesDependencyGraph
        WHERE child_series_id = ?
        ORDER BY parent_series_id
        """
        result = self.database.execute_query(query, parameters=[child_series_id])
        # Handle both ClickHouse and DuckDB result formats
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        elif hasattr(result, "columns") and hasattr(result, "to_dict"):
            return result.to_dict("records")
        return []

    def get_child_dependencies(self, parent_series_id: int) -> List[Dict[str, Any]]:
        """Get all child dependencies for a parent series."""
        query = """
        SELECT * FROM seriesDependencyGraph
        WHERE parent_series_id = ?
        ORDER BY child_series_id
        """
        result = self.database.execute_query(query, parameters=[parent_series_id])
        # Handle both ClickHouse and DuckDB result formats
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        elif hasattr(result, "columns") and hasattr(result, "to_dict"):
            return result.to_dict("records")
        return []

    def get_dependency_tree(self, series_id: int, max_depth: int = 10) -> Dict[str, Any]:
        """Get full dependency tree for a series (recursive)."""
        tree = {"series_id": series_id, "parents": [], "children": []}

        if max_depth <= 0:
            return tree

        # Get parents
        parents = self.get_parent_dependencies(series_id)
        if isinstance(tree.get("parents"), list):
            for parent_dep in parents:
                parent_tree = self.get_dependency_tree(
                    parent_dep["parent_series_id"], max_depth - 1
                )
                tree["parents"].append(
                    {
                        "dependency": parent_dep,
                        "series": parent_tree,
                    }
                )

        # Get children
        children = self.get_child_dependencies(series_id)
        if isinstance(tree.get("children"), list):
            for child_dep in children:
                child_tree = self.get_dependency_tree(child_dep["child_series_id"], max_depth - 1)
                tree["children"].append(
                    {
                        "dependency": child_dep,
                        "series": child_tree,
                    }
                )

        return tree


class CalculationLogManager:
    """Manager for calculation log operations."""

    def __init__(self, database: DatabaseResource):
        """Initialize with database resource (ClickHouse or DuckDB).

        Args:
            database: Database resource instance (ClickHouseResource or DuckDBResource)
        """
        self.database = database

    def create_calculation_log(self, calculation: CalculationLogBase) -> int:
        """Create a new calculation log entry."""
        # Get next calculation_id
        result = self.database.execute_query("SELECT max(calculation_id) FROM calculationLog")
        # Handle both ClickHouse and DuckDB result formats
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_id = result.result_rows[0][0] + 1
        elif hasattr(result, "iloc") and not result.empty:
            max_id = result.iloc[0, 0]
            next_id = (max_id + 1) if max_id is not None else 1
        else:
            next_id = 1

        # Use DuckDB-compatible SQL syntax
        # For arrays, DuckDB accepts Python lists directly
        now = utc_now_metadata()

        query = """
        INSERT INTO calculationLog (
            calculation_id, series_id, calculation_type, status,
            input_series_ids, parameters, formula, rows_processed, error_message, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.database.execute_command(
            query,
            parameters=[
                next_id,
                calculation.series_id,
                calculation.calculation_type,
                str(calculation.status.value),
                calculation.input_series_ids or [],  # DuckDB accepts Python lists as arrays
                calculation.parameters or "",
                calculation.formula or "",
                calculation.rows_processed or 0,
                calculation.error_message or "",
                now,
            ],
        )

        return next_id

    def update_calculation_log(
        self,
        calculation_id: int,
        status: CalculationStatus,
        execution_end: Optional[datetime] = None,
        rows_processed: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a calculation log entry."""
        # Build update query dynamically based on what's provided
        # Use DuckDB-compatible UPDATE syntax
        updates = ["status = ?"]
        params = [str(status.value)]

        if rows_processed is not None:
            updates.append("rows_processed = ?")
            params.append(rows_processed)

        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        # DuckDB uses standard UPDATE syntax, not ALTER TABLE UPDATE
        query = f"""
        UPDATE calculationLog
        SET {', '.join(updates)}
        WHERE calculation_id = ?
        """
        params.append(calculation_id)

        self.database.execute_command(query, parameters=params)

    def get_calculation_logs(
        self,
        series_id: Optional[int] = None,
        status: Optional[CalculationStatus] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get calculation logs with optional filters."""
        query = "SELECT * FROM calculationLog WHERE 1=1"
        params = []

        if series_id:
            query += " AND series_id = ?"
            params.append(series_id)

        if status:
            query += " AND status = ?"
            params.append(str(status.value))

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        result = self.database.execute_query(query, parameters=params)
        # Handle both ClickHouse and DuckDB result formats
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        elif hasattr(result, "columns") and hasattr(result, "to_dict"):
            return result.to_dict("records")
        return []
