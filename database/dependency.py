"""Dependency graph and calculation log management."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.datetime_utils import utc_now_metadata
from database.models import (
    CalculationLogBase,
    CalculationStatus,
    SeriesDependencyBase,
)


class DependencyManager:
    """Manager for dependency graph operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def create_dependency(self, dependency: SeriesDependencyBase) -> int:
        """Create a new dependency relationship."""
        now = datetime.now()

        # Get next dependency_id
        result = self.clickhouse.execute_query(
            "SELECT max(dependency_id) FROM seriesDependencyGraph"
        )
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_id = result.result_rows[0][0] + 1
        else:
            next_id = 1

        query = """
        INSERT INTO seriesDependencyGraph (
            dependency_id, parent_series_id, child_series_id, weight,
            formula, created_at, updated_at
        ) VALUES (
            {id:UInt64}, {parent:UInt32}, {child:UInt32}, {weight:Float64},
            {formula:String}, {now:DateTime64(3)}, {now:DateTime64(3)}
        )
        """

        self.clickhouse.execute_command(
            query,
            parameters={
                "id": next_id,
                "parent": dependency.parent_series_id,
                "child": dependency.child_series_id,
                "weight": dependency.weight or 1.0,
                "formula": dependency.formula or "",
                "now": now,
            },
        )

        return next_id

    def get_parent_dependencies(self, child_series_id: int) -> List[Dict[str, Any]]:
        """Get all parent dependencies for a child series."""
        query = """
        SELECT * FROM seriesDependencyGraph
        WHERE child_series_id = {child:UInt32}
        ORDER BY parent_series_id
        """
        result = self.clickhouse.execute_query(query, parameters={"child": child_series_id})
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        return []

    def get_child_dependencies(self, parent_series_id: int) -> List[Dict[str, Any]]:
        """Get all child dependencies for a parent series."""
        query = """
        SELECT * FROM seriesDependencyGraph
        WHERE parent_series_id = {parent:UInt32}
        ORDER BY child_series_id
        """
        result = self.clickhouse.execute_query(query, parameters={"parent": parent_series_id})
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
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

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def create_calculation_log(self, calculation: CalculationLogBase) -> int:
        """Create a new calculation log entry."""
        # Get next calculation_id
        result = self.clickhouse.execute_query("SELECT max(calculation_id) FROM calculationLog")
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_id = result.result_rows[0][0] + 1
        else:
            next_id = 1

        query = """
        INSERT INTO calculationLog (
            calculation_id, series_id, calculation_type, status,
            input_series_ids, parameters, formula, rows_processed, error_message, created_at
        ) VALUES (
            {id:UInt64}, {series_id:UInt32}, {type:String}, {status:String},
            {inputs:Array(UInt32)}, {params:String}, {formula:String}, {rows:UInt64}, {error:String}, {now:DateTime64(3)}
        )
        """

        self.clickhouse.execute_command(
            query,
            parameters={
                "id": next_id,
                "series_id": calculation.series_id,
                "type": calculation.calculation_type,
                "status": str(calculation.status.value),
                "inputs": calculation.input_series_ids,
                "params": calculation.parameters or "",
                "formula": calculation.formula or "",
                "rows": calculation.rows_processed or 0,
                "error": calculation.error_message or "",
                "now": utc_now_metadata(),
            },
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
        updates = ["status = {status:String}"]
        params = {
            "id": calculation_id,
            "status": str(status.value),
        }

        if rows_processed is not None:
            updates.append("rows_processed = {rows:UInt64}")
            params["rows"] = rows_processed

        if error_message is not None:
            updates.append("error_message = {error:String}")
            params["error"] = error_message

        query = f"""
        ALTER TABLE calculationLog
        UPDATE {', '.join(updates)}
        WHERE calculation_id = {{id:UInt64}}
        """

        self.clickhouse.execute_command(query, parameters=params)

    def get_calculation_logs(
        self,
        series_id: Optional[int] = None,
        status: Optional[CalculationStatus] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get calculation logs with optional filters."""
        query = "SELECT * FROM calculationLog WHERE 1=1"
        params = {}

        if series_id:
            query += " AND series_id = {series_id:UInt32}"
            params["series_id"] = series_id

        if status:
            query += " AND status = {status:String}"
            params["status"] = str(status.value)

        query += " ORDER BY created_at DESC LIMIT {limit:UInt32}"
        params["limit"] = limit

        result = self.clickhouse.execute_query(query, parameters=params)
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        return []
