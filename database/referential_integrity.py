"""Referential integrity validation for meta series and lookup table references."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    LOOKUP_TABLE_PROCESSING_ORDER,
)
from dagster_quickstart.utils.exceptions import InvalidLookupReferenceError


@dataclass
class LookupValidationError:
    """Structured error record for a single invalid lookup reference."""

    lookup_type: str
    lookup_value: str
    series_code: str

    def to_exception(self) -> InvalidLookupReferenceError:
        """Convert to InvalidLookupReferenceError exception."""
        return InvalidLookupReferenceError(
            lookup_type=self.lookup_type,
            lookup_value=self.lookup_value,
            series_code=self.series_code,
        )

    def __str__(self) -> str:
        """String representation for error messages."""
        if self.series_code:
            return f"Series '{self.series_code}' references invalid {self.lookup_type} '{self.lookup_value}'"
        return f"Invalid {self.lookup_type} reference: '{self.lookup_value}'"


class ReferentialIntegrityValidator:
    """Validates referential integrity for meta series lookup references.

    This validator ensures that all lookup values referenced in meta series
    exist in their respective lookup tables before insertion.
    """

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize the validator with a ClickHouse resource.

        Args:
            clickhouse: ClickHouse resource for database queries
        """
        self.clickhouse = clickhouse
        self._lookup_cache: Dict[str, Set[str]] = {}

    def _get_query_field(self, lookup_type: str) -> str:
        """Determine which database column to query for a given lookup type.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class", "field_type")

        Returns:
            Name of the database column to query for validation
        """
        if lookup_type in CODE_BASED_LOOKUPS:
            # For code-based lookups, use the check_field (which field is used for validation)
            _, _, check_field = CODE_BASED_LOOKUPS[lookup_type]
            return check_field

        # For simple lookups, use the name column
        _, name_column = DB_COLUMNS[lookup_type]
        return name_column

    def _get_lookup_values(self, lookup_type: str) -> Set[str]:
        """Get all valid lookup values for a given lookup type.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class", "field_type")

        Returns:
            Set of valid lookup values (names or codes depending on lookup type)
        """
        if lookup_type in self._lookup_cache:
            return self._lookup_cache[lookup_type]

        table_name = DB_TABLES[lookup_type]
        query_field = self._get_query_field(lookup_type)

        query = (
            f"SELECT DISTINCT {query_field} FROM {table_name} "
            f"WHERE {query_field} IS NOT NULL AND {query_field} != ''"
        )
        result = self.clickhouse.execute_query(query)

        values: Set[str] = set()
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                if row[0] is not None:
                    values.add(str(row[0]).strip())

        self._lookup_cache[lookup_type] = values
        return values

    def validate_lookup_reference(
        self, lookup_type: str, lookup_value: str, series_code: str = ""
    ) -> Optional[LookupValidationError]:
        """Validate that a lookup value exists in its lookup table.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class", "field_type")
            lookup_value: The lookup value to validate
            series_code: Optional series code for error messages

        Returns:
            LookupValidationError if validation fails, None if valid
        """
        if not lookup_value or lookup_value.strip() == "":
            return None  # NULL/empty values are allowed

        valid_values = self._get_lookup_values(lookup_type)
        lookup_value_clean = lookup_value.strip()

        if lookup_value_clean not in valid_values:
            return LookupValidationError(
                lookup_type=lookup_type,
                lookup_value=lookup_value_clean,
                series_code=series_code,
            )

        return None

    def validate_meta_series_references(
        self,
        context: AssetExecutionContext,
        staging_data: List[Dict[str, Optional[str]]],
    ) -> None:
        """Validate all lookup references for meta series data.

        Args:
            context: Dagster execution context for logging
            staging_data: List of dictionaries representing staging table rows
                Each dict should have keys like 'series_code', 'field_type', 'asset_class', etc.

        Raises:
            InvalidLookupReferenceError: If any lookup reference is invalid
        """
        context.log.info("Validating referential integrity for meta series references")

        errors: List[LookupValidationError] = []

        # Use LOOKUP_TABLE_PROCESSING_ORDER from constants instead of hard-coding
        for row in staging_data:
            series_code = str(row.get("series_code", "") or "")

            # Validate all lookup references using the processing order from constants
            for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
                lookup_value = row.get(lookup_type)
                if lookup_value is not None:
                    lookup_value_str = str(lookup_value).strip()
                    if lookup_value_str:
                        error = self.validate_lookup_reference(
                            lookup_type, lookup_value_str, series_code
                        )
                        if error:
                            errors.append(error)

        if errors:
            # Build aggregated error message from structured error records
            error_message = (
                f"Referential integrity validation failed with {len(errors)} error(s):\n"
            )
            error_message += "\n".join(f"  - {error}" for error in errors[:20])
            if len(errors) > 20:
                error_message += f"\n  ... and {len(errors) - 20} more error(s)"

            context.log.error(error_message)

            # Raise a single aggregated exception with details
            raise InvalidLookupReferenceError(
                lookup_type="multiple",
                lookup_value="",
                series_code="",
                message=error_message,
            )

        context.log.info(
            f"Successfully validated referential integrity for {len(staging_data)} meta series records"
        )

    def clear_cache(self) -> None:
        """Clear the lookup value cache."""
        self._lookup_cache.clear()
