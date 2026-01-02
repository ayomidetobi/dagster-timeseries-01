"""Referential integrity validation for meta series and lookup table references."""

from typing import Dict, List, Optional, Set

from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import DB_COLUMNS, DB_TABLES
from dagster_quickstart.utils.exceptions import InvalidLookupReferenceError


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
        _, name_column = DB_COLUMNS[lookup_type]

        # For code-based lookups, we need to get the code field
        if lookup_type in ["currency", "tenor", "country"]:
            code_field_map = {
                "currency": "currency_code",
                "tenor": "tenor_code",
                "country": "country_code",
            }
            query_field = code_field_map[lookup_type]
        elif lookup_type == "ticker_source":
            # ticker_source uses ticker_source_name for lookup
            query_field = name_column
        else:
            # All other lookups use the name column
            query_field = name_column

        query = f"SELECT DISTINCT {query_field} FROM {table_name} WHERE {query_field} IS NOT NULL AND {query_field} != ''"
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
    ) -> None:
        """Validate that a lookup value exists in its lookup table.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class", "field_type")
            lookup_value: The lookup value to validate
            series_code: Optional series code for error messages

        Raises:
            InvalidLookupReferenceError: If the lookup value does not exist
        """
        if not lookup_value or lookup_value.strip() == "":
            return  # NULL/empty values are allowed

        valid_values = self._get_lookup_values(lookup_type)
        if lookup_value.strip() not in valid_values:
            raise InvalidLookupReferenceError(
                lookup_type=lookup_type,
                lookup_value=lookup_value,
                series_code=series_code,
            )

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

        errors: List[str] = []
        lookup_types = [
            "field_type",
            "asset_class",
            "product_type",
            "data_type",
            "structure_type",
            "market_segment",
            "ticker_source",
            "sub_asset_class",
            "region",
            "currency",
            "term",
            "tenor",
            "country",
        ]

        for row in staging_data:
            series_code = row.get("series_code", "")

            # Validate all lookup references (sub_asset_class is now independent)
            for lookup_type in lookup_types:
                lookup_value = row.get(lookup_type)
                if lookup_value is not None:
                    lookup_value_str = str(lookup_value).strip()
                    if lookup_value_str:
                        try:
                            self.validate_lookup_reference(
                                lookup_type, lookup_value_str, series_code
                            )
                        except InvalidLookupReferenceError as e:
                            errors.append(str(e))

        if errors:
            error_message = (
                f"Referential integrity validation failed with {len(errors)} error(s):\n"
            )
            error_message += "\n".join(
                f"  - {error}" for error in errors[:20]
            )  # Limit to 20 errors
            if len(errors) > 20:
                error_message += f"\n  ... and {len(errors) - 20} more error(s)"
            context.log.error(error_message)
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
