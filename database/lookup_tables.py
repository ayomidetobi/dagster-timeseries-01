"""Lookup table management for the financial platform."""

from typing import Any, Dict, Optional

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import DB_COLUMNS, DB_TABLES
from dagster_quickstart.utils.datetime_utils import utc_now_metadata
from dagster_quickstart.utils.exceptions import DatabaseError
from database.models import (
    AssetClassLookup,
    CountryLookup,
    CurrencyLookup,
    DataTypeLookup,
    FieldTypeLookup,
    MarketSegmentLookup,
    ProductTypeLookup,
    RegionLookup,
    StructureTypeLookup,
    SubAssetClassLookup,
    TenorLookup,
    TermLookup,
    TickerSourceLookup,
)
from database.utils import (
    execute_insert_query,
    execute_update_query,
    get_by_name,
    get_next_id,
)


class LookupTableManager:
    """Manager for lookup table operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def _insert_or_update_lookup(
        self,
        lookup_type: str,
        record_id: Optional[int],
        name: str,
        description: Optional[str],
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Generic method to insert or update a lookup record.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            record_id: Optional existing ID for update
            name: Name of the lookup
            description: Optional description
            extra_fields: Optional extra fields (e.g., {"is_derived": 0, "field_type_code": "PX_LAST"})

        Returns:
            The ID of the record (existing or newly created)
        """
        table_name = DB_TABLES[lookup_type]
        id_column, name_column = DB_COLUMNS[lookup_type]
        now = utc_now_metadata()

        if record_id:
            # Update existing record
            update_fields = {name_column: name}
            if description is not None:
                update_fields["description"] = description
            if extra_fields:
                update_fields.update(extra_fields)

            try:
                execute_update_query(
                    self.clickhouse,
                    table_name,
                    id_column,
                    record_id,
                    update_fields,
                    now,
                )
                return record_id
            except Exception as e:
                raise DatabaseError(f"Failed to update {lookup_type}: {e}") from e
        else:
            # Insert new record
            next_id = get_next_id(self.clickhouse, table_name, id_column)
            insert_fields = {name_column: name}
            if description is not None:
                insert_fields["description"] = description
            if extra_fields:
                insert_fields.update(extra_fields)

            try:
                execute_insert_query(
                    self.clickhouse,
                    table_name,
                    id_column,
                    next_id,
                    insert_fields,
                    now,
                )
                return next_id
            except Exception as e:
                raise DatabaseError(f"Failed to insert {lookup_type}: {e}") from e

    def _get_lookup_by_name(self, lookup_type: str, name: str) -> Optional[Dict[str, Any]]:
        """Generic method to get a lookup record by name.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            name: Name to search for

        Returns:
            Dictionary with record data or None if not found
        """
        table_name = DB_TABLES[lookup_type]
        _, name_column = DB_COLUMNS[lookup_type]
        return get_by_name(self.clickhouse, table_name, name_column, name)

    # Asset Class methods
    def insert_asset_class(self, asset_class: AssetClassLookup) -> int:
        """Insert or update an asset class."""
        return self._insert_or_update_lookup(
            "asset_class",
            asset_class.asset_class_id,
            asset_class.name,
            asset_class.description,
        )

    def get_asset_class_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get asset class by name."""
        return self._get_lookup_by_name("asset_class", name)

    # Product Type methods
    def insert_product_type(self, product_type: ProductTypeLookup) -> int:
        """Insert or update a product type."""
        return self._insert_or_update_lookup(
            "product_type",
            product_type.product_type_id,
            product_type.name,
            product_type.description,
            extra_fields={"is_derived": 1 if product_type.is_derived else 0},
        )

    def get_product_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get product type by name."""
        return self._get_lookup_by_name("product_type", name)

    # Sub Asset Class methods
    def insert_sub_asset_class(self, sub_asset_class: SubAssetClassLookup) -> int:
        """Insert or update a sub-asset class."""
        return self._insert_or_update_lookup(
            "sub_asset_class",
            sub_asset_class.sub_asset_class_id,
            sub_asset_class.name,
            sub_asset_class.description,
            extra_fields={"asset_class_id": sub_asset_class.asset_class_id},
        )

    def get_sub_asset_class_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get sub-asset class by name."""
        return self._get_lookup_by_name("sub_asset_class", name)

    # Data Type methods
    def insert_data_type(self, data_type: DataTypeLookup) -> int:
        """Insert or update a data type."""
        return self._insert_or_update_lookup(
            "data_type",
            data_type.data_type_id,
            data_type.name,
            data_type.description,
        )

    def get_data_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get data type by name."""
        return self._get_lookup_by_name("data_type", name)

    # Structure Type methods
    def insert_structure_type(self, structure_type: StructureTypeLookup) -> int:
        """Insert or update a structure type."""
        return self._insert_or_update_lookup(
            "structure_type",
            structure_type.structure_type_id,
            structure_type.name,
            structure_type.description,
        )

    def get_structure_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get structure type by name."""
        return self._get_lookup_by_name("structure_type", name)

    # Market Segment methods
    def insert_market_segment(self, market_segment: MarketSegmentLookup) -> int:
        """Insert or update a market segment."""
        return self._insert_or_update_lookup(
            "market_segment",
            market_segment.market_segment_id,
            market_segment.name,
            market_segment.description,
        )

    def get_market_segment_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get market segment by name."""
        return self._get_lookup_by_name("market_segment", name)

    # Field Type methods
    def insert_field_type(self, field_type: FieldTypeLookup) -> int:
        """Insert or update a field type."""
        return self._insert_or_update_lookup(
            "field_type",
            field_type.field_type_id,
            field_type.name,
            field_type.description,
            extra_fields={"field_type_code": field_type.field_type_code},
        )

    def get_field_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get field type by name."""
        return self._get_lookup_by_name("field_type", name)

    # Ticker Source methods
    def insert_ticker_source(self, ticker_source: TickerSourceLookup) -> int:
        """Insert or update a ticker source."""
        return self._insert_or_update_lookup(
            "ticker_source",
            ticker_source.ticker_source_id,
            ticker_source.name,
            ticker_source.description,
            extra_fields={"ticker_source_code": ticker_source.ticker_source_code},
        )

    def get_ticker_source_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get ticker source by name."""
        return self._get_lookup_by_name("ticker_source", name)

    def get_ticker_source_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get ticker source by code."""
        table_name = DB_TABLES["ticker_source"]
        return get_by_name(self.clickhouse, table_name, "ticker_source_code", code)

    # Region methods
    def insert_region(self, region: RegionLookup) -> int:
        """Insert or update a region."""
        return self._insert_or_update_lookup(
            "region",
            region.region_id,
            region.name,
            region.description,
        )

    def get_region_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get region by name."""
        return self._get_lookup_by_name("region", name)

    # Currency methods
    def insert_currency(self, currency: CurrencyLookup) -> int:
        """Insert or update a currency."""
        return self._insert_or_update_lookup(
            "currency",
            currency.currency_id,
            currency.currency_code,
            currency.description,
            extra_fields={"currency_name": currency.currency_name},
        )

    def get_currency_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get currency by code."""
        table_name = DB_TABLES["currency"]
        return get_by_name(self.clickhouse, table_name, "currency_code", code)

    # Term methods
    def insert_term(self, term: TermLookup) -> int:
        """Insert or update a term."""
        return self._insert_or_update_lookup(
            "term",
            term.term_id,
            term.name,
            term.description,
        )

    def get_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get term by name."""
        return self._get_lookup_by_name("term", name)

    # Tenor methods
    def insert_tenor(self, tenor: TenorLookup) -> int:
        """Insert or update a tenor."""
        return self._insert_or_update_lookup(
            "tenor",
            tenor.tenor_id,
            tenor.tenor_code,
            tenor.description,
            extra_fields={"tenor_name": tenor.tenor_name},
        )

    def get_tenor_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get tenor by code."""
        table_name = DB_TABLES["tenor"]
        return get_by_name(self.clickhouse, table_name, "tenor_code", code)

    # Country methods
    def insert_country(self, country: CountryLookup) -> int:
        """Insert or update a country."""
        return self._insert_or_update_lookup(
            "country",
            country.country_id,
            country.country_code,
            country.description,
            extra_fields={"country_name": country.country_name},
        )

    def get_country_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get country by code."""
        table_name = DB_TABLES["country"]
        return get_by_name(self.clickhouse, table_name, "country_code", code)
