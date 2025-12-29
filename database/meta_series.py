"""Meta series management for the financial platform."""

from typing import Any, Dict, List, Optional

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.datetime_utils import utc_now_metadata
from database.models import DataSource, MetaSeriesCreate
from database.utils import get_next_id, query_to_dict, query_to_dict_list

# Constants to avoid circular imports
META_SERIES_TABLE = "metaSeries"
QUERY_LIMIT_DEFAULT = 1000


class MetaSeriesManager:
    """Manager for meta series operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def create_meta_series(
        self, meta_series: MetaSeriesCreate, created_by: Optional[str] = None
    ) -> int:
        """Create a new meta series."""
        now = utc_now_metadata()

        # Get next series_id
        next_id = get_next_id(self.clickhouse, META_SERIES_TABLE, "series_id")

        # Build query with NULL handling for optional ID fields
        field_id_val = "{field_id:UInt32}" if meta_series.field_type_id is not None else "NULL"
        asset_id_val = "{asset_id:UInt32}" if meta_series.asset_class_id is not None else "NULL"
        sub_asset_id_val = (
            "{sub_asset_id:UInt32}" if meta_series.sub_asset_class_id is not None else "NULL"
        )
        product_id_val = (
            "{product_id:UInt32}" if meta_series.product_type_id is not None else "NULL"
        )
        data_id_val = "{data_id:UInt32}" if meta_series.data_type_id is not None else "NULL"
        struct_id_val = (
            "{struct_id:UInt32}" if meta_series.structure_type_id is not None else "NULL"
        )
        market_id_val = (
            "{market_id:UInt32}" if meta_series.market_segment_id is not None else "NULL"
        )
        ticker_id_val = "{ticker_id:UInt32}" if meta_series.ticker_source_id is not None else "NULL"
        region_id_val = "{region_id:UInt32}" if meta_series.region_id is not None else "NULL"
        currency_id_val = "{currency_id:UInt32}" if meta_series.currency_id is not None else "NULL"
        term_id_val = "{term_id:UInt32}" if meta_series.term_id is not None else "NULL"
        tenor_id_val = "{tenor_id:UInt32}" if meta_series.tenor_id is not None else "NULL"
        country_id_val = "{country_id:UInt32}" if meta_series.country_id is not None else "NULL"

        # Convert is_active boolean to UInt8 (1 for True, 0 for False)
        is_active_val = 1 if meta_series.is_active else 0

        query = f"""
        INSERT INTO {META_SERIES_TABLE} (
            series_id, series_name, series_code, data_source, field_type_id,
            asset_class_id, sub_asset_class_id, product_type_id, data_type_id,
            structure_type_id, market_segment_id, ticker_source_id, ticker,
            region_id, currency_id, term_id, tenor_id, country_id,
            calculation_formula, description, is_active, created_at, updated_at, created_by
        ) VALUES (
            {{id:UInt32}}, {{name:String}}, {{code:String}}, {{source:String}}, {field_id_val},
            {asset_id_val}, {sub_asset_id_val}, {product_id_val}, {data_id_val},
            {struct_id_val}, {market_id_val}, {ticker_id_val}, {{ticker:String}},
            {region_id_val}, {currency_id_val}, {term_id_val}, {tenor_id_val}, {country_id_val},
            {{formula:String}}, {{desc:String}}, {is_active_val}, {{now:DateTime64(6)}}, {{now:DateTime64(6)}}, {{created_by:String}}
        )
        """
        params = {
            "id": next_id,
            "name": meta_series.series_name,
            "code": meta_series.series_code,
            "source": meta_series.data_source.value,
            "ticker": meta_series.ticker,
            "formula": meta_series.calculation_formula,
            "desc": meta_series.description,
            "now": now,
            "created_by": created_by,
        }
        # Add optional ID parameters
        if meta_series.region_id is not None:
            params["region_id"] = meta_series.region_id
        if meta_series.currency_id is not None:
            params["currency_id"] = meta_series.currency_id
        if meta_series.term_id is not None:
            params["term_id"] = meta_series.term_id
        if meta_series.tenor_id is not None:
            params["tenor_id"] = meta_series.tenor_id
        if meta_series.country_id is not None:
            params["country_id"] = meta_series.country_id
        # Only add parameters for non-NULL values
        if meta_series.field_type_id is not None:
            params["field_id"] = meta_series.field_type_id
        if meta_series.asset_class_id is not None:
            params["asset_id"] = meta_series.asset_class_id
        if meta_series.sub_asset_class_id is not None:
            params["sub_asset_id"] = meta_series.sub_asset_class_id
        if meta_series.product_type_id is not None:
            params["product_id"] = meta_series.product_type_id
        if meta_series.data_type_id is not None:
            params["data_id"] = meta_series.data_type_id
        if meta_series.structure_type_id is not None:
            params["struct_id"] = meta_series.structure_type_id
        if meta_series.market_segment_id is not None:
            params["market_id"] = meta_series.market_segment_id
        if meta_series.ticker_source_id is not None:
            params["ticker_id"] = meta_series.ticker_source_id

        self.clickhouse.execute_command(query, parameters=params)

        return next_id

    def get_meta_series(self, series_id: int) -> Optional[Dict[str, Any]]:
        """Get a meta series by ID."""
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_id = {{id:UInt32}} LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"id": series_id})
        return query_to_dict(result)

    def get_meta_series_by_code(self, series_code: str) -> Optional[Dict[str, Any]]:
        """Get a meta series by code."""
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_code = {{code:String}} LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"code": series_code})
        return query_to_dict(result)

    def get_active_series(
        self,
        data_source: Optional[DataSource] = None,
        asset_class_id: Optional[int] = None,
        limit: int = QUERY_LIMIT_DEFAULT,
    ) -> List[Dict[str, Any]]:
        """Get active meta series (is_active = 1) with optional filters."""
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE is_active = 1"
        params = {}

        if data_source:
            query += " AND data_source = {source:String}"
            params["source"] = data_source.value

        if asset_class_id:
            query += " AND asset_class_id = {asset_id:UInt32}"
            params["asset_id"] = asset_class_id

        query += " ORDER BY series_id LIMIT {limit:UInt32}"
        params["limit"] = limit

        result = self.clickhouse.execute_query(query, parameters=params)
        return query_to_dict_list(result)
