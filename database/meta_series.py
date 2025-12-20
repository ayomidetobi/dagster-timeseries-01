"""Meta series management for the financial platform."""

from typing import Optional, List, Dict, Any
from datetime import datetime
from dagster_clickhouse.resources import ClickHouseResource
from database.models import MetaSeriesCreate, MetaSeries, DataSource
from database.utils import get_next_id, query_to_dict, query_to_dict_list

# Constants to avoid circular imports
META_SERIES_TABLE = "metaSeries"
QUERY_LIMIT_DEFAULT = 1000


class MetaSeriesManager:
    """Manager for meta series operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def create_meta_series(self, meta_series: MetaSeriesCreate, created_by: Optional[str] = None) -> int:
        """Create a new meta series."""
        now = datetime.now()

        # Get next series_id
        next_id = get_next_id(self.clickhouse, META_SERIES_TABLE, "series_id")

        # Build query with NULL handling for optional ID fields
        field_id_val = f"{{field_id:UInt32}}" if meta_series.field_type_id is not None else "NULL"
        asset_id_val = f"{{asset_id:UInt32}}" if meta_series.asset_class_id is not None else "NULL"
        sub_asset_id_val = f"{{sub_asset_id:UInt32}}" if meta_series.sub_asset_class_id is not None else "NULL"
        product_id_val = f"{{product_id:UInt32}}" if meta_series.product_type_id is not None else "NULL"
        data_id_val = f"{{data_id:UInt32}}" if meta_series.data_type_id is not None else "NULL"
        struct_id_val = f"{{struct_id:UInt32}}" if meta_series.structure_type_id is not None else "NULL"
        market_id_val = f"{{market_id:UInt32}}" if meta_series.market_segment_id is not None else "NULL"
        ticker_id_val = f"{{ticker_id:UInt32}}" if meta_series.ticker_source_id is not None else "NULL"

        query = f"""
        INSERT INTO {META_SERIES_TABLE} (
            series_id, series_name, series_code, data_source, field_type_id,
            asset_class_id, sub_asset_class_id, product_type_id, data_type_id,
            structure_type_id, market_segment_id, ticker_source_id, ticker,
            is_active, is_latest, version,
            calculation_formula, description, created_at, updated_at, created_by
        ) VALUES (
            {{id:UInt32}}, {{name:String}}, {{code:String}}, {{source:String}}, {field_id_val},
            {asset_id_val}, {sub_asset_id_val}, {product_id_val}, {data_id_val},
            {struct_id_val}, {market_id_val}, {ticker_id_val}, {{ticker:String}},
            {{active:UInt8}}, {{latest:UInt8}}, {{version:UInt32}},
            {{formula:String}}, {{desc:String}}, {{now:DateTime64(3)}}, {{now:DateTime64(3)}}, {{created_by:String}}
        )
        """
        params = {
            "id": next_id,
            "name": meta_series.series_name,
            "code": meta_series.series_code,
            "source": meta_series.data_source.value,
            "ticker": meta_series.ticker,
            "active": 1,
            "latest": 1,
            "version": 1,
            "formula": meta_series.calculation_formula,
            "desc": meta_series.description,
            "now": now,
            "created_by": created_by,
        }
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
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_id = {{id:UInt32}} AND is_latest = 1 LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"id": series_id})
        return query_to_dict(result)

    def get_meta_series_by_code(self, series_code: str) -> Optional[Dict[str, Any]]:
        """Get a meta series by code."""
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_code = {{code:String}} AND is_latest = 1 LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"code": series_code})
        return query_to_dict(result)

    def get_active_series(
        self,
        data_source: Optional[DataSource] = None,
        asset_class_id: Optional[int] = None,
        limit: int = QUERY_LIMIT_DEFAULT,
    ) -> List[Dict[str, Any]]:
        """Get active meta series with optional filters."""
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE is_active = 1 AND is_latest = 1"
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

    def update_data_quality_score(
        self, series_id: int, score: float, updated_by: Optional[str] = None
    ) -> None:
        """Update data quality score for a series."""
        now = datetime.now()
        query = f"""
        ALTER TABLE {META_SERIES_TABLE}
        UPDATE data_quality_score = {{score:Float64}},
               updated_at = {{now:DateTime64(3)}},
               updated_by = {{updated_by:String}}
        WHERE series_id = {{id:UInt32}} AND is_latest = 1
        """
        self.clickhouse.execute_command(
            query,
            parameters={"id": series_id, "score": score, "now": now, "updated_by": updated_by},
        )

    def deactivate_series(self, series_id: int, updated_by: Optional[str] = None) -> None:
        """Deactivate a series."""
        now = datetime.now()
        query = f"""
        ALTER TABLE {META_SERIES_TABLE}
        UPDATE is_active = 0,
               updated_at = {{now:DateTime64(3)}},
               updated_by = {{updated_by:String}}
        WHERE series_id = {{id:UInt32}} AND is_latest = 1
        """
        self.clickhouse.execute_command(
            query, parameters={"id": series_id, "now": now, "updated_by": updated_by}
        )

    def create_new_version(
        self, series_id: int, meta_series: MetaSeriesCreate, created_by: Optional[str] = None
    ) -> int:
        """Create a new version of an existing series."""
        # Mark old version as not latest
        now = datetime.now()
        query = f"""
        ALTER TABLE {META_SERIES_TABLE}
        UPDATE is_latest = 0,
               updated_at = {{now:DateTime64(3)}}
        WHERE series_id = {{id:UInt32}} AND is_latest = 1
        """
        self.clickhouse.execute_command(query, parameters={"id": series_id, "now": now})

        # Get next version number
        result = self.clickhouse.execute_query(
            f"SELECT max(version) FROM {META_SERIES_TABLE} WHERE series_id = {{id:UInt32}}",
            parameters={"id": series_id},
        )
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_version = result.result_rows[0][0] + 1
        else:
            next_version = 1

        # Build query with NULL handling for optional ID fields
        field_id_val = f"{{field_id:UInt32}}" if meta_series.field_type_id is not None else "NULL"
        asset_id_val = f"{{asset_id:UInt32}}" if meta_series.asset_class_id is not None else "NULL"
        sub_asset_id_val = f"{{sub_asset_id:UInt32}}" if meta_series.sub_asset_class_id is not None else "NULL"
        product_id_val = f"{{product_id:UInt32}}" if meta_series.product_type_id is not None else "NULL"
        data_id_val = f"{{data_id:UInt32}}" if meta_series.data_type_id is not None else "NULL"
        struct_id_val = f"{{struct_id:UInt32}}" if meta_series.structure_type_id is not None else "NULL"
        market_id_val = f"{{market_id:UInt32}}" if meta_series.market_segment_id is not None else "NULL"
        ticker_id_val = f"{{ticker_id:UInt32}}" if meta_series.ticker_source_id is not None else "NULL"

        query = f"""
        INSERT INTO {META_SERIES_TABLE} (
            series_id, series_name, series_code, data_source, field_type_id,
            asset_class_id, sub_asset_class_id, product_type_id, data_type_id,
            structure_type_id, market_segment_id, ticker_source_id, ticker,
            is_active, is_latest, version,
            calculation_formula, description, created_at, updated_at, created_by
        ) VALUES (
            {{id:UInt32}}, {{name:String}}, {{code:String}}, {{source:String}}, {field_id_val},
            {asset_id_val}, {sub_asset_id_val}, {product_id_val}, {data_id_val},
            {struct_id_val}, {market_id_val}, {ticker_id_val}, {{ticker:String}},
            {{active:UInt8}}, {{latest:UInt8}}, {{version:UInt32}},
            {{formula:String}}, {{desc:String}}, {{now:DateTime64(3)}}, {{now:DateTime64(3)}}, {{created_by:String}}
        )
        """
        params = {
            "id": series_id,
            "name": meta_series.series_name,
            "code": meta_series.series_code,
            "source": meta_series.data_source.value,
            "ticker": meta_series.ticker,
            "active": 1,
            "latest": 1,
            "version": next_version,
            "formula": meta_series.calculation_formula,
            "desc": meta_series.description,
            "now": now,
            "created_by": created_by,
        }
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

        return series_id

