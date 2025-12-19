"""Meta series management for the financial platform."""

from typing import Optional, List, Dict, Any
from datetime import datetime
from dagster_clickhouse.resources import ClickHouseResource
from database.models import MetaSeriesCreate, MetaSeries, DataSource


class MetaSeriesManager:
    """Manager for meta series operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def create_meta_series(self, meta_series: MetaSeriesCreate, created_by: Optional[str] = None) -> int:
        """Create a new meta series."""
        now = datetime.now()

        # Get next series_id
        result = self.clickhouse.execute_query("SELECT max(series_id) FROM metaSeries")
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_id = result.result_rows[0][0] + 1
        else:
            next_id = 1

        query = """
        INSERT INTO metaSeries (
            series_id, series_name, series_code, data_source, field_type_id,
            asset_class_id, sub_asset_class_id, product_type_id, data_type_id,
            structure_type_id, market_segment_id, ticker_source_id, ticker,
            is_active, is_latest, version, valid_from, valid_to,
            calculation_formula, description, created_at, updated_at, created_by
        ) VALUES (
            {id:UInt32}, {name:String}, {code:String}, {source:String}, {field_id:UInt32},
            {asset_id:UInt32}, {sub_asset_id:UInt32}, {product_id:UInt32}, {data_id:UInt32},
            {struct_id:UInt32}, {market_id:UInt32}, {ticker_id:UInt32}, {ticker:String},
            {active:UInt8}, {latest:UInt8}, {version:UInt32}, {valid_from:DateTime64(6)}, {valid_to:DateTime64(6)},
            {formula:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)}, {created_by:String}
        )
        """

        self.clickhouse.execute_command(
            query,
            parameters={
                "id": next_id,
                "name": meta_series.series_name,
                "code": meta_series.series_code,
                "source": meta_series.data_source.value,
                "field_id": meta_series.field_type_id,
                "asset_id": meta_series.asset_class_id,
                "sub_asset_id": meta_series.sub_asset_class_id,
                "product_id": meta_series.product_type_id,
                "data_id": meta_series.data_type_id,
                "struct_id": meta_series.structure_type_id,
                "market_id": meta_series.market_segment_id,
                "ticker_id": meta_series.ticker_source_id,
                "ticker": meta_series.ticker,
                "active": 1,
                "latest": 1,
                "version": 1,
                "valid_from": meta_series.valid_from,
                "valid_to": meta_series.valid_to,
                "formula": meta_series.calculation_formula,
                "desc": meta_series.description,
                "now": now,
                "created_by": created_by,
            },
        )

        return next_id

    def get_meta_series(self, series_id: int) -> Optional[Dict[str, Any]]:
        """Get a meta series by ID."""
        query = "SELECT * FROM metaSeries WHERE series_id = {id:UInt32} AND is_latest = 1 LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"id": series_id})
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            return dict(zip(columns, result.result_rows[0]))
        return None

    def get_meta_series_by_code(self, series_code: str) -> Optional[Dict[str, Any]]:
        """Get a meta series by code."""
        query = "SELECT * FROM metaSeries WHERE series_code = {code:String} AND is_latest = 1 LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"code": series_code})
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            return dict(zip(columns, result.result_rows[0]))
        return None

    def get_active_series(
        self,
        data_source: Optional[DataSource] = None,
        asset_class_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Get active meta series with optional filters."""
        query = "SELECT * FROM metaSeries WHERE is_active = 1 AND is_latest = 1"
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
        if hasattr(result, "column_names") and hasattr(result, "result_rows"):
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        return []

    def update_data_quality_score(
        self, series_id: int, score: float, updated_by: Optional[str] = None
    ) -> None:
        """Update data quality score for a series."""
        now = datetime.now()
        query = """
        ALTER TABLE metaSeries
        UPDATE data_quality_score = {score:Float64},
               updated_at = {now:DateTime64(3)},
               updated_by = {updated_by:String}
        WHERE series_id = {id:UInt32} AND is_latest = 1
        """
        self.clickhouse.execute_command(
            query,
            parameters={"id": series_id, "score": score, "now": now, "updated_by": updated_by},
        )

    def deactivate_series(self, series_id: int, updated_by: Optional[str] = None) -> None:
        """Deactivate a series."""
        now = datetime.now()
        query = """
        ALTER TABLE metaSeries
        UPDATE is_active = 0,
               updated_at = {now:DateTime64(3)},
               updated_by = {updated_by:String}
        WHERE series_id = {id:UInt32} AND is_latest = 1
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
        query = """
        ALTER TABLE metaSeries
        UPDATE is_latest = 0,
               updated_at = {now:DateTime64(3)}
        WHERE series_id = {id:UInt32} AND is_latest = 1
        """
        self.clickhouse.execute_command(query, parameters={"id": series_id, "now": now})

        # Get next version number
        result = self.clickhouse.execute_query(
            "SELECT max(version) FROM metaSeries WHERE series_id = {id:UInt32}",
            parameters={"id": series_id},
        )
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            next_version = result.result_rows[0][0] + 1
        else:
            next_version = 1

        # Create new version with same series_id
        query = """
        INSERT INTO metaSeries (
            series_id, series_name, series_code, data_source, field_type_id,
            asset_class_id, sub_asset_class_id, product_type_id, data_type_id,
            structure_type_id, market_segment_id, ticker_source_id, ticker,
            is_active, is_latest, version, valid_from, valid_to,
            calculation_formula, description, created_at, updated_at, created_by
        ) VALUES (
            {id:UInt32}, {name:String}, {code:String}, {source:String}, {field_id:UInt32},
            {asset_id:UInt32}, {sub_asset_id:UInt32}, {product_id:UInt32}, {data_id:UInt32},
            {struct_id:UInt32}, {market_id:UInt32}, {ticker_id:UInt32}, {ticker:String},
            {active:UInt8}, {latest:UInt8}, {version:UInt32}, {valid_from:DateTime64(6)}, {valid_to:DateTime64(6)},
            {formula:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)}, {created_by:String}
        )
        """

        self.clickhouse.execute_command(
            query,
            parameters={
                "id": series_id,
                "name": meta_series.series_name,
                "code": meta_series.series_code,
                "source": meta_series.data_source.value,
                "field_id": meta_series.field_type_id,
                "asset_id": meta_series.asset_class_id,
                "sub_asset_id": meta_series.sub_asset_class_id,
                "product_id": meta_series.product_type_id,
                "data_id": meta_series.data_type_id,
                "struct_id": meta_series.structure_type_id,
                "market_id": meta_series.market_segment_id,
                "ticker_id": meta_series.ticker_source_id,
                "ticker": meta_series.ticker,
                "active": 1,
                "latest": 1,
                "version": next_version,
                "valid_from": meta_series.valid_from,
                "valid_to": meta_series.valid_to,
                "formula": meta_series.calculation_formula,
                "desc": meta_series.description,
                "now": now,
                "created_by": created_by,
            },
        )

        return series_id

