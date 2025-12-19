"""Lookup table management for the financial platform."""

from typing import Optional, List, Dict, Any
from datetime import datetime
from dagster_clickhouse.resources import ClickHouseResource
from database.models import (
    AssetClassLookup,
    ProductTypeLookup,
    SubAssetClassLookup,
    DataTypeLookup,
    StructureTypeLookup,
    MarketSegmentLookup,
    FieldTypeLookup,
    TickerSourceLookup,
)


class LookupTableManager:
    """Manager for lookup table operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def insert_asset_class(self, asset_class: AssetClassLookup) -> int:
        """Insert or update an asset class."""
        now = datetime.now()
        if asset_class.asset_class_id:
            # Update
            query = """
            ALTER TABLE assetClassLookup
            UPDATE asset_class_name = {name:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE asset_class_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": asset_class.asset_class_id,
                    "name": asset_class.name,
                    "desc": asset_class.description,
                    "now": now,
                },
            )
            return asset_class.asset_class_id
        else:
            # Insert
            query = """
            INSERT INTO assetClassLookup (asset_class_name, description, created_at, updated_at)
            VALUES ({name:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={"name": asset_class.name, "desc": asset_class.description, "now": now},
            )
            # Get the inserted ID
            result = self.clickhouse.execute_query(
                "SELECT max(asset_class_id) FROM assetClassLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def get_asset_class_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get asset class by name."""
        query = "SELECT * FROM assetClassLookup WHERE asset_class_name = {name:String} LIMIT 1"
        result = self.clickhouse.execute_query(query, parameters={"name": name})
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            return dict(zip(columns, result.result_rows[0]))
        return None

    def insert_product_type(self, product_type: ProductTypeLookup) -> int:
        """Insert or update a product type."""
        now = datetime.now()
        if product_type.product_type_id:
            query = """
            ALTER TABLE productTypeLookup
            UPDATE product_type_name = {name:String},
                   is_derived = {derived:UInt8},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE product_type_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": product_type.product_type_id,
                    "name": product_type.name,
                    "derived": 1 if product_type.is_derived else 0,
                    "desc": product_type.description,
                    "now": now,
                },
            )
            return product_type.product_type_id
        else:
            query = """
            INSERT INTO productTypeLookup (product_type_name, is_derived, description, created_at, updated_at)
            VALUES ({name:String}, {derived:UInt8}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "name": product_type.name,
                    "derived": 1 if product_type.is_derived else 0,
                    "desc": product_type.description,
                    "now": now,
                },
            )
            result = self.clickhouse.execute_query(
                "SELECT max(product_type_id) FROM productTypeLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_sub_asset_class(self, sub_asset_class: SubAssetClassLookup) -> int:
        """Insert or update a sub-asset class."""
        now = datetime.now()
        if sub_asset_class.sub_asset_class_id:
            query = """
            ALTER TABLE subAssetClassLookup
            UPDATE sub_asset_class_name = {name:String},
                   asset_class_id = {asset_id:UInt32},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE sub_asset_class_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": sub_asset_class.sub_asset_class_id,
                    "name": sub_asset_class.name,
                    "asset_id": sub_asset_class.asset_class_id,
                    "desc": sub_asset_class.description,
                    "now": now,
                },
            )
            return sub_asset_class.sub_asset_class_id
        else:
            query = """
            INSERT INTO subAssetClassLookup (sub_asset_class_name, asset_class_id, description, created_at, updated_at)
            VALUES ({name:String}, {asset_id:UInt32}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "name": sub_asset_class.name,
                    "asset_id": sub_asset_class.asset_class_id,
                    "desc": sub_asset_class.description,
                    "now": now,
                },
            )
            result = self.clickhouse.execute_query(
                "SELECT max(sub_asset_class_id) FROM subAssetClassLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_data_type(self, data_type: DataTypeLookup) -> int:
        """Insert or update a data type."""
        now = datetime.now()
        if data_type.data_type_id:
            query = """
            ALTER TABLE dataTypeLookup
            UPDATE data_type_name = {name:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE data_type_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": data_type.data_type_id,
                    "name": data_type.name,
                    "desc": data_type.description,
                    "now": now,
                },
            )
            return data_type.data_type_id
        else:
            query = """
            INSERT INTO dataTypeLookup (data_type_name, description, created_at, updated_at)
            VALUES ({name:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={"name": data_type.name, "desc": data_type.description, "now": now},
            )
            result = self.clickhouse.execute_query("SELECT max(data_type_id) FROM dataTypeLookup")
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_structure_type(self, structure_type: StructureTypeLookup) -> int:
        """Insert or update a structure type."""
        now = datetime.now()
        if structure_type.structure_type_id:
            query = """
            ALTER TABLE structureTypeLookup
            UPDATE structure_type_name = {name:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE structure_type_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": structure_type.structure_type_id,
                    "name": structure_type.name,
                    "desc": structure_type.description,
                    "now": now,
                },
            )
            return structure_type.structure_type_id
        else:
            query = """
            INSERT INTO structureTypeLookup (structure_type_name, description, created_at, updated_at)
            VALUES ({name:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={"name": structure_type.name, "desc": structure_type.description, "now": now},
            )
            result = self.clickhouse.execute_query(
                "SELECT max(structure_type_id) FROM structureTypeLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_market_segment(self, market_segment: MarketSegmentLookup) -> int:
        """Insert or update a market segment."""
        now = datetime.now()
        if market_segment.market_segment_id:
            query = """
            ALTER TABLE marketSegmentLookup
            UPDATE market_segment_name = {name:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE market_segment_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": market_segment.market_segment_id,
                    "name": market_segment.name,
                    "desc": market_segment.description,
                    "now": now,
                },
            )
            return market_segment.market_segment_id
        else:
            query = """
            INSERT INTO marketSegmentLookup (market_segment_name, description, created_at, updated_at)
            VALUES ({name:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={"name": market_segment.name, "desc": market_segment.description, "now": now},
            )
            result = self.clickhouse.execute_query(
                "SELECT max(market_segment_id) FROM marketSegmentLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_field_type(self, field_type: FieldTypeLookup) -> int:
        """Insert or update a field type."""
        now = datetime.now()
        if field_type.field_type_id:
            query = """
            ALTER TABLE fieldTypeLookup
            UPDATE field_type_name = {name:String},
                   field_type_code = {code:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE field_type_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": field_type.field_type_id,
                    "name": field_type.name,
                    "code": field_type.field_type_code,
                    "desc": field_type.description,
                    "now": now,
                },
            )
            return field_type.field_type_id
        else:
            query = """
            INSERT INTO fieldTypeLookup (field_type_name, field_type_code, description, created_at, updated_at)
            VALUES ({name:String}, {code:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "name": field_type.name,
                    "code": field_type.field_type_code,
                    "desc": field_type.description,
                    "now": now,
                },
            )
            result = self.clickhouse.execute_query("SELECT max(field_type_id) FROM fieldTypeLookup")
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

    def insert_ticker_source(self, ticker_source: TickerSourceLookup) -> int:
        """Insert or update a ticker source."""
        now = datetime.now()
        if ticker_source.ticker_source_id:
            query = """
            ALTER TABLE tickerSourceLookup
            UPDATE ticker_source_name = {name:String},
                   ticker_source_code = {code:String},
                   description = {desc:String},
                   updated_at = {now:DateTime64(3)}
            WHERE ticker_source_id = {id:UInt32}
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "id": ticker_source.ticker_source_id,
                    "name": ticker_source.name,
                    "code": ticker_source.ticker_source_code,
                    "desc": ticker_source.description,
                    "now": now,
                },
            )
            return ticker_source.ticker_source_id
        else:
            query = """
            INSERT INTO tickerSourceLookup (ticker_source_name, ticker_source_code, description, created_at, updated_at)
            VALUES ({name:String}, {code:String}, {desc:String}, {now:DateTime64(3)}, {now:DateTime64(3)})
            """
            self.clickhouse.execute_command(
                query,
                parameters={
                    "name": ticker_source.name,
                    "code": ticker_source.ticker_source_code,
                    "desc": ticker_source.description,
                    "now": now,
                },
            )
            result = self.clickhouse.execute_query(
                "SELECT max(ticker_source_id) FROM tickerSourceLookup"
            )
            if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]
            return 1

