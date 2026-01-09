"""Initial schema migration for DuckDB.

This migration creates all the base tables for the financial platform.
DuckDB handles referential integrity automatically via foreign keys.
"""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    """Create all base tables for the financial platform."""
    # Lookup Tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS assetClassLookup (
            asset_class_id INTEGER PRIMARY KEY,
            asset_class_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS productTypeLookup (
            product_type_id INTEGER PRIMARY KEY,
            product_type_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS subAssetClassLookup (
            sub_asset_class_id INTEGER PRIMARY KEY,
            sub_asset_class_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataTypeLookup (
            data_type_id INTEGER PRIMARY KEY,
            data_type_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS structureTypeLookup (
            structure_type_id INTEGER PRIMARY KEY,
            structure_type_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketSegmentLookup (
            market_segment_id INTEGER PRIMARY KEY,
            market_segment_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fieldTypeLookup (
            field_type_id INTEGER PRIMARY KEY,
            field_type_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tickerSourceLookup (
            ticker_source_id INTEGER PRIMARY KEY,
            ticker_source_name VARCHAR NOT NULL,
            ticker_source_code VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS regionLookup (
            region_id INTEGER PRIMARY KEY,
            region_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS currencyLookup (
            currency_id INTEGER PRIMARY KEY,
            currency_code VARCHAR NOT NULL,
            currency_name VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS termLookup (
            term_id INTEGER PRIMARY KEY,
            term_name VARCHAR NOT NULL,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tenorLookup (
            tenor_id INTEGER PRIMARY KEY,
            tenor_code VARCHAR NOT NULL,
            tenor_name VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS countryLookup (
            country_id INTEGER PRIMARY KEY,
            country_code VARCHAR NOT NULL,
            country_name VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Meta Series Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metaSeries (
            series_id INTEGER PRIMARY KEY,
            series_name VARCHAR NOT NULL,
            series_code VARCHAR NOT NULL UNIQUE,
            data_source VARCHAR NOT NULL,
            field_type_id INTEGER,
            asset_class_id INTEGER,
            sub_asset_class_id INTEGER,
            product_type_id INTEGER,
            data_type_id INTEGER,
            structure_type_id INTEGER,
            market_segment_id INTEGER,
            ticker_source_id INTEGER,
            ticker VARCHAR NOT NULL,
            region_id INTEGER,
            currency_id INTEGER,
            term_id INTEGER,
            tenor_id INTEGER,
            country_id INTEGER,
            calculation_formula VARCHAR,
            description VARCHAR,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR,
            updated_by VARCHAR,
            FOREIGN KEY (field_type_id) REFERENCES fieldTypeLookup(field_type_id),
            FOREIGN KEY (asset_class_id) REFERENCES assetClassLookup(asset_class_id),
            FOREIGN KEY (sub_asset_class_id) REFERENCES subAssetClassLookup(sub_asset_class_id),
            FOREIGN KEY (product_type_id) REFERENCES productTypeLookup(product_type_id),
            FOREIGN KEY (data_type_id) REFERENCES dataTypeLookup(data_type_id),
            FOREIGN KEY (structure_type_id) REFERENCES structureTypeLookup(structure_type_id),
            FOREIGN KEY (market_segment_id) REFERENCES marketSegmentLookup(market_segment_id),
            FOREIGN KEY (ticker_source_id) REFERENCES tickerSourceLookup(ticker_source_id),
            FOREIGN KEY (region_id) REFERENCES regionLookup(region_id),
            FOREIGN KEY (currency_id) REFERENCES currencyLookup(currency_id),
            FOREIGN KEY (term_id) REFERENCES termLookup(term_id),
            FOREIGN KEY (tenor_id) REFERENCES tenorLookup(tenor_id),
            FOREIGN KEY (country_id) REFERENCES countryLookup(country_id)
        )
    """)

    # Dependency Graph
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seriesDependencyGraph (
            dependency_id BIGINT PRIMARY KEY,
            parent_series_id INTEGER NOT NULL,
            child_series_id INTEGER NOT NULL,
            weight DOUBLE DEFAULT 1.0,
            formula VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_series_id) REFERENCES metaSeries(series_id),
            FOREIGN KEY (child_series_id) REFERENCES metaSeries(series_id)
        )
    """)

    # Calculation Log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calculationLog (
            calculation_id BIGINT PRIMARY KEY,
            series_id INTEGER NOT NULL,
            calculation_type VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            input_series_ids INTEGER[],
            parameters VARCHAR,
            formula VARCHAR NOT NULL,
            rows_processed BIGINT,
            error_message VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (series_id) REFERENCES metaSeries(series_id)
        )
    """)

    # Value Data (Time-Series)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS valueData (
            series_id INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            value DECIMAL(18, 6) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (series_id, timestamp),
            FOREIGN KEY (series_id) REFERENCES metaSeries(series_id)
        )
    """)

    # Create indexes for performance
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_valueData_series_timestamp ON valueData(series_id, timestamp)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_valueData_timestamp ON valueData(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_metaSeries_code ON metaSeries(series_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_metaSeries_active ON metaSeries(is_active)")


def downgrade(conn: DuckDBPyConnection) -> None:
    """Drop all tables created in upgrade."""
    # Drop tables in reverse order to respect foreign key constraints
    conn.execute("DROP TABLE IF EXISTS valueData")
    conn.execute("DROP TABLE IF EXISTS calculationLog")
    conn.execute("DROP TABLE IF EXISTS seriesDependencyGraph")
    conn.execute("DROP TABLE IF EXISTS metaSeries")
    conn.execute("DROP TABLE IF EXISTS countryLookup")
    conn.execute("DROP TABLE IF EXISTS tenorLookup")
    conn.execute("DROP TABLE IF EXISTS termLookup")
    conn.execute("DROP TABLE IF EXISTS currencyLookup")
    conn.execute("DROP TABLE IF EXISTS regionLookup")
    conn.execute("DROP TABLE IF EXISTS tickerSourceLookup")
    conn.execute("DROP TABLE IF EXISTS fieldTypeLookup")
    conn.execute("DROP TABLE IF EXISTS marketSegmentLookup")
    conn.execute("DROP TABLE IF EXISTS structureTypeLookup")
    conn.execute("DROP TABLE IF EXISTS dataTypeLookup")
    conn.execute("DROP TABLE IF EXISTS subAssetClassLookup")
    conn.execute("DROP TABLE IF EXISTS productTypeLookup")
    conn.execute("DROP TABLE IF EXISTS assetClassLookup")
