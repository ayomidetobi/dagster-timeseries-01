"""Staging table for meta series migration."""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    """Create staging table for meta series."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_meta_series (
            series_name VARCHAR,
            series_code VARCHAR,
            data_source VARCHAR,
            field_type VARCHAR,
            asset_class VARCHAR,
            sub_asset_class VARCHAR,
            product_type VARCHAR,
            data_type VARCHAR,
            structure_type VARCHAR,
            market_segment VARCHAR,
            ticker_source VARCHAR,
            ticker VARCHAR,
            region VARCHAR,
            currency VARCHAR,
            term VARCHAR,
            tenor VARCHAR,
            country VARCHAR,
            valid_from VARCHAR,
            valid_to VARCHAR,
            calculation_formula VARCHAR,
            description VARCHAR,
            is_active VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def downgrade(conn: DuckDBPyConnection) -> None:
    """Drop staging table for meta series."""
    conn.execute("DROP TABLE IF EXISTS staging_meta_series")

