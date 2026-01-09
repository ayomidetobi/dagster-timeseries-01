"""Staging table for lookup tables migration."""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    """Create staging table for lookup tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_lookup_tables (
            asset_class VARCHAR,
            product_type VARCHAR,
            sub_asset_class VARCHAR,
            data_type VARCHAR,
            structure_type VARCHAR,
            market_segment VARCHAR,
            field_type VARCHAR,
            ticker_source VARCHAR,
            country VARCHAR,
            currency VARCHAR,
            region VARCHAR,
            term VARCHAR,
            tenor VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def downgrade(conn: DuckDBPyConnection) -> None:
    """Drop staging table for lookup tables."""
    conn.execute("DROP TABLE IF EXISTS staging_lookup_tables")
