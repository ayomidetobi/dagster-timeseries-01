"""Add is_active field to metaSeries table.

This migration adds the is_active column to the metaSeries table for existing installations.
New installations will have this column from 001_initial_schema.py.

The is_active column is a UInt8 with DEFAULT 1, meaning all existing rows will be set to active (1).
"""

from clickhouse_migrate import Step

migrations = [
    Step(
        sql="""
        ALTER TABLE metaSeries
        ADD COLUMN IF NOT EXISTS is_active UInt8 DEFAULT 1
        """,
        description="Add is_active column to metaSeries table with default value 1 (active)",
    ),
]
