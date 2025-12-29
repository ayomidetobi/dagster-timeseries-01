"""Optimize partitioning for better query performance.

This migration documents the partitioning optimization that was applied in
001_initial_schema.py for new installations. For existing installations
with data, manual table recreation is required.

IMPORTANT: This migration is informational only.
The actual schema change (daily partitioning) is in 001_initial_schema.py.
For existing installations with data, you need to:
1. Create a new table with daily partitioning
2. Migrate data: INSERT INTO new_table SELECT * FROM old_table
3. Swap tables: RENAME TABLE old_table TO old_table_backup, new_table TO old_table
4. Drop backup after validation: DROP TABLE old_table_backup

Rationale: Daily partitioning (toYYYYMMDD) provides better partition pruning
for date-range queries compared to monthly partitioning (toYYYYMM), aligns
with Dagster's daily partition-based assets, and keeps partitions smaller
for better query performance.
"""

from clickhouse_migrate import Step

migrations = [
    Step(
        sql="""
        -- Note: This migration is informational only.
        -- The actual schema change is in 001_initial_schema.py for new installations.
        -- For existing installations, manually recreate the table with daily partitioning.
        -- Daily partitioning (toYYYYMMDD) provides better partition pruning and aligns
        -- with Dagster's daily partition-based assets.
        SELECT 1
        """,
        description="Document valueData partitioning optimization (daily partitions - applied in 001_initial_schema.py for new installations)",
    ),
]
