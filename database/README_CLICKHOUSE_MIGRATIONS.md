# ClickHouse Migrations Setup

This project uses `clickhouse-migrate` for managing ClickHouse database migrations.

## Installation

Install the package:

```bash
pip install clickhouse-migrate
```

## Setup

### 1. Environment Variables

Set these environment variables (or use `.env` file):

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=financial_platform
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_SECURE=false
CLICKHOUSE_VERIFY=true
```

Or set the connection URL directly:

```bash
export CLICKHOUSE_MIGRATE_DATABASES="clickhousedb://default@localhost:9000/financial_platform?secure=false"
export CLICKHOUSE_MIGRATE_DIRECTORY=./migrations
```

**Note:** The migration URL uses `clickhousedb://` protocol (not `clickhouse+native://`). For secure connections, add `?secure=true` to the URL.

### 2. Migration Files

Migration files are located in the `migrations/` directory. Each migration file should:

- Be a Python file (e.g., `001_initial_schema.py`)
- Import `Step` from `clickhouse_migrate`
- Define a `migrations` list containing `Step` objects

Example migration file:

```python
from clickhouse_migrate import Step

migrations = [
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS my_table (
            id UInt32,
            name String
        ) ENGINE = MergeTree
        PRIMARY KEY (id)
        ORDER BY (id)
        """,
        description="Create my_table"
    )
]
```

## Usage

### Running Migrations

```bash
# Apply all pending migrations
clickhouse-migrate migrate

# Or use the Python helper
python -c "from dagster_clickhouse.resources import ClickHouseResource; ClickHouseResource.from_config().run_migrations()"
```

### Creating New Migrations

1. Create a new file in `migrations/` directory (e.g., `002_add_column.py`)
2. Follow the naming convention: `{number}_{description}.py`
3. Define the migration steps:

```python
from clickhouse_migrate import Step

migrations = [
    Step(
        sql="ALTER TABLE my_table ADD COLUMN new_column String",
        description="Add new_column to my_table"
    )
]
```

### Migration Management

The `clickhouse-migrate` tool automatically:
- Tracks which migrations have been applied
- Only runs pending migrations
- Maintains a migration history table in ClickHouse

## Integration with Dagster

The `init_database_schema` asset automatically runs migrations when executed:

```python
from dagster_quickstart.assets.csv_loader import init_database_schema

# This will run all pending migrations
init_database_schema(context, clickhouse)
```

## Manual Migration Execution

You can also run migrations programmatically using ClickHouseResource:

```python
from dagster_clickhouse.resources import ClickHouseResource

clickhouse = ClickHouseResource.from_config()

# Ensure database exists
clickhouse.ensure_database()

# Run all pending migrations
clickhouse.run_migrations()
```

## Migration Best Practices

1. **Idempotent SQL**: Use `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE` carefully
2. **Descriptive Names**: Use clear, descriptive migration file names
3. **One Change Per Migration**: Keep migrations focused on a single change
4. **Test First**: Always test migrations in a development environment
5. **Backup**: Backup your database before running migrations in production

## Troubleshooting

### Migration Fails

If a migration fails:
1. Check the error message in the logs
2. Verify the SQL syntax is correct for ClickHouse
3. Ensure the database connection is working
4. Check if previous migrations were applied successfully

### Reset Migrations

To reset migrations (use with caution):
1. Drop the migration tracking table: `DROP TABLE IF EXISTS schema_migrations`
2. Re-run migrations: `clickhouse-migrate migrate`

## Notes

- ClickHouse doesn't support transactions, so migrations are applied one at a time
- Failed migrations may leave the database in a partial state
- Always test migrations thoroughly before applying to production
- The migration tool creates a `schema_migrations` table to track applied migrations

