# Database Setup

This project uses ClickHouse for data storage with the following tools:

## Migration Management

- **clickhouse-migrate**: For managing database schema migrations
- Migration files are in the `migrations/` directory
- Migrations are run via `ClickHouseResource.run_migrations()`
- See `database/README_CLICKHOUSE_MIGRATIONS.md` for details

## Database Operations

- **clickhouse-connect**: Direct SQL queries via `ClickHouseResource`
- All database operations use raw SQL queries (no ORM)
- Managers (`LookupTableManager`, `MetaSeriesManager`, etc.) handle database interactions

## ClickHouseResource

The `ClickHouseResource` class (in `dagster_clickhouse/resources.py`) provides:
- Database connection management
- Query execution (`execute_query`, `execute_command`)
- Data insertion (`insert_data`)
- Database initialization (`ensure_database`)
- Migration management (`run_migrations`)

### Usage Example

```python
from dagster_clickhouse.resources import ClickHouseResource

clickhouse = ClickHouseResource.from_config()

# Ensure database exists
clickhouse.ensure_database()

# Run migrations
clickhouse.run_migrations()

# Execute queries
result = clickhouse.execute_query("SELECT * FROM metaSeries LIMIT 10")

# Insert data
clickhouse.insert_data("valueData", data=[...], column_names=[...])
```

## Why No SQLAlchemy?

We don't use SQLAlchemy because:
1. **Migrations**: We use `clickhouse-migrate` instead of Alembic (which required SQLAlchemy)
2. **Queries**: The codebase uses raw SQL queries via `clickhouse_connect`, which is simpler and more direct for ClickHouse
3. **ClickHouse-specific features**: Raw SQL gives us full access to ClickHouse-specific features without ORM abstraction

## Environment Variables

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=financial_platform
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_SECURE=false
CLICKHOUSE_VERIFY=true
```

## Deprecated Code

- `database/schemas.py`: Kept for reference only. Schema changes should be made through migrations.
- `ClickHouseResource.setup_schema()`: Deprecated. Use `ensure_database()` and `run_migrations()` instead.

