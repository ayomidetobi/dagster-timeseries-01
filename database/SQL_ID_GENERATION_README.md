# SQL-Based ID Generation

This document explains the SQL-based approach for generating deterministic IDs from staging tables, matching the pattern:

```sql
INSERT INTO dim_table
SELECT
    rowNumberInAllBlocks() AS id,
    name
FROM (SELECT DISTINCT name FROM staging_table)
ORDER BY name;
```

## Approach

We provide two methods for ID generation:

### 1. Hash-Based IDs (Recommended)

Uses ClickHouse's `cityHash64()` function for deterministic, hash-based IDs:

```sql
INSERT INTO productTypeLookup (product_type_id, product_type_name, created_at, updated_at)
SELECT DISTINCT
    toUInt32(if(cityHash64('product_type:', product_type) % 4294967295 = 0, 1,
                cityHash64('product_type:', product_type) % 4294967295)) AS product_type_id,
    product_type AS product_type_name,
    now64(6) AS created_at,
    now64(6) AS updated_at
FROM staging_lookup_tables
WHERE product_type IS NOT NULL
    AND product_type != ''
    AND product_type NOT IN (SELECT product_type_name FROM productTypeLookup)
ORDER BY product_type;
```

**Benefits:**
- Deterministic: Same input always produces same ID
- Backfill-safe: Re-running produces consistent results
- No dependency on existing data or insertion order
- Hash-based: IDs are distributed, not sequential

### 2. Sequential IDs

Uses `row_number()` with ORDER BY for deterministic sequential IDs:

```sql
INSERT INTO productTypeLookup (product_type_id, product_type_name, created_at, updated_at)
SELECT
    (SELECT if(max(product_type_id) IS NULL, 0, max(product_type_id)) FROM productTypeLookup) +
    row_number() OVER (ORDER BY product_type) AS product_type_id,
    product_type AS product_type_name,
    now64(6) AS created_at,
    now64(6) AS updated_at
FROM (
    SELECT DISTINCT product_type
    FROM staging_lookup_tables
    WHERE product_type IS NOT NULL
        AND product_type != ''
        AND product_type NOT IN (SELECT product_type_name FROM productTypeLookup)
)
ORDER BY product_type;
```

**Benefits:**
- Sequential IDs (1, 2, 3...)
- Deterministic via ORDER BY
- Matches the user's example pattern more closely

**Note:** Sequential IDs require existing data (to get max ID). Hash-based IDs don't have this dependency.

## Why Not `rowNumberInAllBlocks()`?

`rowNumberInAllBlocks()` is **not deterministic** - it can produce different results across queries or blocks. Using `row_number() OVER (ORDER BY column)` with a deterministic sort order ensures reproducible results.

## Usage

### Python API

```python
from database.id_generation_sql import generate_ids_from_staging_sql
from dagster_quickstart.resources import ClickHouseResource

clickhouse = ClickHouseResource.from_config()

# Generate hash-based IDs
sql = generate_ids_from_staging_sql(
    staging_table="staging_lookup_tables",
    target_table="productTypeLookup",
    id_column="product_type_id",
    name_column="product_type",
    order_by_column="product_type",
    namespace="product_type"
)

clickhouse.execute_command(sql)
```

### Direct SQL

See `database/examples_sql_id_generation.sql` for complete examples of:
- Simple lookups (asset_class, product_type)
- Code-based lookups (currency, tenor, country)
- Dependent lookups (sub_asset_class with foreign key)

## Module Structure

- **`database/id_generation_sql.py`**: Core SQL generation functions
- **`database/lookup_table_sql_loader.py`**: High-level loader functions
- **`database/examples_sql_id_generation.sql`**: Example SQL queries

## Migration from Python Hash-Based Approach

The previous Python-based hash approach (`database/id_generation.py`) is deprecated in favor of SQL-based generation because:

1. **Performance**: SQL-based is more efficient (single query vs multiple Python queries)
2. **Simplicity**: Leverages ClickHouse's native hash functions
3. **Consistency**: Matches the SQL-first approach for data transformation
4. **Staging Integration**: Works directly with staging tables

Existing Python code using `get_or_create_deterministic_id()` will continue to work, but new code should use SQL-based generation from staging tables.

