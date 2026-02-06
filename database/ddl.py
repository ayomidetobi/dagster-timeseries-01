"""DDL (Data Definition Language) templates for DuckDB operations.

With S3 as the datalake, these templates are used to create DuckDB views
over S3 Parquet control tables for querying.
"""

# Generic SQL template for creating DuckDB views over S3 control tables
# Placeholders: {view_name}, {full_s3_path}, {select_columns}, {where_clause}, {order_by_column}
CREATE_VIEW_FROM_S3_TEMPLATE = """
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT {select_columns}
    FROM read_parquet('{full_s3_path}')
    {where_clause}
    ORDER BY {order_by_column}
"""

# Template for lookup table view SELECT columns (code-based lookups)
# Placeholders: {id_column}, {code_field}, {name_field}
LOOKUP_TABLE_VIEW_SELECT_CODE_BASED = """
                    row_number() OVER (ORDER BY code) AS {id_column},
                    code AS {code_field},
                    name AS {name_field},
                    lookup_type
            """

# Template for lookup table view SELECT columns (simple lookups)
# Placeholders: {id_column}, {name_column}
LOOKUP_TABLE_VIEW_SELECT_SIMPLE = """
                    row_number() OVER (ORDER BY name) AS {id_column},
                    name AS {name_column},
                    lookup_type
            """

# Template for lookup table view WHERE clause (generic - works for both code-based and simple)
# Placeholders: {lookup_type}, {column_name} (use 'code' for code-based, 'name' for simple)
LOOKUP_TABLE_VIEW_WHERE = """
                WHERE lookup_type = '{lookup_type}'
                    AND {column_name} IS NOT NULL AND {column_name} != ''
            """

# Template for UNION ALL SELECT (code-based lookups)
# Placeholders: {lookup_type}, {code_field}, {name_field}, {temp_table}
UNION_ALL_SELECT_CODE_BASED = """
                    SELECT '{lookup_type}' AS lookup_type,
                    {code_field} AS code,
                    {name_field} AS name
                    FROM {temp_table}
            """

# Template for UNION ALL SELECT (simple lookups)
# Placeholders: {lookup_type}, {name_column}, {temp_table}
UNION_ALL_SELECT_SIMPLE = """
                    SELECT '{lookup_type}' AS lookup_type,
                    {name_column} AS code,
                    {name_column} AS name
                    FROM {temp_table}
            """

# Template for building lookup results query (name -> row_index)
# Placeholders: {name_column}, {temp_lookup_table}
LOOKUP_RESULTS_QUERY = """
        SELECT 
            {name_column} AS name_value,
            row_number() OVER (ORDER BY {name_column}) AS row_index
        FROM {temp_lookup_table}
        WHERE {name_column} IS NOT NULL AND {name_column} != ''
        ORDER BY row_index
    """

# Template for extracting lookup table data (generic - works for both code-based and simple)
# Placeholders: {lookup_type}, {uuid}, {staging_column}, {select_columns}, {source_temp_table}
# For code-based: {select_columns} = "{staging_column} AS {code_field}, {staging_column} AS {name_field}"
# For simple: {select_columns} = "{staging_column} AS {name_column}"
EXTRACT_LOOKUP = """
            CREATE TEMP TABLE _temp_lookup_{lookup_type}_{{uuid}} AS
            SELECT DISTINCT 
                {select_columns}
            FROM {source_temp_table}
            WHERE {staging_column} IS NOT NULL AND {staging_column} != ''
            ORDER BY {staging_column}
        """

# Template for building meta series results query (series_code -> row_index)
# Placeholders: {temp_table}
# Note: row_index is computed in Python via enumerate() to avoid double row_number() computation
META_SERIES_RESULTS_QUERY = """
        SELECT 
            series_code
        FROM {temp_table}
        WHERE series_code IS NOT NULL AND series_code != ''
        ORDER BY series_code
    """

# Template for meta series validation condition (single lookup type check)
# Placeholders: {lookup_type}, {full_s3_path}, {canonical_column}
META_SERIES_VALIDATION_CONDITION = """
    (ms.{lookup_type} IS NULL OR ms.{lookup_type} = '' OR 
     EXISTS (
         SELECT 1 FROM read_parquet('{full_s3_path}') lk 
         WHERE lk.lookup_type = '{lookup_type}'
           AND lk.{canonical_column} = ms.{lookup_type}
     ))
            """

# Template for meta series validation query (finds invalid rows)
# Placeholders: {select_columns}, {temp_table}, {validation_sql}
META_SERIES_VALIDATION_QUERY = """
    SELECT {select_columns}
    FROM {temp_table} ms
    WHERE NOT ({validation_sql})
    """

# ============================================================================
# DDL Statements for DuckDB Operations
# ============================================================================

# Template for creating temporary table from CSV file
# Placeholders: {temp_table}, {csv_path}, {null_value}
CREATE_TEMP_TABLE_FROM_CSV = """
    CREATE TEMP TABLE {temp_table} AS
    SELECT * FROM read_csv('{csv_path}', 
        nullstr='{null_value}',
        header=true,
        auto_detect=true,
        ignore_errors=false
    )
"""

# Template for creating temporary table from SQL query
# Placeholders: {temp_table_name}, {query}
CREATE_TEMP_TABLE_FROM_QUERY = """
    CREATE TEMP TABLE {temp_table_name} AS {query}
"""

# Template for dropping temporary table
# Placeholders: {temp_table}
DROP_TEMP_TABLE = "DROP TABLE IF EXISTS {temp_table}"

# Template for reading Parquet schema (returns column names)
# Placeholders: {full_s3_path}
READ_PARQUET_SCHEMA = "SELECT * FROM read_parquet('{full_s3_path}') LIMIT 0"

# ============================================================================
# Common SQL Patterns (reusable fragments)
# ============================================================================

# Pattern for checking if a column is not null and not empty
# Placeholders: {column_name}
NOT_NULL_AND_NOT_EMPTY = "{column_name} IS NOT NULL AND {column_name} != ''"

# ============================================================================
# Dependency SQL Templates
# ============================================================================

# Template for dependency validation query (checks referential integrity)
# Placeholders: {temp_table}
DEPENDENCY_VALIDATION_QUERY = """
    SELECT 
        d.parent_series_code,
        d.child_series_code,
        CASE 
            WHEN p.series_id IS NULL THEN 'Parent series not found: ' || d.parent_series_code
            WHEN c.series_id IS NULL THEN 'Child series not found: ' || d.child_series_code
            ELSE NULL
        END AS error_message
    FROM {temp_table} d
    LEFT JOIN metaSeries p ON d.parent_series_code = p.series_code
    LEFT JOIN metaSeries c ON d.child_series_code = c.series_code
    WHERE p.series_id IS NULL OR c.series_id IS NULL
"""

# Template for creating enriched dependency table with series IDs
# Placeholders: {enriched_table}, {temp_table}, {calc_type_expr}
DEPENDENCY_ENRICHED_TABLE_QUERY = """
    CREATE TEMP TABLE {enriched_table} AS
    SELECT 
        row_number() OVER (ORDER BY p.series_id, c.series_id) AS dependency_id,
        d.parent_series_code,
        d.child_series_code,
        p.series_id AS parent_series_id,
        c.series_id AS child_series_id,
        {calc_type_expr}
    FROM {temp_table} d
    INNER JOIN metaSeries p ON d.parent_series_code = p.series_code
    INNER JOIN metaSeries c ON d.child_series_code = c.series_code
"""

# Template for building dependency results query (dependency_id -> row_index)
# Placeholders: {temp_table}
# Note: row_index is computed in Python via enumerate() to avoid double row_number() computation
# dependency_id is already computed when writing to S3, so we just select it ordered
DEPENDENCY_RESULTS_QUERY = """
    SELECT 
        dependency_id
    FROM {temp_table}
    ORDER BY parent_series_id, child_series_id
"""
