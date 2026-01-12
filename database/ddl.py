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

# Template for lookup table view WHERE clause (code-based lookups)
# Placeholders: {lookup_type}
LOOKUP_TABLE_VIEW_WHERE_CODE_BASED = """
                WHERE lookup_type = '{lookup_type}'
                    AND code IS NOT NULL AND code != ''
            """

# Template for lookup table view WHERE clause (simple lookups)
# Placeholders: {lookup_type}
LOOKUP_TABLE_VIEW_WHERE_SIMPLE = """
                WHERE lookup_type = '{lookup_type}'
                    AND name IS NOT NULL AND name != ''
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

# Template for extracting lookup table data (code-based lookups)
# Placeholders: {lookup_type}, {uuid}, {staging_column}, {code_field}, {name_field}, {source_temp_table}
EXTRACT_LOOKUP_CODE_BASED = """
            CREATE TEMP TABLE _temp_lookup_{lookup_type}_{{uuid}} AS
            SELECT DISTINCT 
                {staging_column} AS {code_field},
                {staging_column} AS {name_field}
            FROM {source_temp_table}
            WHERE {staging_column} IS NOT NULL AND {staging_column} != ''
            ORDER BY {staging_column}
        """

# Template for extracting lookup table data (simple lookups)
# Placeholders: {lookup_type}, {uuid}, {staging_column}, {name_column}, {source_temp_table}
EXTRACT_LOOKUP_SIMPLE = """
            CREATE TEMP TABLE _temp_lookup_{lookup_type}_{{uuid}} AS
            SELECT DISTINCT 
                {staging_column} AS {name_column}
            FROM {source_temp_table}
            WHERE {staging_column} IS NOT NULL AND {staging_column} != ''
            ORDER BY {staging_column}
        """

# Template for building meta series results query (series_code -> row_index)
# Placeholders: {temp_table}
META_SERIES_RESULTS_QUERY = """
        SELECT 
            series_code,
            row_number() OVER (ORDER BY (SELECT NULL)) AS row_index
        FROM {temp_table}
        WHERE series_code IS NOT NULL AND series_code != ''
        ORDER BY row_index
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
