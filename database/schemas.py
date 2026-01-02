"""SQL query schemas for database operations."""

# SQL query to insert meta series from staging table to metaSeries table.
# This query resolves all lookup name values to IDs using LEFT JOINs with lookup tables.
# All lookup values come as names from CSV and are resolved to IDs.
INSERT_META_SERIES_FROM_STAGING_SQL = """
    INSERT INTO metaSeries (
        series_id, series_name, series_code, data_source,
        field_type_id, asset_class_id, sub_asset_class_id, product_type_id,
        data_type_id, structure_type_id, market_segment_id, ticker_source_id,
        ticker, region_id, currency_id, term_id, tenor_id, country_id,
        calculation_formula, description, is_active, created_at, updated_at, created_by
    )
    SELECT
        (SELECT if(max(series_id) IS NULL, 0, max(series_id)) FROM metaSeries) +
        row_number() OVER (ORDER BY st.series_code) AS series_id,
        st.series_name,
        st.series_code,
        st.data_source,
        -- Resolve field_type: name -> ID
        if(st.field_type IS NOT NULL AND st.field_type != '', ft.field_type_id, NULL) AS field_type_id,
        -- Resolve asset_class: name -> ID
        if(st.asset_class IS NOT NULL AND st.asset_class != '', ac.asset_class_id, NULL) AS asset_class_id,
        -- Resolve sub_asset_class: name -> ID (independent lookup)
        if(st.sub_asset_class IS NOT NULL AND st.sub_asset_class != '', sac.sub_asset_class_id, NULL) AS sub_asset_class_id,
        -- Resolve product_type: name -> ID
        if(st.product_type IS NOT NULL AND st.product_type != '', pt.product_type_id, NULL) AS product_type_id,
        -- Resolve data_type: name -> ID
        if(st.data_type IS NOT NULL AND st.data_type != '', dt.data_type_id, NULL) AS data_type_id,
        -- Resolve structure_type: name -> ID
        if(st.structure_type IS NOT NULL AND st.structure_type != '', stt.structure_type_id, NULL) AS structure_type_id,
        -- Resolve market_segment: name -> ID
        if(st.market_segment IS NOT NULL AND st.market_segment != '', ms.market_segment_id, NULL) AS market_segment_id,
        -- Resolve ticker_source: name -> ID
        if(st.ticker_source IS NOT NULL AND st.ticker_source != '', ts.ticker_source_id, NULL) AS ticker_source_id,
        st.ticker,
        -- Resolve region: name -> ID
        if(st.region IS NOT NULL AND st.region != '', r.region_id, NULL) AS region_id,
        -- Resolve currency: code -> ID
        if(st.currency IS NOT NULL AND st.currency != '', c.currency_id, NULL) AS currency_id,
        -- Resolve term: name -> ID
        if(st.term IS NOT NULL AND st.term != '', t.term_id, NULL) AS term_id,
        -- Resolve tenor: code -> ID
        if(st.tenor IS NOT NULL AND st.tenor != '', tn.tenor_id, NULL) AS tenor_id,
        -- Resolve country: code -> ID
        if(st.country IS NOT NULL AND st.country != '', co.country_id, NULL) AS country_id,
        st.calculation_formula,
        st.description,
        -- Parse is_active: convert string to UInt8 (default to 1 if not provided)
        if(st.is_active IS NULL OR st.is_active = '',
           1,
           if(lower(st.is_active) IN ('1', 'true', 'yes', 'y', 'active'), 1, 0)) AS is_active,
        now64(6) AS created_at,
        now64(6) AS updated_at,
        'csv_loader' AS created_by
    FROM staging_meta_series st
    LEFT JOIN fieldTypeLookup ft ON st.field_type = ft.field_type_name
    LEFT JOIN assetClassLookup ac ON st.asset_class = ac.asset_class_name
    LEFT JOIN productTypeLookup pt ON st.product_type = pt.product_type_name
    LEFT JOIN dataTypeLookup dt ON st.data_type = dt.data_type_name
    LEFT JOIN structureTypeLookup stt ON st.structure_type = stt.structure_type_name
    LEFT JOIN marketSegmentLookup ms ON st.market_segment = ms.market_segment_name
    LEFT JOIN tickerSourceLookup ts ON st.ticker_source = ts.ticker_source_name
    LEFT JOIN subAssetClassLookup sac ON st.sub_asset_class = sac.sub_asset_class_name
    LEFT JOIN regionLookup r ON st.region = r.region_name
    LEFT JOIN currencyLookup c ON st.currency = c.currency_code
    LEFT JOIN termLookup t ON st.term = t.term_name
    LEFT JOIN tenorLookup tn ON st.tenor = tn.tenor_code
    LEFT JOIN countryLookup co ON st.country = co.country_code
    WHERE st.series_code IS NOT NULL
        AND st.series_code != ''
        AND st.series_code NOT IN (SELECT series_code FROM metaSeries)
    ORDER BY st.series_code
    """

