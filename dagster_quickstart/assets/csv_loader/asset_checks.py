"""Asset checks for CSV loader assets using Great Expectations via dagster-ge."""

from typing import Optional

import polars as pl
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_quickstart.assets.data_quality.expectation_suites import (
    build_all_dimensions_expectations,
)
from dagster_quickstart.resources import ClickHouseResource, GreatExpectationsResource
from dagster_quickstart.utils.constants import META_SERIES_REQUIRED_COLUMNS
from dagster_quickstart.utils.data_quality_helpers import polars_to_pandas, validate_foreign_keys


def _validate_with_ge(
    context: AssetCheckExecutionContext,
    df: pl.DataFrame,
    suite_name: str,
    asset_name: str,
    great_expectations: GreatExpectationsResource,
    expectations: Optional[list] = None,
) -> AssetCheckResult:
    """Validate DataFrame using Great Expectations.

    Args:
        context: Dagster execution context
        df: Polars DataFrame to validate
        suite_name: Name of the expectation suite
        asset_name: Name of the asset being checked
        great_expectations: Great Expectations resource
        expectations: Optional list of expectation configurations

    Returns:
        AssetCheckResult with validation results
    """
    try:
        df_pandas = polars_to_pandas(df)

        if df_pandas.empty:
            return AssetCheckResult(
                passed=False,
                description="Validation failed: DataFrame is empty",
                severity=AssetCheckSeverity.ERROR,
            )

        # Get validator from resource
        validator = great_expectations.get_validator(
            asset_df=df_pandas,
            suite_name=suite_name,
        )

        # Validate each expectation configuration
        if expectations:
            all_passed = True
            results = []
            for expectation_config in expectations:
                # Use ExpectationConfiguration.validate() method as shown in docs
                validation_result = expectation_config.validate(validator)
                results.append(validation_result)
                if not validation_result.success:
                    all_passed = False

            # Aggregate results
            total_expectations = len(expectations)
            successful = sum(1 for r in results if r.success)
            unsuccessful = total_expectations - successful

            return AssetCheckResult(
                passed=all_passed,
                description=f"Great Expectations validation: {'PASSED' if all_passed else 'FAILED'} ({successful}/{total_expectations} expectations passed)",
                metadata={
                    "expectations_evaluated": total_expectations,
                    "successful_expectations": successful,
                    "unsuccessful_expectations": unsuccessful,
                    "success_percent": (successful / total_expectations * 100)
                    if total_expectations > 0
                    else 0.0,
                },
                severity=AssetCheckSeverity.ERROR if not all_passed else AssetCheckSeverity.WARN,
            )
        else:
            # No expectations provided, just return success
            return AssetCheckResult(
                passed=True,
                description="No expectations to validate",
                severity=AssetCheckSeverity.WARN,
            )

    except Exception as e:
        context.log.error(f"Great Expectations validation error: {e}")
        return AssetCheckResult(
            passed=False,
            description=f"Validation error: {e!s}",
            severity=AssetCheckSeverity.ERROR,
        )


@asset_check(asset="load_lookup_tables_from_csv", name="lookup_tables_quality")
def check_lookup_tables_quality(
    context: AssetCheckExecutionContext,
    load_lookup_tables_from_csv: pl.DataFrame,
    great_expectations: GreatExpectationsResource,
) -> AssetCheckResult:
    """Check data quality for lookup tables across all 6 dimensions."""
    suite_name = "lookup_tables_all_dimensions"

    # Build expectations
    expectations = build_all_dimensions_expectations(
        required_columns=["lookup_table_type", "name"],
        unique_columns=None,
        composite_keys=[["lookup_table_type", "name"]],
        column_types={
            "lookup_table_type": "string",
            "name": "string",
        },
    )

    return _validate_with_ge(
        context,
        load_lookup_tables_from_csv,
        suite_name,
        "lookup_tables",
        great_expectations,
        expectations,
    )


@asset_check(asset="load_meta_series_from_csv", name="meta_series_quality")
def check_meta_series_quality(
    context: AssetCheckExecutionContext,
    load_meta_series_from_csv: pl.DataFrame,
    great_expectations: GreatExpectationsResource,
) -> AssetCheckResult:
    """Check data quality for meta series across all 6 dimensions."""
    suite_name = "meta_series_all_dimensions"

    # Build expectations
    expectations = build_all_dimensions_expectations(
        required_columns=META_SERIES_REQUIRED_COLUMNS,
        unique_columns=["series_code"],
        column_types={
            "series_code": "string",
            "series_name": "string",
            "ticker": "string",
            "data_source": "string",
        },
    )

    return _validate_with_ge(
        context,
        load_meta_series_from_csv,
        suite_name,
        "meta_series",
        great_expectations,
        expectations,
    )


@asset_check(asset="load_meta_series_from_csv", name="meta_series_consistency")
def check_meta_series_consistency(
    context: AssetCheckExecutionContext,
    load_meta_series_from_csv: pl.DataFrame,
    clickhouse: ClickHouseResource,
) -> AssetCheckResult:
    """Check consistency: referential integrity for meta series foreign keys."""
    from dagster_quickstart.utils.data_quality_helpers import polars_to_pandas

    df_pandas = polars_to_pandas(load_meta_series_from_csv)
    if df_pandas.empty:
        return AssetCheckResult(
            passed=False,
            description="Consistency check: No data available",
            severity=AssetCheckSeverity.WARN,
        )

    foreign_keys = {
        "field_type_id": {"lookup_table": "fieldTypeLookup", "lookup_column": "field_type_id"},
        "asset_class_id": {"lookup_table": "assetClassLookup", "lookup_column": "asset_class_id"},
        "sub_asset_class_id": {
            "lookup_table": "subAssetClassLookup",
            "lookup_column": "sub_asset_class_id",
        },
        "product_type_id": {
            "lookup_table": "productTypeLookup",
            "lookup_column": "product_type_id",
        },
        "data_type_id": {"lookup_table": "dataTypeLookup", "lookup_column": "data_type_id"},
        "structure_type_id": {
            "lookup_table": "structureTypeLookup",
            "lookup_column": "structure_type_id",
        },
        "market_segment_id": {
            "lookup_table": "marketSegmentLookup",
            "lookup_column": "market_segment_id",
        },
        "ticker_source_id": {
            "lookup_table": "tickerSourceLookup",
            "lookup_column": "ticker_source_id",
        },
        "region_id": {"lookup_table": "regionLookup", "lookup_column": "region_id"},
        "currency_id": {"lookup_table": "currencyLookup", "lookup_column": "currency_id"},
        "term_id": {"lookup_table": "termLookup", "lookup_column": "term_id"},
        "tenor_id": {"lookup_table": "tenorLookup", "lookup_column": "tenor_id"},
        "country_id": {"lookup_table": "countryLookup", "lookup_column": "country_id"},
    }

    try:
        results = validate_foreign_keys(context, df_pandas, clickhouse, foreign_keys)
        all_valid = all(r["valid"] for r in results.values())
        invalid_details = {k: v for k, v in results.items() if not v["valid"]}

        return AssetCheckResult(
            passed=all_valid,
            description=f"Consistency check: {'PASSED' if all_valid else 'FAILED'}",
            metadata={
                "checks_run": len(results),
                "checks_passed": sum(1 for r in results.values() if r["valid"]),
                "invalid_foreign_keys": invalid_details,
            },
            severity=AssetCheckSeverity.ERROR if not all_valid else AssetCheckSeverity.WARN,
        )
    except Exception as e:
        context.log.error(f"Error in consistency check: {e}")
        return AssetCheckResult(
            passed=False,
            description=f"Consistency check error: {e!s}",
            severity=AssetCheckSeverity.ERROR,
        )
