"""CSV-based loading for lookup tables and meta series with validation."""

from typing import Any, Callable, Dict, Optional, Set

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    LOOKUP_TABLE_COLUMNS,
    LOOKUP_TABLE_PROCESSING_ORDER,
    META_SERIES_REQUIRED_COLUMNS,
    NULL_VALUE_REPRESENTATION,
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
)
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DataSourceValidationError,
    MetaSeriesNotFoundError,
)
from dagster_quickstart.utils.helpers import (
    is_empty_row,
    parse_data_source,
    read_csv_safe,
    resolve_lookup_id_from_string,
    safe_int,
    validate_csv_columns,
)
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.models import (
    AssetClassLookup,
    CountryLookup,
    CurrencyLookup,
    DataTypeLookup,
    FieldTypeLookup,
    MarketSegmentLookup,
    MetaSeriesCreate,
    ProductTypeLookup,
    RegionLookup,
    StructureTypeLookup,
    SubAssetClassLookup,
    TenorLookup,
    TermLookup,
    TickerSourceLookup,
)


class LookupTableCSVConfig(Config):
    """Configuration for loading lookup tables from CSV."""

    csv_path: str = "data/lookup_tables.csv"  # Path to CSV file
    allowed_names_csv_path: str = "data/allowed_names.csv"  # Path to CSV with allowed names
    lookup_table_type: str = "all"  # Type: "all" (load all types), or specific: asset_class, product_type, sub_asset_class, data_type, structure_type, market_segment, field_type, ticker_source, region, currency, term, tenor, country


class MetaSeriesCSVConfig(Config):
    """Configuration for loading meta series from CSV."""

    csv_path: str = "data/meta_series.csv"  # Path to CSV file with meta series data


def load_allowed_names(csv_path: str, lookup_table_type: str) -> Set[str]:
    """Load allowed names from CSV file.

    Supports two formats:
    1. Wide format: Columns are lookup table types (asset_class, product_type, etc.)
    2. Long format: Has lookup_table_type and name columns
    """
    try:
        # Read CSV with truncate_ragged_lines to handle trailing commas
        df = pl.read_csv(csv_path, truncate_ragged_lines=True)

        # Check if this is wide format (columns are lookup table types)
        column_name = lookup_table_type.replace("_", "_")  # Keep as is
        if column_name in df.columns:
            # Wide format: extract unique values from the column
            values = df[column_name].drop_nulls().unique().to_list()
            return set[str](str(v).strip() for v in values if v and str(v).strip() != "")

        # Long format: has lookup_table_type and name columns
        if "name" in df.columns:
            if "lookup_table_type" in df.columns:
                df = df.filter(pl.col("lookup_table_type") == lookup_table_type)
            return set[str](df["name"].unique().to_list())

        raise ValueError(
            f"CSV file {csv_path} must have either '{column_name}' column "
            f"(wide format) or 'name' column (long format)"
        )
    except Exception as e:
        raise CSVValidationError(f"Error loading allowed names from {csv_path}: {e}") from e


def validate_lookup_name(name: str, allowed_names: Set[str], lookup_type: str) -> None:
    """Validate that a lookup name is in the allowed list."""
    if name not in allowed_names:
        raise ValueError(
            f"Name '{name}' is not in the allowed list for {lookup_type}. "
            f"Allowed names: {sorted(allowed_names)}"
        )


def process_simple_lookup_type(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    lookup_type: str,
    values: list,
    allowed_names: Set[str],
    lookup_factory: Callable[[str, Optional[str]], Any],
    insert_method: Callable[[Any], int],
    get_by_name_method: Callable[[str], Optional[Dict[str, Any]]],
    id_field_name: str,
) -> Dict[str, int]:
    """Process a simple lookup type (no dependencies, no special code generation).

    Args:
        context: Dagster execution context
        lookup_manager: Lookup table manager instance
        lookup_type: Type of lookup table (e.g., "data_type")
        values: List of values to process
        allowed_names: Set of allowed names for validation
        lookup_factory: Function to create lookup object (name, description) -> Lookup
        insert_method: Method to insert lookup (lookup) -> int
        get_by_name_method: Method to get existing lookup by name (name) -> Optional[Dict]
        id_field_name: Name of the ID field in the returned dict (e.g., "data_type_id")

    Returns:
        Dictionary mapping name to lookup_id
    """
    results = {}
    for name_value in values:
        if not name_value or str(name_value).strip() == "":
            continue
        name = str(name_value).strip()
        validate_lookup_name(name, allowed_names, lookup_type)

        try:
            existing = get_by_name_method(name)
            if existing:
                context.log.info(f"{lookup_type} '{name}' already exists, using existing ID")
                results[name] = existing[id_field_name]
            else:
                lookup_obj = lookup_factory(name, None)
                lookup_id = insert_method(lookup_obj)
                results[name] = lookup_id
        except Exception as e:
            context.log.error(f"Error processing {lookup_type} {name}: {e}")
            raise
    return results


def process_asset_class_lookup(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    values: list,
    allowed_names: Set[str],
) -> Dict[str, int]:
    """Process asset_class lookup type (returns mapping for sub_asset_class).

    Returns:
        Dictionary mapping asset_class name to asset_class_id
    """
    asset_class_mapping = {}
    for name_value in values:
        if not name_value or str(name_value).strip() == "":
            continue
        name = str(name_value).strip()
        validate_lookup_name(name, allowed_names, "asset_class")

        try:
            asset_lookup = AssetClassLookup(name=name, description=None)
            existing = lookup_manager.get_asset_class_by_name(name)
            if not existing:
                lookup_id = lookup_manager.insert_asset_class(asset_lookup)
                asset_class_mapping[name] = lookup_id
            else:
                asset_class_mapping[name] = existing["asset_class_id"]
        except Exception as e:
            context.log.error(f"Error processing asset_class {name}: {e}")
            raise
    return asset_class_mapping


def process_code_based_lookup_type(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    lookup_type: str,
    values: list,
    allowed_names: Set[str],
    lookup_factory: Callable[[str, Optional[str], str], Any],
    insert_method: Callable[[Any], int],
    get_by_name_method: Callable[[str], Optional[Dict[str, Any]]],
    id_field_name: str,
) -> Dict[str, int]:
    """Process lookup type that requires code generation (field_type, ticker_source).

    Args:
        context: Dagster execution context
        lookup_manager: Lookup table manager instance
        lookup_type: Type of lookup table
        values: List of values to process
        allowed_names: Set of allowed names for validation
        lookup_factory: Function to create lookup object (name, description, code) -> Lookup
        insert_method: Method to insert lookup (lookup) -> int
        get_by_name_method: Method to get existing lookup by name (name) -> Optional[Dict]
        id_field_name: Name of the ID field in the returned dict (e.g., "field_type_id")

    Returns:
        Dictionary mapping name to lookup_id
    """
    results = {}
    for name_value in values:
        if not name_value or str(name_value).strip() == "":
            continue
        name = str(name_value).strip()
        validate_lookup_name(name, allowed_names, lookup_type)

        try:
            existing = get_by_name_method(name)
            if existing:
                context.log.info(f"{lookup_type} '{name}' already exists, using existing ID")
                results[name] = existing[id_field_name]
            else:
                code = name.upper().replace(" ", "_")
                lookup_obj = lookup_factory(name, None, code)
                lookup_id = insert_method(lookup_obj)
                results[name] = lookup_id
        except Exception as e:
            context.log.error(f"Error processing {lookup_type} {name}: {e}")
            raise
    return results


def process_product_type_lookup(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    values: list,
    allowed_names: Set[str],
) -> Dict[str, int]:
    """Process product_type lookup (has is_derived flag)."""
    results = {}
    for name_value in values:
        if not name_value or str(name_value).strip() == "":
            continue
        name = str(name_value).strip()
        validate_lookup_name(name, allowed_names, "product_type")

        try:
            existing = lookup_manager.get_product_type_by_name(name)
            if existing:
                context.log.info(f"product_type '{name}' already exists, using existing ID")
                results[name] = existing["product_type_id"]
            else:
                product_lookup = ProductTypeLookup(name=name, description=None, is_derived=False)
                lookup_id = lookup_manager.insert_product_type(product_lookup)
                results[name] = lookup_id
        except Exception as e:
            context.log.error(f"Error processing product_type {name}: {e}")
            raise
    return results


def process_sub_asset_class_lookup(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    df: pl.DataFrame,
    allowed_names: Set[str],
    asset_class_mapping: Dict[str, int],
) -> Dict[str, int]:
    """Process sub_asset_class lookup (requires asset_class mapping)."""
    results = {}
    for row in df.filter(pl.col("sub_asset_class").is_not_null()).iter_rows(named=True):
        sub_name = str(row["sub_asset_class"]).strip()
        asset_name = str(row.get("asset_class", "")).strip()

        if not sub_name or sub_name == "":
            continue

        validate_lookup_name(sub_name, allowed_names, "sub_asset_class")

        if not asset_name or asset_name not in asset_class_mapping:
            context.log.warning(
                f"Skipping sub_asset_class '{sub_name}' - asset_class '{asset_name}' not found"
            )
            continue

        try:
            existing = lookup_manager.get_sub_asset_class_by_name(sub_name)
            if existing:
                context.log.info(f"sub_asset_class '{sub_name}' already exists, using existing ID")
                results[sub_name] = existing["sub_asset_class_id"]
            else:
                sub_asset_lookup = SubAssetClassLookup(
                    name=sub_name, description=None, asset_class_id=asset_class_mapping[asset_name]
                )
                lookup_id = lookup_manager.insert_sub_asset_class(sub_asset_lookup)
                results[sub_name] = lookup_id
        except Exception as e:
            context.log.error(f"Error processing sub_asset_class {sub_name}: {e}")
            raise
    return results


def process_wide_format_lookup(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    df: pl.DataFrame,
    config: LookupTableCSVConfig,
    available_columns: list,
) -> Dict[str, Any]:
    """Process lookup tables from wide format CSV.

    Returns:
        Dictionary mapping lookup type to results (name -> id)
    """
    all_results: Dict[str, Any] = {}
    asset_class_mapping: Dict[str, int] = {}

    # Helper functions for creating lookup objects
    def create_data_type_lookup(name: str, description: Optional[str]) -> DataTypeLookup:
        """Create a DataTypeLookup object."""
        return DataTypeLookup(name=name, description=description)

    def create_structure_type_lookup(name: str, description: Optional[str]) -> StructureTypeLookup:
        """Create a StructureTypeLookup object."""
        return StructureTypeLookup(name=name, description=description)

    def create_market_segment_lookup(name: str, description: Optional[str]) -> MarketSegmentLookup:
        """Create a MarketSegmentLookup object."""
        return MarketSegmentLookup(name=name, description=description)

    def create_field_type_lookup(
        name: str, description: Optional[str], field_type_code: str
    ) -> FieldTypeLookup:
        """Create a FieldTypeLookup object."""
        return FieldTypeLookup(name=name, description=description, field_type_code=field_type_code)

    def create_ticker_source_lookup(
        name: str, description: Optional[str], ticker_source_code: str
    ) -> TickerSourceLookup:
        """Create a TickerSourceLookup object."""
        return TickerSourceLookup(
            name=name, description=description, ticker_source_code=ticker_source_code
        )

    def create_region_lookup(name: str, description: Optional[str]) -> RegionLookup:
        """Create a RegionLookup object."""
        return RegionLookup(name=name, description=description)

    def create_currency_lookup(
        name: str, description: Optional[str], currency_code: Optional[str] = None
    ) -> CurrencyLookup:
        """Create a CurrencyLookup object."""
        return CurrencyLookup(
            name=name, description=description, currency_code=currency_code or name
        )

    def create_term_lookup(name: str, description: Optional[str]) -> TermLookup:
        """Create a TermLookup object."""
        return TermLookup(name=name, description=description)

    def create_tenor_lookup(
        name: str, description: Optional[str], tenor_code: Optional[str] = None
    ) -> TenorLookup:
        """Create a TenorLookup object."""
        return TenorLookup(name=name, description=description, tenor_code=tenor_code or name)

    def create_country_lookup(
        name: str, description: Optional[str], country_code: Optional[str] = None
    ) -> CountryLookup:
        """Create a CountryLookup object."""
        return CountryLookup(name=name, description=description, country_code=country_code or name)

    # Processor functions for each lookup type
    def process_asset_class(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process asset_class lookup values."""
        return process_asset_class_lookup(context, lookup_manager, values, allowed_names)

    def process_product_type(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process product_type lookup values."""
        return process_product_type_lookup(context, lookup_manager, values, allowed_names)

    def process_data_type(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process data_type lookup values."""
        return process_simple_lookup_type(
            context,
            lookup_manager,
            "data_type",
            values,
            allowed_names,
            create_data_type_lookup,
            lookup_manager.insert_data_type,
            lookup_manager.get_data_type_by_name,
            "data_type_id",
        )

    def process_structure_type(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process structure_type lookup values."""
        return process_simple_lookup_type(
            context,
            lookup_manager,
            "structure_type",
            values,
            allowed_names,
            create_structure_type_lookup,
            lookup_manager.insert_structure_type,
            lookup_manager.get_structure_type_by_name,
            "structure_type_id",
        )

    def process_market_segment(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process market_segment lookup values."""
        return process_simple_lookup_type(
            context,
            lookup_manager,
            "market_segment",
            values,
            allowed_names,
            create_market_segment_lookup,
            lookup_manager.insert_market_segment,
            lookup_manager.get_market_segment_by_name,
            "market_segment_id",
        )

    def process_field_type(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process field_type lookup values."""
        return process_code_based_lookup_type(
            context,
            lookup_manager,
            "field_type",
            values,
            allowed_names,
            create_field_type_lookup,
            lookup_manager.insert_field_type,
            lookup_manager.get_field_type_by_name,
            "field_type_id",
        )

    def process_ticker_source(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process ticker_source lookup values."""
        return process_code_based_lookup_type(
            context,
            lookup_manager,
            "ticker_source",
            values,
            allowed_names,
            create_ticker_source_lookup,
            lookup_manager.insert_ticker_source,
            lookup_manager.get_ticker_source_by_name,
            "ticker_source_id",
        )

    def process_region(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process region lookup values."""
        return process_simple_lookup_type(
            context,
            lookup_manager,
            "region",
            values,
            allowed_names,
            create_region_lookup,
            lookup_manager.insert_region,
            lookup_manager.get_region_by_name,
            "region_id",
        )

    def process_currency(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process currency lookup values."""
        results = {}
        for value in values:
            if not value or str(value).strip() == "":
                continue
            code = str(value).strip()
            validate_lookup_name(code, allowed_names, "currency")
            try:
                existing = lookup_manager.get_currency_by_code(code)
                if existing:
                    context.log.info(f"currency '{code}' already exists, using existing ID")
                    results[code] = existing["currency_id"]
                else:
                    currency_lookup = create_currency_lookup(code, None, code)
                    lookup_id = lookup_manager.insert_currency(currency_lookup)
                    results[code] = lookup_id
            except Exception as e:
                context.log.error(f"Error processing currency {code}: {e}")
                raise
        return results

    def process_term(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process term lookup values."""
        return process_simple_lookup_type(
            context,
            lookup_manager,
            "term",
            values,
            allowed_names,
            create_term_lookup,
            lookup_manager.insert_term,
            lookup_manager.get_term_by_name,
            "term_id",
        )

    def process_tenor(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process tenor lookup values."""
        results = {}
        for value in values:
            if not value or str(value).strip() == "":
                continue
            code = str(value).strip()
            validate_lookup_name(code, allowed_names, "tenor")
            try:
                existing = lookup_manager.get_tenor_by_code(code)
                if existing:
                    context.log.info(f"tenor '{code}' already exists, using existing ID")
                    results[code] = existing["tenor_id"]
                else:
                    tenor_lookup = create_tenor_lookup(code, None, code)
                    lookup_id = lookup_manager.insert_tenor(tenor_lookup)
                    results[code] = lookup_id
            except Exception as e:
                context.log.error(f"Error processing tenor {code}: {e}")
                raise
        return results

    def process_country(values: list, allowed_names: Set[str]) -> Dict[str, int]:
        """Process country lookup values."""
        results = {}
        for value in values:
            if not value or str(value).strip() == "":
                continue
            code = str(value).strip()
            validate_lookup_name(code, allowed_names, "country")
            try:
                existing = lookup_manager.get_country_by_code(code)
                if existing:
                    context.log.info(f"country '{code}' already exists, using existing ID")
                    results[code] = existing["country_id"]
                else:
                    country_lookup = create_country_lookup(code, None, code)
                    lookup_id = lookup_manager.insert_country(country_lookup)
                    results[code] = lookup_id
            except Exception as e:
                context.log.error(f"Error processing country {code}: {e}")
                raise
        return results

    def get_asset_class_mapping() -> Dict[str, int]:
        """Get the asset_class mapping."""
        return asset_class_mapping

    # Define lookup type processors
    lookup_processors = {
        "asset_class": (process_asset_class, get_asset_class_mapping),
        "product_type": (process_product_type, None),
        "data_type": (process_data_type, None),
        "structure_type": (process_structure_type, None),
        "market_segment": (process_market_segment, None),
        "field_type": (process_field_type, None),
        "ticker_source": (process_ticker_source, None),
        "region": (process_region, None),
        "currency": (process_currency, None),
        "term": (process_term, None),
        "tenor": (process_tenor, None),
        "country": (process_country, None),
    }

    # Process independent lookup types first
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type in available_columns:
            if config.lookup_table_type in ("all", lookup_type):
                allowed_names = load_allowed_names(config.allowed_names_csv_path, lookup_type)
                values = df[lookup_type].drop_nulls().unique().to_list()
                context.log.info(f"Loading {len(values)} {lookup_type}s")

                processor, result_getter = lookup_processors[lookup_type]
                results = processor(values, allowed_names)

                if result_getter:
                    # For asset_class, update the mapping
                    asset_class_mapping.update(results)
                    all_results[lookup_type] = asset_class_mapping
                else:
                    all_results[lookup_type] = results

    # Process sub_asset_class last (depends on asset_class)
    if "sub_asset_class" in available_columns and "asset_class" in available_columns:
        if config.lookup_table_type in ("all", "sub_asset_class"):
            # Ensure asset_class_mapping is populated
            if not asset_class_mapping:
                for name in df["asset_class"].drop_nulls().unique().to_list():
                    if name and str(name).strip():
                        existing = lookup_manager.get_asset_class_by_name(str(name).strip())
                        if existing:
                            asset_class_mapping[str(name).strip()] = existing["asset_class_id"]

            allowed_names = load_allowed_names(config.allowed_names_csv_path, "sub_asset_class")
            context.log.info(
                f"Loading sub_asset_classes with {len(asset_class_mapping)} asset classes available"
            )

            results = process_sub_asset_class_lookup(
                context, lookup_manager, df, allowed_names, asset_class_mapping
            )
            all_results["sub_asset_class"] = results

    return all_results


def process_long_format_lookup(
    context: AssetExecutionContext,
    lookup_manager: LookupTableManager,
    df: pl.DataFrame,
    config: LookupTableCSVConfig,
) -> Dict[str, int]:
    """Process lookup tables from long format CSV.

    Returns:
        Dictionary mapping name to lookup_id
    """
    allowed_names = load_allowed_names(config.allowed_names_csv_path, config.lookup_table_type)
    context.log.info(
        f"Loaded {len(allowed_names)} allowed names from {config.allowed_names_csv_path}"
    )

    if "lookup_table_type" in df.columns:
        df = df.filter(pl.col("lookup_table_type") == config.lookup_table_type)
        context.log.info(f"Filtered to {len(df)} rows for {config.lookup_table_type}")

    results: Dict[str, int] = {}

    # Handler functions for each lookup type
    def handle_asset_class(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle asset_class insertion for long format."""
        _handle_asset_class(lookup_manager, name, description, results)

    def handle_product_type(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle product_type insertion for long format."""
        _handle_product_type(lookup_manager, name, description, row, results)

    def handle_sub_asset_class(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle sub_asset_class insertion for long format."""
        _handle_sub_asset_class(lookup_manager, name, description, row, results)

    def handle_data_type(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle data_type insertion for long format."""
        _handle_simple_lookup(
            lookup_manager,
            name,
            description,
            DataTypeLookup,
            lookup_manager.insert_data_type,
            lookup_manager.get_data_type_by_name,
            "data_type_id",
            results,
        )

    def handle_structure_type(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle structure_type insertion for long format."""
        _handle_simple_lookup(
            lookup_manager,
            name,
            description,
            StructureTypeLookup,
            lookup_manager.insert_structure_type,
            lookup_manager.get_structure_type_by_name,
            "structure_type_id",
            results,
        )

    def handle_market_segment(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle market_segment insertion for long format."""
        _handle_simple_lookup(
            lookup_manager,
            name,
            description,
            MarketSegmentLookup,
            lookup_manager.insert_market_segment,
            lookup_manager.get_market_segment_by_name,
            "market_segment_id",
            results,
        )

    def handle_field_type(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle field_type insertion for long format."""
        _handle_field_type(lookup_manager, name, description, row, results)

    def handle_ticker_source(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle ticker_source insertion for long format."""
        _handle_ticker_source(lookup_manager, name, description, row, results)

    def handle_region(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle region insertion for long format."""
        _handle_simple_lookup(
            lookup_manager,
            name,
            description,
            RegionLookup,
            lookup_manager.insert_region,
            lookup_manager.get_region_by_name,
            "region_id",
            results,
        )

    def handle_currency(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle currency insertion for long format."""
        existing = lookup_manager.get_currency_by_code(name)
        if existing:
            results[name] = existing["currency_id"]
        else:
            currency_code = row.get("currency_code", name)
            currency_name = row.get("currency_name")
            currency_lookup = CurrencyLookup(
                name=name,
                description=description,
                currency_code=currency_code,
                currency_name=currency_name,
            )
            lookup_id = lookup_manager.insert_currency(currency_lookup)
            results[name] = lookup_id

    def handle_term(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle term insertion for long format."""
        _handle_simple_lookup(
            lookup_manager,
            name,
            description,
            TermLookup,
            lookup_manager.insert_term,
            lookup_manager.get_term_by_name,
            "term_id",
            results,
        )

    def handle_tenor(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle tenor insertion for long format."""
        existing = lookup_manager.get_tenor_by_code(name)
        if existing:
            results[name] = existing["tenor_id"]
        else:
            tenor_code = row.get("tenor_code", name)
            tenor_name = row.get("tenor_name")
            tenor_lookup = TenorLookup(
                name=name, description=description, tenor_code=tenor_code, tenor_name=tenor_name
            )
            lookup_id = lookup_manager.insert_tenor(tenor_lookup)
            results[name] = lookup_id

    def handle_country(name: str, description: Optional[str], row: Dict[str, Any]) -> None:
        """Handle country insertion for long format."""
        existing = lookup_manager.get_country_by_code(name)
        if existing:
            results[name] = existing["country_id"]
        else:
            country_code = row.get("country_code", name)
            country_name = row.get("country_name")
            country_lookup = CountryLookup(
                name=name,
                description=description,
                country_code=country_code,
                country_name=country_name,
            )
            lookup_id = lookup_manager.insert_country(country_lookup)
            results[name] = lookup_id

    # Define handlers for each lookup type
    handlers = {
        "asset_class": handle_asset_class,
        "product_type": handle_product_type,
        "sub_asset_class": handle_sub_asset_class,
        "data_type": handle_data_type,
        "structure_type": handle_structure_type,
        "market_segment": handle_market_segment,
        "field_type": handle_field_type,
        "ticker_source": handle_ticker_source,
        "region": handle_region,
        "currency": handle_currency,
        "term": handle_term,
        "tenor": handle_tenor,
        "country": handle_country,
    }

    handler = handlers.get(config.lookup_table_type)
    if not handler:
        raise ValueError(f"Unknown lookup table type: {config.lookup_table_type}")

    # Process each row
    for row in df.iter_rows(named=True):
        name = row["name"]
        validate_lookup_name(name, allowed_names, config.lookup_table_type)
        description = row.get("description")

        try:
            handler(name, description, row)
        except Exception as e:
            context.log.error(f"Error processing row for {name}: {e}")
            raise

    return results


def _handle_asset_class(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    results: Dict[str, int],
) -> None:
    """Handle asset_class insertion for long format."""
    asset_lookup = AssetClassLookup(name=name, description=description)
    existing = lookup_manager.get_asset_class_by_name(name)
    if not existing:
        lookup_id = lookup_manager.insert_asset_class(asset_lookup)
        results[name] = lookup_id
    else:
        results[name] = existing["asset_class_id"]


def _handle_product_type(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    row: Dict[str, Any],
    results: Dict[str, int],
) -> None:
    """Handle product_type insertion for long format."""
    existing = lookup_manager.get_product_type_by_name(name)
    if existing:
        results[name] = existing["product_type_id"]
    else:
        is_derived = row.get("is_derived", False)
        if isinstance(is_derived, str):
            is_derived = is_derived.lower() in ("true", "1", "yes")
        product_lookup = ProductTypeLookup(
            name=name, description=description, is_derived=bool(is_derived)
        )
        lookup_id = lookup_manager.insert_product_type(product_lookup)
        results[name] = lookup_id


def _handle_sub_asset_class(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    row: Dict[str, Any],
    results: Dict[str, int],
) -> None:
    """Handle sub_asset_class insertion for long format."""
    existing = lookup_manager.get_sub_asset_class_by_name(name)
    if existing:
        results[name] = existing["sub_asset_class_id"]
    else:
        asset_class_id = row.get("asset_class_id")
        if asset_class_id is None:
            raise ValueError("sub_asset_class requires asset_class_id")
        sub_asset_lookup = SubAssetClassLookup(
            name=name, description=description, asset_class_id=int(asset_class_id)
        )
        lookup_id = lookup_manager.insert_sub_asset_class(sub_asset_lookup)
        results[name] = lookup_id


def _handle_simple_lookup(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    lookup_class: type,
    insert_method: Callable,
    get_by_name_method: Callable[[str], Optional[Dict[str, Any]]],
    id_field_name: str,
    results: Dict[str, int],
) -> None:
    """Handle simple lookup insertion for long format."""
    existing = get_by_name_method(name)
    if existing:
        results[name] = existing[id_field_name]
    else:
        lookup_obj = lookup_class(name=name, description=description)
        lookup_id = insert_method(lookup_obj)
        results[name] = lookup_id


def _handle_field_type(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    row: Dict[str, Any],
    results: Dict[str, int],
) -> None:
    """Handle field_type insertion for long format."""
    existing = lookup_manager.get_field_type_by_name(name)
    if existing:
        results[name] = existing["field_type_id"]
    else:
        field_type_code = row.get("field_type_code")
        if not field_type_code:
            raise ValueError("field_type requires field_type_code")
        field_lookup = FieldTypeLookup(
            name=name, description=description, field_type_code=str(field_type_code)
        )
        lookup_id = lookup_manager.insert_field_type(field_lookup)
        results[name] = lookup_id


def _handle_ticker_source(
    lookup_manager: LookupTableManager,
    name: str,
    description: Optional[str],
    row: Dict[str, Any],
    results: Dict[str, int],
) -> None:
    """Handle ticker_source insertion for long format."""
    existing = lookup_manager.get_ticker_source_by_name(name)
    if existing:
        results[name] = existing["ticker_source_id"]
    else:
        ticker_source_code = row.get("ticker_source_code")
        if not ticker_source_code:
            raise ValueError("ticker_source requires ticker_source_code")
        ticker_lookup = TickerSourceLookup(
            name=name,
            description=description,
            ticker_source_code=str(ticker_source_code),
        )
        lookup_id = lookup_manager.insert_ticker_source(ticker_lookup)
        results[name] = lookup_id


@asset(
    group_name="metadata",
    description="Initialize database schema - create all required tables",
    io_manager_key="polars_parquet_io_manager",
    kinds=["clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def init_database_schema(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Initialize database schema using clickhouse-migrate.

    This asset ensures the database exists and runs all pending migrations
    to set up the database schema.
    """
    context.log.info("Initializing database schema using clickhouse-migrate...")

    try:
        # Ensure database exists
        clickhouse.ensure_database()
        context.log.info("Database ensured")

        # Run migrations
        clickhouse.run_migrations()
        context.log.info("ClickHouse migrations applied successfully")
    except Exception as e:
        context.log.error(f"Error initializing schema with clickhouse-migrate: {e}")
        raise

    context.log.info("Database schema initialized successfully")
    # Return as DataFrame for polars_parquet_io_manager
    from dagster_quickstart.utils.datetime_utils import utc_now

    return pl.DataFrame({"status": ["Schema initialized"], "timestamp": [utc_now()]})


@asset(
    group_name="metadata",
    description="Load lookup tables from CSV with validation against allowed names",
    deps=[AssetKey("init_database_schema")],  # Schema must be initialized first
    io_manager_key="polars_parquet_io_manager",
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_lookup_tables_from_csv(
    context: AssetExecutionContext,
    config: LookupTableCSVConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Load lookup tables from CSV file with validation.

    Supports two CSV formats:
    1. Wide format: Columns are lookup table types (asset_class, product_type, etc.)
       When in wide format and lookup_table_type is "all", processes all lookup tables.
    2. Long format: Has lookup_table_type and name columns
    """
    context.log.info(f"Loading {config.lookup_table_type} from {config.csv_path}")

    # Load lookup table data
    df = read_csv_safe(config.csv_path)

    lookup_manager = LookupTableManager(clickhouse)
    available_columns = [col for col in LOOKUP_TABLE_COLUMNS if col in df.columns]

    if available_columns:
        # Wide format: process all lookup tables
        context.log.info(f"Detected wide format with columns: {available_columns}")
        all_results = process_wide_format_lookup(
            context, lookup_manager, df, config, available_columns
        )

        # Return results based on requested type
        if config.lookup_table_type != "all" and config.lookup_table_type in all_results:
            results = all_results[config.lookup_table_type]
            # Convert dictionary to Polars DataFrame
            result_df = pl.DataFrame(
                [
                    {"lookup_table_type": config.lookup_table_type, "name": name, "id": id_val}
                    for name, id_val in results.items()
                ]
            )
            context.add_output_metadata(
                {
                    "lookups_loaded": MetadataValue.int(len(result_df)),
                    "lookup_table_type": MetadataValue.text(config.lookup_table_type),
                    "details": MetadataValue.json(results),
                }
            )
            return result_df
        elif config.lookup_table_type == "all":
            total_loaded = sum(len(v) for v in all_results.values())
            # Convert all results to a single DataFrame
            rows = []
            for lookup_type, type_results in all_results.items():
                for name, id_val in type_results.items():
                    rows.append({"lookup_table_type": lookup_type, "name": name, "id": id_val})
            result_df = pl.DataFrame(rows)
            context.add_output_metadata(
                {
                    "lookups_loaded": MetadataValue.int(total_loaded),
                    "lookup_table_type": MetadataValue.text("all"),
                    "details": MetadataValue.json(all_results),
                }
            )
            return result_df
        else:
            raise ValueError(
                f"lookup_table_type '{config.lookup_table_type}' not found in CSV columns. "
                f"Available columns: {available_columns}"
            )

    elif "name" in df.columns:
        # Long format: has lookup_table_type and name columns
        results = process_long_format_lookup(context, lookup_manager, df, config)
        # Convert dictionary to Polars DataFrame
        result_df = pl.DataFrame(
            [
                {"lookup_table_type": config.lookup_table_type, "name": name, "id": id_val}
                for name, id_val in results.items()
            ]
        )
        context.add_output_metadata(
            {
                "lookups_loaded": MetadataValue.int(len(result_df)),
                "lookup_table_type": MetadataValue.text(config.lookup_table_type),
                "details": MetadataValue.json(results),
            }
        )
        return result_df
    else:
        raise CSVValidationError(
            f"CSV file {config.csv_path} must have lookup table columns "
            f"(wide format: {LOOKUP_TABLE_COLUMNS}) or 'name' column (long format)"
        )


@asset(
    group_name="metadata",
    description="Load meta series from CSV file",
    deps=[
        AssetKey("init_database_schema"),  # Schema must be initialized first
        AssetKey("load_lookup_tables_from_csv"),  # Depends on lookup tables being loaded first
    ],
    io_manager_key="polars_parquet_io_manager",
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_meta_series_from_csv(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Load meta series from CSV file."""
    context.log.info(f"Loading meta series from {config.csv_path}")

    # Load meta series data
    df = read_csv_safe(
        config.csv_path,
        null_values=NULL_VALUE_REPRESENTATION,
        truncate_ragged_lines=True,
    )

    # Validate required columns
    validate_csv_columns(df, META_SERIES_REQUIRED_COLUMNS, config.csv_path)

    meta_manager = MetaSeriesManager(clickhouse)
    lookup_manager = LookupTableManager(clickhouse)
    results = {}

    # Process each row
    for row in df.iter_rows(named=True):
        try:
            # Skip empty rows (where series_name or series_code is missing)
            if is_empty_row(row, ["series_name", "series_code"]):
                context.log.warning("Skipping empty row")
                continue

            # Parse data_source
            data_source_str = str(row.get("data_source", ""))
            data_source = parse_data_source(data_source_str)

            # Look up IDs for region, currency, term, tenor, country if string values are provided
            region_id = resolve_lookup_id_from_string(
                row,
                "region_id",
                "region",
                lookup_manager,
                "get_region_by_name",
                context,
            )

            currency_id = resolve_lookup_id_from_string(
                row,
                "currency_id",
                "currency",
                lookup_manager,
                "get_currency_by_code",
                context,
            )

            term_id = resolve_lookup_id_from_string(
                row,
                "term_id",
                "term",
                lookup_manager,
                "get_term_by_name",
                context,
            )

            tenor_id = resolve_lookup_id_from_string(
                row,
                "tenor_id",
                "tenor",
                lookup_manager,
                "get_tenor_by_code",
                context,
            )

            # Country can be in either "country" or "countries" field
            country_id = resolve_lookup_id_from_string(
                row,
                "country_id",
                "country",
                lookup_manager,
                "get_country_by_code",
                context,
            )
            if not country_id:
                country_id = resolve_lookup_id_from_string(
                    row,
                    "country_id",
                    "countries",
                    lookup_manager,
                    "get_country_by_code",
                    context,
                )

            # Parse is_active (default to True if not provided)
            is_active = True
            if row.get("is_active") is not None:
                is_active_val = str(row.get("is_active")).strip().lower()
                is_active = is_active_val in ("1", "true", "yes", "y", "active")

            meta_series = MetaSeriesCreate(
                series_name=str(row["series_name"]),
                series_code=str(row["series_code"]),
                data_source=data_source,
                field_type_id=safe_int(row.get("field_type_id"), "field_type_id", required=False),
                asset_class_id=safe_int(
                    row.get("asset_class_id"), "asset_class_id", required=False
                ),
                sub_asset_class_id=safe_int(
                    row.get("sub_asset_class_id"), "sub_asset_class_id", required=False
                ),
                product_type_id=safe_int(
                    row.get("product_type_id"), "product_type_id", required=False
                ),
                data_type_id=safe_int(row.get("data_type_id"), "data_type_id", required=False),
                structure_type_id=safe_int(
                    row.get("structure_type_id"), "structure_type_id", required=False
                ),
                market_segment_id=safe_int(
                    row.get("market_segment_id"), "market_segment_id", required=False
                ),
                ticker_source_id=safe_int(
                    row.get("ticker_source_id"), "ticker_source_id", required=False
                ),
                ticker=str(row["ticker"]),
                region_id=region_id,
                currency_id=currency_id,
                term_id=term_id,
                tenor_id=tenor_id,
                country_id=country_id,
                calculation_formula=str(row["calculation_formula"])
                if row.get("calculation_formula")
                else None,
                description=str(row["description"]) if row.get("description") else None,
                is_active=is_active,
            )

            # Check if series already exists
            existing = meta_manager.get_meta_series_by_code(meta_series.series_code)
            if existing:
                context.log.warning(
                    f"Meta series {meta_series.series_code} already exists, skipping"
                )
                results[meta_series.series_code] = existing["series_id"]
            else:
                series_id = meta_manager.create_meta_series(meta_series, created_by="csv_loader")
                results[meta_series.series_code] = series_id

        except (CSVValidationError, DataSourceValidationError, MetaSeriesNotFoundError) as e:
            context.log.error(
                f"Validation error processing row for {row.get('series_code', 'unknown')}: {e}"
            )
            raise
        except (ValueError, TypeError) as e:
            context.log.error(
                f"Data error processing row for {row.get('series_code', 'unknown')}: {e}"
            )
            raise CSVValidationError(
                f"Invalid data in row for {row.get('series_code', 'unknown')}: {e}"
            ) from e
        except Exception as e:
            context.log.error(
                f"Unexpected error processing row for {row.get('series_code', 'unknown')}: {e}"
            )
            raise CSVValidationError(f"Unexpected error processing row: {e}") from e

    # Convert dictionary to Polars DataFrame
    result_df = pl.DataFrame(
        [
            {"series_code": series_code, "series_id": series_id}
            for series_code, series_id in results.items()
        ]
    )
    context.add_output_metadata(
        {
            "series_loaded": MetadataValue.int(len(result_df)),
            "details": MetadataValue.json(results),
        }
    )

    return result_df
