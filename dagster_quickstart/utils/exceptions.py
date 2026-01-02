"""Custom exceptions for Dagster assets."""


class CSVValidationError(ValueError):
    """Raised when CSV validation fails."""

    pass


class DataSourceValidationError(ValueError):
    """Raised when data source validation fails."""

    pass


class LookupTableError(ValueError):
    """Raised when lookup table operations fail."""

    pass


class MetaSeriesNotFoundError(ValueError):
    """Raised when a meta series is not found."""

    pass


class CalculationError(ValueError):
    """Raised when calculation operations fail."""

    pass


class DatabaseError(Exception):
    """Base exception for database operations."""

    pass


class DatabaseQueryError(DatabaseError):
    """Raised when a database query fails."""

    pass


class DatabaseInsertError(DatabaseError):
    """Raised when a database insert operation fails."""

    pass


class DatabaseUpdateError(DatabaseError):
    """Raised when a database update operation fails."""

    pass


class RecordNotFoundError(DatabaseError):
    """Raised when a requested record is not found."""

    pass


class PyPDLError(Exception):
    """Raised when PyPDL operations fail."""

    pass


class PyPDLConnectionError(PyPDLError):
    """Raised when PyPDL connection fails."""

    pass


class PyPDLExecutionError(PyPDLError):
    """Raised when PyPDL execution fails."""

    pass


class DataQualityError(Exception):
    """Base exception for data quality checks."""

    pass


class TimelinessCheckError(DataQualityError):
    """Raised when timeliness checks fail."""

    pass


class CompletenessCheckError(DataQualityError):
    """Raised when completeness checks fail."""

    pass


class AccuracyCheckError(DataQualityError):
    """Raised when accuracy checks fail."""

    pass


class ValidityCheckError(DataQualityError):
    """Raised when validity checks fail."""

    pass


class UniquenessCheckError(DataQualityError):
    """Raised when uniqueness checks fail."""

    pass


class ConsistencyCheckError(DataQualityError):
    """Raised when consistency checks fail."""

    pass


class GreatExpectationsError(Exception):
    """Raised when Great Expectations operations fail."""

    pass


class ReferentialIntegrityError(DatabaseError):
    """Base exception for referential integrity violations."""

    pass


class LookupNotFoundError(ReferentialIntegrityError):
    """Raised when a referenced lookup value does not exist."""

    def __init__(self, lookup_type: str, lookup_value: str, message: str = ""):
        self.lookup_type = lookup_type
        self.lookup_value = lookup_value
        if not message:
            message = f"Lookup value '{lookup_value}' not found in {lookup_type} lookup table"
        super().__init__(message)


class InvalidLookupReferenceError(ReferentialIntegrityError):
    """Raised when a meta series references an invalid lookup ID or name."""

    def __init__(self, lookup_type: str, lookup_value: str, series_code: str = "", message: str = ""):
        self.lookup_type = lookup_type
        self.lookup_value = lookup_value
        self.series_code = series_code
        if not message:
            if series_code:
                message = f"Series '{series_code}' references invalid {lookup_type} '{lookup_value}'"
            else:
                message = f"Invalid {lookup_type} reference: '{lookup_value}'"
        super().__init__(message)


class SubAssetClassMismatchError(ReferentialIntegrityError):
    """Raised when sub_asset_class does not match the provided asset_class."""

    def __init__(self, sub_asset_class: str, asset_class: str, series_code: str = "", message: str = ""):
        self.sub_asset_class = sub_asset_class
        self.asset_class = asset_class
        self.series_code = series_code
        if not message:
            if series_code:
                message = f"Series '{series_code}': sub_asset_class '{sub_asset_class}' does not belong to asset_class '{asset_class}'"
            else:
                message = f"sub_asset_class '{sub_asset_class}' does not belong to asset_class '{asset_class}'"
        super().__init__(message)
