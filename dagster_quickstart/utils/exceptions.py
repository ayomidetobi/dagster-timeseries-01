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
