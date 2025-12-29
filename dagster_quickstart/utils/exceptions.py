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
