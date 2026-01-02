"""Data quality checks using Great Expectations and Dagster asset checks.

This module provides asset checks covering 6 dimensions of data quality:
1. Timeliness - Data freshness and recency
2. Completeness - Missing values and required fields
3. Accuracy - Value ranges and outliers
4. Validity - Data types, formats, and constraints
5. Uniqueness - Duplicate detection
6. Consistency - Cross-field relationships and referential integrity
"""

from .expectation_suites import (
    build_accuracy_expectations,
    build_all_dimensions_expectations,
    build_completeness_expectations,
    build_consistency_expectations,
    build_timeliness_expectations,
    build_uniqueness_expectations,
    build_validity_expectations,
)

__all__ = [
    "build_all_dimensions_expectations",
    "build_accuracy_expectations",
    "build_completeness_expectations",
    "build_consistency_expectations",
    "build_timeliness_expectations",
    "build_uniqueness_expectations",
    "build_validity_expectations",
]
