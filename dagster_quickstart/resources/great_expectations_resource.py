"""Great Expectations resource for Dagster."""

from typing import Optional

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.validator.validator import Validator

from dagster_quickstart.utils.constants import GE_EXPECTATION_SUITE_PREFIX

logger = get_dagster_logger()


class GreatExpectationsResource(ConfigurableResource):
    """Great Expectations resource for Dagster using EphemeralDataContext."""

    datasource_name: str = "my_pandas_datasource"
    asset_name: str = "asset_check_df"

    def get_validator(
        self,
        asset_df: pd.DataFrame,
        suite_name: Optional[str] = None,
    ) -> Validator:
        """Get a Great Expectations validator for the given DataFrame.

        Args:
            asset_df: Pandas DataFrame to validate
            suite_name: Optional name of the expectation suite (for organization)

        Returns:
            Validator configured for the DataFrame
        """
        # Create ephemeral data context with in-memory storage
        project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
        data_context = EphemeralDataContext(project_config=project_config)

        # Add pandas datasource
        data_source: PandasDatasource = data_context.sources.add_pandas(name=self.datasource_name)

        # Add DataFrame asset
        data_asset = data_source.add_dataframe_asset(name=self.asset_name)

        # Build batch request from DataFrame
        batch_request = data_asset.build_batch_request(dataframe=asset_df)

        # Create or get expectation suite
        if suite_name:
            full_suite_name = f"{GE_EXPECTATION_SUITE_PREFIX}{suite_name}"
        else:
            full_suite_name = "asset_check_expectation_suite"
        data_context.add_or_update_expectation_suite(full_suite_name)

        # Get validator
        validator = data_context.get_validator(
            batch_request=batch_request, expectation_suite_name=full_suite_name
        )

        return validator

    def get_expectation_suite(self, suite_name: str) -> ExpectationSuite:
        """Get an expectation suite (for compatibility with existing code).

        Args:
            suite_name: Name of the expectation suite

        Returns:
            ExpectationSuite (ephemeral, created on-the-fly)
        """
        # Create ephemeral context to get suite structure
        project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
        data_context = EphemeralDataContext(project_config=project_config)
        full_suite_name = f"{GE_EXPECTATION_SUITE_PREFIX}{suite_name}"
        return data_context.add_or_update_expectation_suite(full_suite_name)
