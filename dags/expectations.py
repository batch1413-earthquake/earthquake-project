import great_expectations as gx


def run_expectations():
    """
    Validates a given DataFrame against a predefined set of expectations using Great Expectations.

    The function performs the following steps:
    1. Gets the current Great Expectations context.
    2. Adds or updates big query dataset.
    3. Adds a dataframe asset to the datasource.
    4. Builds a batch request using the provided DataFrame.
    5. Create an expectation suites
    6. Create validator
    7. Add rules in validator
    8. Creates or updates a checkpoint for validation.
    9. Runs the checkpoint and captures the result.

    Parameters:
    ----------
    None

    Returns:
    -------
       None: The function raises an exception if any test fails.
    """

    context = gx.get_context()
    datasource = context.sources.add_or_update_sql(name="my_bigquery_datasource", connection_string="bigquery://batch1413-earthquake/gold_earthquake_dataset")
    data_asset = datasource.add_table_asset(name="my_table_asset", table_name="earthquakes")
    batch_request = data_asset.build_batch_request()
    context.add_or_update_expectation_suite("earthquake_expectations")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="earthquake_expectations",
    )
    validator.head()
    validator.expect_column_values_to_not_be_null(column="id")
    validator.expect_column_values_to_be_between(column="latitude", min_value=-90, max_value=90)
    validator.expect_column_values_to_be_between(column="longitude", min_value=-180, max_value=180)
    validator.expect_column_values_to_be_between(column="properties_magnitude", min_value=0, max_value=10)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
    )
    checkpoint_result = checkpoint.run()

    for expectation_result in next(iter(checkpoint_result["run_results"].values()))["validation_result"]["results"]:
        if not expectation_result["success"]:
            raise Exception("Data validation failed. Error : " + expectation_result)


if __name__ == "__main__":
    run_expectations()
