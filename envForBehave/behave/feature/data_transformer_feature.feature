Feature: data cleaner

  Scenario: transformed file should exist
    Given the transformed file path is configured
    When I read data from transformed data file
    Then transformed file should exist

  Scenario: Processed file column count is correct
    Given the transformed file is loaded into a DataFrame
    When I check the number of transformed columns
    Then the number of columns should be 27

  Scenario: Processed dataFrame schema matches expectations
    Given the transformed file is loaded into a DataFrame
    When I check the transformed DataFrame schema
    Then the transformed DataFrame schema should match the expected columns and data types