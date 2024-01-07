Feature: data cleaner

  Scenario: Cleaned file should exist
    Given the cleaned file path is configured
    When I read data from clean data file
    Then file should exist

  Scenario: Column count is correct
    Given the cleaned file is loaded into a DataFrame
    When I check the number of columns
    Then the number of columns should be 22

  Scenario: Column length is correct
    Given the cleaned file is loaded into a DataFrame
    When I check the length of the FRSHTT column
    Then all values in the FRSHTT column should have a length of 6

  Scenario: DataFrame schema matches expectations
    Given the cleaned file is loaded into a DataFrame
    When I check the DataFrame schema
    Then the DataFrame schema should match the expected columns and data types