Feature: data loader api

  Scenario: API call returns 200
    Given the API url is configured
    When an API is called
    Then the response status code should be 200
    And the API call should returns a response

  Scenario: File count is correct
    Given the archive folder path is configured
    when I check the file count in archive folder
    then archive file count should match with the expected file count