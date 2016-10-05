Feature: Feed initialization
  Scenario:
    Given a new feed environment
    When we start up the feed processor
    And some events are published
    And the number of events is lower than the feed threshold
    Then the events are stored in the recent table with a null feed if
    And there are no archived events
    And there are no records in the feeds table