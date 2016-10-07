@twoprocessors
Feature: Concurrency
  Scenario:
    Given two concurrent atom feed event processors
    When 40 events are evenly distributed to the processors
    And the event threshold is 2
    Then 20 feeds are created
    And two events belong to each feed