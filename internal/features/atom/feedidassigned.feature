Feature: Feed id assigned
  Scenario:
    Given some initial events and no archived events and no feeds
    When the feed page threshold is reached
    And the archiver has not run
    Then feeds is updated with a new feedid with a null previous feed
    And the recent items with a null id are updated with the feedid
    And no records are added to the archive table

  Scenario:
    Given some initial events and some feeds and archive items
    When the feed page threshold is reached again
    And the archiver has not run
    Then feeds is update with a new feedid with the previous feed id as previous
    And the most recent items with a null id are updated with the new feedid
    And no new records are added to the archive table