Feature: Topic Query Service

  Scenario: Fetching All Requested Topic Records
    Given no topic that is called ANGRY_TOPIC
    When Mehmet subscribes to ANGRY_TOPIC
    Then he should see TopicNotFoundException