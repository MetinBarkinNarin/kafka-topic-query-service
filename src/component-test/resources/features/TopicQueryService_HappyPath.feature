Feature: Topic Query Service
  Background:
    Given CITY_TOPIC populated with :
      | id | name      | sending_time   |
      | 6  | ANKARA    | before_request |
      | 50 | NEVSEHIR  | before_request |
      | 34 | ISTANBUL  | before_request |
      | 38 | KAYSERI   | before_request |
      | 1  | ADANA     | before_request |
      | 26 | ESKISEHIR | after_request  |

  Scenario Outline: Fetching All Requested Topic Records
    When Ahmet subscribes to CITY_TOPIC
    Then he should see <results>

    Examples: TOPIC QUERY SAMPLES
      | offset   | results    |
      | earliest | ALL_CITIES |

  Scenario Outline: Fetching All Requested Topic Records
    When Ahmet subscribes to CITY_TOPIC with <offset>
    Then he should see <results>

    Examples: TOPIC QUERY SAMPLES
      | offset   | results    |
      | latest   | ESKISEHIR  |
      | earliest | ALL_CITIES |

  Scenario Outline: Fetching All Requested Topic Records That Is Filtered With Id
    When Ahmet subscribes to CITY_TOPIC for <id> and <offset>
    Then he should see <results>

    Examples: TOPIC QUERY SAMPLES
      | id  | offset   | results    |
      | -1  | earliest | NO_RESULTS |
      | 100 | earliest | NO_RESULTS |
      | 26  | latest   | ESKISEHIR  |
      | 26  | earliest | ESKISEHIR  |
      | 50  | latest   | NO_RESULTS |
      | 50  | earliest | NEVSEHIR   |

