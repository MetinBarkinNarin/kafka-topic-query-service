Feature: Topic Query Service

  Scenario Outline: Fetching All Requested Topic Records
    Given a <topic> populated with cities:
      | id | name      | sending_time   |
      | 6  | ANKARA    | before_request |
      | 50 | NEVSEHIR  | before_request |
      | 34 | ISTANBUL  | before_request |
      | 38 | KAYSERI   | before_request |
      | 1  | ADANA     | before_request |
      | 26 | ESKISEHIR | after_request  |
    When Ahmet requests to <request_topic> with <offset>
    Then He should see <results>

    Examples: TOPIC QUERY CALCULATION

      | topic      | offset   | request_topic | results    |
      | TEST_TOPIC | earliest | null          | Exception  |
      | TEST_TOPIC | earliest | UNKNOWN_TOPIC | Exception  |
      | TEST_TOPIC | latest   | TEST_TOPIC    | ESKISEHIR  |
      | TEST_TOPIC | earliest | TEST_TOPIC    | ALL_CITIES |

  Scenario Outline: Fetching All Requested Topic Records That Is Filtered With Id
    Given a <topic> populated with cities:
      | id | name      | when           |
      | 6  | ANKARA    | before_request |
      | 50 | NEVSEHIR  | before_request |
      | 34 | ISTANBUL  | before_request |
      | 38 | KAYSERI   | before_request |
      | 1  | ADANA     | before_request |
      | 26 | ESKISEHIR | after_request  |
    When Ahmet requests to <request_topic> with <id> and <offset>
    Then He should see <results>

    Examples: TOPIC QUERY CALCULATION

      | topic      | id   | offset   | request_topic | results    |
      | TEST_TOPIC | 6    | earliest | null          | Exception  |
      | TEST_TOPIC | 6    | earliest | UNKNOWN_TOPIC | Exception  |
      | TEST_TOPIC | null | earliest | TEST_TOPIC    | No Results |
      | TEST_TOPIC | 100  | earliest | TEST_TOPIC    | No Results |
      | TEST_TOPIC | 26   | latest   | TEST_TOPIC    | ESKISEHIR  |
      | TEST_TOPIC | 26   | earliest | TEST_TOPIC    | ESKISEHIR  |
      | TEST_TOPIC | 50   | latest   | TEST_TOPIC    | No Results |
      | TEST_TOPIC | 50   | earliest | TEST_TOPIC    | NEVSEHIR   |