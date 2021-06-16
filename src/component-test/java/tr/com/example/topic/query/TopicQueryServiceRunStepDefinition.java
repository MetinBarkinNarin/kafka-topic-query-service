package tr.com.example.topic.query;


import cucumber.api.DataTable;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tr.com.example.kafka.CustomKafkaReceiver;
import tr.com.example.kafka.TopicNotFoundException;
import tr.com.example.kafka.TopicRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class TopicQueryServiceRunStepDefinition {

    static final String CITY_TOPIC = "city-topic";
    static final String ANGRY_TOPIC = "angry-topic";
    static final String LATEST = "latest";
    static final String EARLIEST = "earliest";
    static final String DEFAULT_OFFSET = "earliest";
    static final int ID_INDEX = 0;
    static final int NAME_INDEX = 1;
    static final int SENDING_TIME_INDEX = 2;

    private Flux<TopicRecord> RESULT_DATA;

    private String EXCEPTION_TYPE;

    private List<TopicQuerySampleData.City> EARLIEST_CITIES;
    private List<TopicQuerySampleData.City> LATEST_CITIES;

    private TopicNameProps topicNameProps;
    private CustomKafkaReceiver customKafkaReceiver;
    private ITopicQueryController topicQueryController;

    @Before
    public void setUp() {
        customKafkaReceiver = mock(CustomKafkaReceiver.class);

        topicNameProps = new TopicNameProps();
        TopicQuery topicQuery = new TopicQuery(customKafkaReceiver, topicNameProps);
        ITopicQueryService topicQueryService = new TopicQueryService(topicQuery);
        topicQueryController = new TopicQueryController(topicQueryService);
    }

    @After
    public void tearDown() {
    }

    @Given("^CITY_TOPIC populated with :$")
    public void prepareCityTopicWithGivenData(DataTable dataTable) {
        EARLIEST_CITIES = dataTableToCityList(dataTable, EARLIEST);
        LATEST_CITIES = dataTableToCityList(dataTable, LATEST);

        given(customKafkaReceiver.receiveFromKafka(CITY_TOPIC, EARLIEST, topicNameProps.getGroupIdPrefix()))
                .willReturn(TopicQuerySampleData.toReceiverRecord(CITY_TOPIC, EARLIEST_CITIES));
        given(customKafkaReceiver.receiveFromKafka(CITY_TOPIC, LATEST, topicNameProps.getGroupIdPrefix()))
                .willReturn(TopicQuerySampleData.toReceiverRecord(CITY_TOPIC, LATEST_CITIES));
        given(customKafkaReceiver.topicExists(CITY_TOPIC))
                .willReturn(true);
        given(customKafkaReceiver.topicExists(ANGRY_TOPIC))
                .willReturn(false);
    }

    @Given("^no topic that is called ANGRY_TOPIC$")
    public void noTopicWithAngryTopic() {
    }

    @When("^Mehmet subscribes to ANGRY_TOPIC$")
    public void subscribeToAngryTopic() {
        try {
            RESULT_DATA = topicQueryController
                    .getTopic(ANGRY_TOPIC, DEFAULT_OFFSET)
                    .map(o -> ((TopicRecord) o));
        } catch (TopicNotFoundException e) {
            EXCEPTION_TYPE = "TopicNotFoundException";
        }
    }

    @When("^Ahmet subscribes to CITY_TOPIC$")
    public void subscribeToCityTopic() {
        RESULT_DATA = topicQueryController
                .getTopic(CITY_TOPIC, DEFAULT_OFFSET)
                .map(o -> ((TopicRecord) o));
    }

    @When("^Ahmet subscribes to CITY_TOPIC with (\\S+)$")
    public void subscribeToCityTopic(String offset) {
        RESULT_DATA = topicQueryController
                .getTopic(CITY_TOPIC, Objects.requireNonNull(offset))
                .map(o -> ((TopicRecord) o));
    }

    @When("^Ahmet subscribes to CITY_TOPIC for (\\S+) and (\\S+)$")
    public void subscribeToCityTopic(String id, String offset) {
        RESULT_DATA = topicQueryController
                .getTopic(CITY_TOPIC, Objects.requireNonNull(id), Objects.requireNonNull(offset))
                .map(o -> ((TopicRecord) o));
    }

    @Then("^he should see (.*)$")
    public void returnResultsAsTopicRecords(String result) {
        List<TopicRecord<?, ?>> expectedCityList;
        switch (result) {
            case "NO_RESULTS":
                expectedCityList = new ArrayList<>();
                verifyTopicRecords(RESULT_DATA, expectedCityList);
                break;
            case "ALL_CITIES":
                expectedCityList = TopicQuerySampleData.toTopicRecord(EARLIEST_CITIES.stream());
                verifyTopicRecords(RESULT_DATA, expectedCityList);
                break;
            case "TopicNotFoundException":
                assertThat(result).isEqualTo(EXCEPTION_TYPE);
                break;
            default:
                expectedCityList = TopicQuerySampleData.getTopicRecordsInResultString(TopicQuerySampleData.toTopicRecord(EARLIEST_CITIES.stream()), result);
                verifyTopicRecords(RESULT_DATA, expectedCityList);
                break;
        }
    }

    private void verifyTopicRecords(Flux<TopicRecord> resultTopicRecords, List<TopicRecord<?, ?>> expectedCityList) {
        StepVerifier.create(resultTopicRecords)
                .recordWith(ArrayList::new)
                .expectNextCount(expectedCityList.size())
                .consumeRecordedWith(results ->
                        assertThat(new ArrayList<>(results))
                                .isNotNull()
                                .usingRecursiveFieldByFieldElementComparator()
                                .containsExactlyInAnyOrderElementsOf(expectedCityList))
                .thenCancel()
                .verify();
    }


    private List<TopicQuerySampleData.City> dataTableToCityList(DataTable dataTable, String offset) {
        return dataTable.cells(1)
                .stream()
                .filter(fields -> !Objects.equals(offset, LATEST) ||
                        Objects.equals(
                                fields.get(SENDING_TIME_INDEX),
                                TopicQuerySampleData.SendingTime.AFTER_REQUEST.name().toLowerCase()
                        )
                )
                .map(fields -> new TopicQuerySampleData.City(fields.get(ID_INDEX), fields.get(NAME_INDEX)))
                .collect(Collectors.toList());
    }
}


