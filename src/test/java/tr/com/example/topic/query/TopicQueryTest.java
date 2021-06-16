package tr.com.example.topic.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import tr.com.example.kafka.CustomKafkaReceiver;
import tr.com.example.kafka.TopicNotFoundException;
import tr.com.example.kafka.TopicRecord;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static tr.com.example.topic.query.TopicQuerySampleData.DEFAULT_OFFSET;
import static tr.com.example.topic.query.TopicQuerySampleData.toReceiverRecord;

@ExtendWith(MockitoExtension.class)
class TopicQueryTest {
    static final String CITY_TOPIC = "city-topic";
    static final String ANGRY_TOPIC = "angry-topic";

    @Mock(lenient = true)
    private CustomKafkaReceiver customKafkaReceiver;
    private TopicNameProps topicNameProps;
    private TopicQuery topicQuery;

    @BeforeEach
    void setUp() {
        topicNameProps = new TopicNameProps();
        topicQuery = new TopicQuery(customKafkaReceiver, topicNameProps);
    }

    @AfterEach
    void tearDown() {
    }

    @ParameterizedTest(name = "#{index} - Return {3} => Given offset is {0}")
    @MethodSource("getTestData_receive")
    void returnTopicRecordsByOffset(String offset,
                                    List<TopicQuerySampleData.City> givenCityList,
                                    List<TopicRecord<?, ?>> expectedCityList,
                                    String resultTag) {//Tag for test name
        // Given
        given(customKafkaReceiver.receiveFromKafka(CITY_TOPIC, offset, topicNameProps.getGroupIdPrefix()))
                .willReturn(toReceiverRecord(CITY_TOPIC, givenCityList));
        given(customKafkaReceiver.topicExists(CITY_TOPIC))
                .willReturn(true);

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQuery
                .receive(CITY_TOPIC, offset);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    @ParameterizedTest(name = "#{index} - Return {4} => Given Offset is {0} AND Id is {1}")
    @MethodSource("getTestData_receiveWithId")
    void returnTopicRecordsByOffsetWithId(String offset,
                                          String id,
                                          List<TopicQuerySampleData.City> givenCityList,
                                          List<TopicRecord<?, ?>> expectedCityList,
                                          String resultTag) {//Tag for test name
        // Given
        given(customKafkaReceiver.receiveFromKafka(CITY_TOPIC, offset, topicNameProps.getGroupIdPrefix()))
                .willReturn(toReceiverRecord(CITY_TOPIC, givenCityList));
        given(customKafkaReceiver.topicExists(CITY_TOPIC))
                .willReturn(true);

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQuery
                .receive(CITY_TOPIC, offset, id);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    @Test
    void throwTopicNotFoundExceptionWhenUnknownTopicIsGiven() {
        // Given
        given(customKafkaReceiver.topicExists(ANGRY_TOPIC))
                .willReturn(false);

        // When && Then
        assertThrows(TopicNotFoundException.class, () -> {
            topicQuery.receive(ANGRY_TOPIC, DEFAULT_OFFSET);
        });
    }

    public static Stream<Arguments> getTestData_receive() {
        return TopicQuerySampleData.getTestData_receive_TopicQueryService();
    }

    public static Stream<Arguments> getTestData_receiveWithId() {
        return TopicQuerySampleData.getTestData_receiveWithId_TopicQueryService();
    }
}
