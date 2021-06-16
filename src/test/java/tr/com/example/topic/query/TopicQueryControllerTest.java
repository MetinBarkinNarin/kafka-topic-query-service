package tr.com.example.topic.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import tr.com.example.kafka.TopicRecord;

import java.util.List;
import java.util.stream.Stream;

import static org.mockito.BDDMockito.given;
import static tr.com.example.topic.query.TopicQuerySampleData.toTopicRecordFluxWithoutCasting;

@ExtendWith(MockitoExtension.class)
class TopicQueryControllerTest {
    static final String CITY_TOPIC = "city-topic";

    @Mock(lenient = true)
    private TopicQueryService topicQueryService;
    private ITopicQueryController topicQueryController;

    @BeforeEach
    void setUp() {
        topicQueryController = new TopicQueryController(topicQueryService);
    }

    @AfterEach
    void tearDown() {
    }

    @ParameterizedTest(name = "#{index} - Return {3} => Given offset is {0}")
    @MethodSource("getTestData_getTopicWith")
    void returnTopicRecordsByOffset(String offset,
                                    List<TopicQuerySampleData.City> mockingData,
                                    List<TopicRecord<?, ?>> expectedCityList,
                                    String resultTag) {//Tag for test name
        // Given
        given(topicQueryService.getFlux(CITY_TOPIC, offset))
                .willReturn(toTopicRecordFluxWithoutCasting(mockingData));

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQueryController
                .getTopic(CITY_TOPIC, offset)
                .map(o -> (TopicRecord<?, ?>) o);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    @ParameterizedTest(name = "#{index} - Return {3} => Given offset is {0}")
    @MethodSource("getTestData_getTopicForClient")
    void returnTopicRecordsByOffsetForClient(String offset,
                                             List<TopicQuerySampleData.City> mockingData,
                                             List<ServerSentEvent<?>> expectedCityList,
                                             String resultTag) {//Tag for test name
        // Given
        given(topicQueryService.getFlux(CITY_TOPIC, offset))
                .willReturn(toTopicRecordFluxWithoutCasting(mockingData));

        // When
        Flux<ServerSentEvent<?>> resultData = topicQueryController
                .getTopicForClient(CITY_TOPIC, offset);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedServerSentEvent(resultData, expectedCityList);
    }

    @ParameterizedTest(name = "#{index} - Return {4} => Given Offset is {0} AND Id is {1}")
    @MethodSource("getTestData_getTopicWithId")
    void returnTopicRecordsByOffsetWithId(String offset,
                                          String id,
                                          List<TopicQuerySampleData.City> mockingData,
                                          List<TopicRecord<?, ?>> expectedCityList,
                                          String resultTag) {//Tag for test name
        // Given
        given(topicQueryService.getFlux(CITY_TOPIC, offset, id))
                .willReturn(toTopicRecordFluxWithoutCasting(mockingData));

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQueryController
                .getTopic(CITY_TOPIC, id, offset)
                .map(o -> (TopicRecord<?, ?>) o);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    @ParameterizedTest(name = "#{index} - Return {4} => Given Offset is {0} AND Id is {1}")
    @MethodSource("getTestData_getTopicForClientWithId")
    void returnTopicRecordsByOffsetWithIdForClient(String offset,
                                                   String id,
                                                   List<TopicQuerySampleData.City> mockingData,
                                                   List<ServerSentEvent<?>> expectedCityList,
                                                   String resultTag) {//Tag for test name
        // Given
        given(topicQueryService.getFlux(CITY_TOPIC, offset, id))
                .willReturn(toTopicRecordFluxWithoutCasting(mockingData));

        // When
        Flux<ServerSentEvent<?>> resultData = topicQueryController
                .getTopicForClient(CITY_TOPIC, id, offset);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedServerSentEvent(resultData, expectedCityList);
    }

    public static Stream<Arguments> getTestData_getTopicWith() {
        return TopicQuerySampleData.getTestData_getTopicWith_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_getTopicForClient() {
        return TopicQuerySampleData.getTestData_getTopicForClient_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_getTopicWithId() {
        return TopicQuerySampleData.getTestData_getTopicWithId_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_getTopicForClientWithId() {
        return TopicQuerySampleData.getTestData_getTopicForClientWithId_TopicQueryController();
    }
}
