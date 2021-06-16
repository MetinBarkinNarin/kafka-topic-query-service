package tr.com.example.topic.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import tr.com.example.kafka.TopicRecord;

import java.util.List;
import java.util.stream.Stream;

import static org.mockito.BDDMockito.given;
import static tr.com.example.topic.query.TopicQuerySampleData.City;
import static tr.com.example.topic.query.TopicQuerySampleData.toTopicRecordFlux;

@ExtendWith(MockitoExtension.class)
class TopicQueryServiceTest {
    static final String CITY_TOPIC = "city-topic";

    @Mock(lenient = true)
    private TopicQuery topicQuery;
    private TopicQueryService topicQueryService;

    @BeforeEach
    void setUp() {
        topicQueryService = new TopicQueryService(topicQuery);
    }

    @AfterEach
    void tearDown() {
    }

    @ParameterizedTest(name = "#{index} - Return {3} => Given offset is {0}")
    @MethodSource("getTestData_getFlux")
    void returnTopicRecordsByOffset(String offset,
                                    List<City> mockingData,
                                    List<TopicRecord<?, ?>> expectedCityList,
                                    String resultTag) {//Tag for test name
        // Given
        given(topicQuery.receive(CITY_TOPIC, offset))
                .willReturn(toTopicRecordFlux(mockingData));

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQueryService
                .getFlux(CITY_TOPIC, offset)
                .map(o -> (TopicRecord<?, ?>) o);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    @ParameterizedTest(name = "#{index} - Return {4} => Given Offset is {0} AND Id is {1}")
    @MethodSource("getTestData_getFluxWithId")
    void returnTopicRecordsByOffsetWithId(String offset,
                                          String id,
                                          List<City> mockingData,
                                          List<TopicRecord<?, ?>> expectedCityList,
                                          String resultTag) {//Tag for test name
        // Given
        given(topicQuery.receive(CITY_TOPIC, offset, id))
                .willReturn(toTopicRecordFlux(mockingData));

        // When
        Flux<TopicRecord<?, ?>> resultData = topicQueryService
                .getFlux(CITY_TOPIC, offset, id)
                .map(o -> (TopicRecord<?, ?>) o);

        // Then
        FluxVerifierUtil.verifyFluxWithExpectedTopicRecords(resultData, expectedCityList);
    }

    public static Stream<Arguments> getTestData_getFlux() {
        return TopicQuerySampleData.getTestData_getFlux_TopicQueryService();
    }

    public static Stream<Arguments> getTestData_getFluxWithId() {
        return TopicQuerySampleData.getTestData_getFluxWithId_TopicQueryService();
    }
}

