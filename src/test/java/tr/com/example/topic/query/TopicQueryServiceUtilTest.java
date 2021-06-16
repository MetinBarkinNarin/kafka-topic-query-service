package tr.com.example.topic.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tr.com.example.topic.query.TopicQuerySampleData.*;

class TopicQueryServiceUtilTest {
    private final static String CITY_TOPIC = "city-topic";

    @Test
    void returnServerSentEventFluxWhenSampleFluxIsGiven() {
        // Given
        Flux<City> cityFlux = Flux.fromStream(CITY_LIST.stream());

        // When
        Flux<ServerSentEvent<?>> sentEventFlux = TopicQueryServiceUtil
                .mapper(
                        () -> cityFlux,
                        CITY_TOPIC
                );
        // Then
        FluxVerifierUtil
                .verifyFluxWithExpectedServerSentEvent(
                        sentEventFlux,
                        toServerEvent(
                                CITY_LIST,
                                CITY_TOPIC
                        )
                );
    }

    @ParameterizedTest(name = "#{index} - Return {2} => Given {3} source key is {0} AND other key is {1}")
    @MethodSource("getTestData_compare")
    void compareTest(
            Object key,
            String otherKey,
            boolean expectedResult,
            String sourceKeyType) { // tag for test name
        // When
        boolean result = TopicQueryServiceUtil
                .compare(key, otherKey);
        // Then
        assertThat(result)
                .isEqualTo(expectedResult);
    }

    @Test
    void compareTest_UnhandledType() {
        // When && Then
        assertThrows(IllegalArgumentException.class, () -> {
            TopicQueryServiceUtil
                    .compare(
                            true, // UnhandledType is boolean
                            ""
                    );
        });
    }

    private static Stream<Arguments> getTestData_compare() {
        return TopicQuerySampleData.getTestData_compare_TopicQueryServiceUtil();
    }
}
