package tr.com.example.topic.query;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tr.com.example.kafka.TopicRecord;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxVerifierUtil {
    public static void verifyFluxWithExpectedTopicRecords(
            Flux<TopicRecord<?, ?>> resultFlux,
            List<TopicRecord<?, ?>> expectedList) {

        StepVerifier.create(resultFlux)
                .recordWith(ArrayList::new)
                .expectNextCount(expectedList.size())
                .consumeRecordedWith(results -> {
                    assertThat(new ArrayList<>(results))
                            .isNotNull()
                            .usingRecursiveFieldByFieldElementComparator()
                            .containsExactlyInAnyOrderElementsOf(expectedList);
                })
                .thenCancel()
                .verify();
    }

    public static void verifyFluxWithExpectedServerSentEvent(
            Flux<ServerSentEvent<?>> resultFlux,
            List<ServerSentEvent<?>> expectedList) {

        StepVerifier.create(resultFlux)
                .recordWith(ArrayList::new)
                .expectNextCount(expectedList.size())
                .consumeRecordedWith(results -> {
                    assertThat(new ArrayList<>(results))
                            .isNotNull()
                            .usingRecursiveFieldByFieldElementComparator()
                            .containsExactlyInAnyOrderElementsOf(expectedList);
                })
                .thenCancel()
                .verify();
    }
}
