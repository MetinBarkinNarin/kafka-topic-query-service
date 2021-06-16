package tr.com.example.topic.query;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;
import tr.com.example.kafka.CustomKafkaReceiver;
import tr.com.example.kafka.CustomProducerConfigBuilder;
import tr.com.example.kafka.TopicRecord;
import tr.com.example.kafka.TopicSenderUtil;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static tr.com.example.topic.query.TopicQuerySampleData.*;

@WebFluxTest(
        properties = {
                "server.port=0",
                "spring.jmx.enabled=false",
                "spring.kafka.consumer.bootstrap-servers=127.0.0.1:9092"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                TopicQueryServiceIntegrationTest.TEST_TOPIC
        },
        brokerProperties = {
                "listeners=PLAINTEXT://127.0.0.1:9092",
                "port=9092"
        }
)
@Import(value = {KafkaAutoConfiguration.class, TopicQueryService.class, TopicQuery.class, TopicNameProps.class, CustomKafkaReceiver.class})
public class TopicQueryServiceIntegrationTest {
    static final String TEST_TOPIC = "test-topic";
    static final String TEST_DATA_ID = ANKARA.getId();
    static final String UNKNOWN_DATA_ID = "100";
    static final String TEST_URI_EARLIEST_QUERY = "offset=earliest";
    static final String TEST_URI_LATEST_QUERY = "offset=latest";
    static final String TEST_URI_FORMAT = "/topic/{0}?{1}";
    static final String TEST_URI_WITH_ID_FORMAT = "/topic/{0}/{1}?{2}";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaSender<String, City> kafkaSender;

    @Autowired
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = createProducerProps();
        kafkaSender = TopicSenderUtil.reactiveKafkaSender(producerProps);
    }

    @AfterEach
    void tearDown() {
        kafkaSender.close();
//        client.delete();
    }

    @Test
    void thrownExceptionWhenTopicIsNull() {
        // When && Then
        client.get()
                .uri(MessageFormat.format(
                        TEST_URI_FORMAT,
                        "",
                        TEST_URI_EARLIEST_QUERY))
                .exchange()
                .expectStatus().isNotFound();
    }

//    @Test
//    void returnedNoResultsWhenIdIsNull(){
//        // When && Then
//        client.get()
//                .uri(MessageFormat.format(
//                        TEST_URI_WITH_ID_FORMAT,
//                        TEST_TOPIC,
//                        "",
//                        TEST_URI_EARLIEST_QUERY))
//                .exchange()
//                .expectStatus().isNotFound();
//    }

//    @Test//TODO no result when unkonown id tekrar bakılacak
//    void returnedNoResultsWhenIdIsUnknownDataId() {
//
//
//        // Given
//        Flux<City> cityFlux = Flux.fromStream(CITY_LIST.stream());
//
//        // When
//        send(TEST_TOPIC, cityFlux);
//
//        List<TopicRecord> responseBody = client.get()
//                .uri(MessageFormat.format(
//                        TEST_URI_WITH_ID_FORMAT,
//                        TEST_TOPIC,
//                        UNKNOWN_DATA_ID,
//                        TEST_URI_EARLIEST_QUERY))
//                .exchange()
//                .returnResult(TopicRecord.class)
//                .getResponseBody()
//                .buffer(0)
//                .blockFirst();
//
//
//        System.out.println("123");

//        StepVerifier.create(responseBody)
//                .expectComplete();

//        StepVerifier.
//                withVirtualTime(() -> responseBody)
//                .expectSubscription()
//                .expectNoEvent(Duration.ofSeconds(1))
//                .expectNextCount(0)
//                .expectComplete();


        // Then
//        StepVerifier.create(responseBody)
//                .recordWith(ArrayList::new)
//                .expectNextCount(0)
//                .consumeRecordedWith(results -> {
//                    assertThat(new ArrayList<>(results))
//                            .isNotNull()
//                            .usingRecursiveFieldByFieldElementComparator()
//                            .containsExactlyInAnyOrderElementsOf(EXPECTED_EMPTY_LIST);
//                })
//                .thenCancel()
//                .verify();
//    }


//        StepVerifier.create(recordFlux)
//                .recordWith(ArrayList::new)
//                .expectNextCount(0)
//                .consumeRecordedWith(results -> {
//                    assertThat(new ArrayList<>(results))
//                            .isNotNull()
//                            .usingRecursiveFieldByFieldElementComparator()
//                            .containsExactlyInAnyOrderElementsOf(EXPECTED_EMPTY_LIST);
//                })
//                .thenCancel()
//                .verify();


    @Test
    void returnAllResultsWhenOffsetIsEarliest() {
        // Given
        Flux<City> cityFlux = Flux.fromStream(CITY_LIST.stream());
        Flux<City> latestCityFlux = Flux.fromStream(LATEST_CITY_LIST.stream());

        // When
        send(TEST_TOPIC, cityFlux);

        Flux<TopicRecord> responseBody = client.get()
                .uri(MessageFormat.format(
                        TEST_URI_FORMAT,
                        TEST_TOPIC,
                        TEST_URI_EARLIEST_QUERY))
                .exchange()
                .returnResult(TopicRecord.class)
                .getResponseBody();


        send(TEST_TOPIC, latestCityFlux);
        // Then
        StepVerifier.create(responseBody)
                .recordWith(ArrayList::new)
                .expectNextCount(EXPECTED_EARLIEST_CITY_LIST.size())
                .consumeRecordedWith(results -> {
                    assertThat(new ArrayList<>(results))
                            .isNotNull()
                            .usingRecursiveFieldByFieldElementComparator()
                            .containsExactlyInAnyOrderElementsOf(EXPECTED_EARLIEST_CITY_LIST);
                })
                .thenCancel()
                .verify();
    }
    @Test
    void returnAllResultsWhenOffsetIsEarliestWithId() {
        // Given
        Flux<City> cityFlux = Flux.fromStream(CITY_LIST.stream());
        Flux<City> latestCityFlux = Flux.fromStream(LATEST_CITY_LIST.stream());

        // When
        send(TEST_TOPIC, cityFlux);

        Flux<TopicRecord> responseBody = client.get()
                .uri(MessageFormat.format(
                        TEST_URI_WITH_ID_FORMAT,
                        TEST_TOPIC,
                        TEST_DATA_ID,
                        TEST_URI_EARLIEST_QUERY))
                .exchange()
                .returnResult(TopicRecord.class)
                .getResponseBody();


        send(TEST_TOPIC, latestCityFlux);
        // Then
        StepVerifier.create(responseBody)
                .recordWith(ArrayList::new)
                .expectNextCount(EXPECTED_ANKARA_LIST.size())
                .consumeRecordedWith(results -> {
                    assertThat(new ArrayList<>(results))
                            .isNotNull()
                            .usingRecursiveFieldByFieldElementComparator()
                            .containsExactlyInAnyOrderElementsOf(EXPECTED_ANKARA_LIST);
                })
                .thenCancel()
                .verify();
    }

//    @Test//TODO latest için tekrar bakılacak
//    void returnAllResultsWhenOffsetIsLatest() throws ExecutionException, InterruptedException {
//        // Given
//        Flux<City> cityFlux = Flux.fromStream(CITY_LIST.stream());
//        Flux<City> latestCityFlux = Flux.fromStream(LATEST_CITY_LIST.stream());
//
//        // When
//        send(TEST_TOPIC, cityFlux);
//
//        CompletableFuture<List<TopicRecord>> listCompletableFuture = CompletableFuture.supplyAsync(() -> client.get()
//                .uri(MessageFormat.format(
//                        TEST_URI_FORMAT,
//                        TEST_TOPIC,
//                        TEST_URI_LATEST_QUERY))
//                .exchange()
//                .expectStatus().isOk()
//                .returnResult(TopicRecord.class)
//                .getResponseBody()
//                .buffer(LATEST_CITY_LIST.size())
//                .blockFirst()
//        );
//
//        send(TEST_TOPIC, latestCityFlux);

        //Then
//        StepVerifier.create(responseBody)
//                .recordWith(ArrayList::new)
//                .expectNextCount(EXPECTED_LATEST_CITY_LIST.size())
//                .consumeRecordedWith(results -> {
//        assertThat(listCompletableFuture.get())
//                            .isNotNull()
//                            .usingRecursiveFieldByFieldElementComparator()
//                            .containsExactlyInAnyOrderElementsOf(EXPECTED_LATEST_CITY_LIST);
//                })
//                .thenCancel()
//                .verify();
//    }

//    @Test//TODO değişti
//    void getFluxWithId() throws ExecutionException, InterruptedException {
//        // Given
//        Flux<City> flux = Flux.fromStream(ESK_LIST.stream());
//        send(TEST_TOPIC, flux);
//
//        // When
//        CompletableFuture<List<TopicRecord>> listCompletableFuture = CompletableFuture.supplyAsync(() ->
//                client.get()
//                        .uri(MessageFormat.format(
//                                TEST_URI_WITH_ID_FORMAT,
//                                TEST_TOPIC,
//                                TEST_DATA_ID,
//                                TEST_URI_EARLIEST_QUERY))
//                        .exchange()
//                        .expectStatus().isOk()
//                        .returnResult(TopicRecord.class)
//                        .getResponseBody()
//                        .buffer(ESK_LIST.size() + 1)
//                        .blockFirst());
//        send(TEST_TOPIC, Flux.fromStream(Stream.of(ESKISEHIR)));
//        List<TopicRecord> topicRecords = listCompletableFuture.get();
//        // Then
//        assertThat(topicRecords)
//                .isNotNull()
//                .usingRecursiveFieldByFieldElementComparator()
//                .containsExactlyInAnyOrderElementsOf(EXPECTED_ESK_LIST);
//    }

//    @Test//TODO değişti
//    void getFluxWithId2() {
//        // Given
//        Flux<City> flux = Flux.fromStream(CITY_LIST.stream());
//        send(TEST_TOPIC, flux);
//
//        // When
//        List<TopicRecord> topicRecords = client
//                .get()
//                .uri(MessageFormat.format(
//                        TEST_URI_WITH_ID_FORMAT,
//                        TEST_TOPIC,
//                        TEST_DATA_ID,
//                        TEST_URI_EARLIEST_QUERY)
//                )
//                .exchange()
//                .expectStatus().isOk()
//                .returnResult(TopicRecord.class)
//                .getResponseBody()
//                .buffer(getExpectedDataCount(CITY_LIST, TEST_DATA_ID))
//                .blockFirst();
//
//        // Then
//        assertThat(topicRecords)
//                .isNotNull()
//                .usingRecursiveFieldByFieldElementComparator()
//                .containsExactlyInAnyOrderElementsOf(EXPECTED_ANKARA_LIST);
//    }

    void send(String topicName, Flux<City> inputFlux) {
        kafkaSender
                .send(inputFlux.map(r -> SenderRecord.create(
                        new ProducerRecord<>(
                                topicName,
                                r.getId(),
                                r),
                        r)))
                .subscribe();
    }

    private Map<String, Object> createProducerProps() {
        return new CustomProducerConfigBuilder()
                .bootstrapServers(embeddedKafka.getBrokersAsString())
                .keySerializer(StringSerializer.class)
                .valueSerializer(JsonSerializer.class)
                .build();
    }
}
