package tr.com.example.topic.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.params.provider.Arguments;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import tr.com.example.kafka.TopicRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

final class TopicQuerySampleData {
    private TopicQuerySampleData() {
    }

    static final String LATEST = "latest";
    static final String EARLIEST = "earliest";
    static final String DEFAULT_OFFSET = "earliest";
    static final String NO_RESULTS = "NO_RESULTS";
    static final String ALL_CITIES = "ALL_CITIES";
    static final String CITY_TOPIC = "city-topic";

    final static City ADANA = new City("01", "Adana");
    final static City AMASYA = new City("05", "Amasya");
    final static City ANKARA = new City("06", "Ankara");
    final static City ANTALYA = new City("07", "Antalya");
    final static City BOLU = new City("14", "Bolu");
    final static City BURSA = new City("16", "Bursa");
    final static City ERZURUM = new City("25", "Erzurum");
    final static City ESKISEHIR = new City("26", "Eskişehir");
    final static City ESKISEHIR_2 = new City("26", "Eskişehir-2");
    final static City ESKISEHIR_3 = new City("26", "Eskişehir-3");
    final static City ESKISEHIR_4 = new City("26", "Eskişehir-4");
    final static City ESKISEHIR_5 = new City("26", "Eskişehir-5");
    final static City KASTAMONU = new City("37", "Kastamonu");
    final static City MALATYA = new City("44", "Malatya");
    final static City NEVSEHIR = new City("50", "Nevşehir");
    final static City ORDU = new City("52", "Ordu");
    final static City SAMSUN = new City("55", "Samsun");
    final static City YOZGAT = new City("66", "Yozgat");
    final static City ZONGULDAK = new City("67", "Zonguldak");
    final static City RIZE = new City("54", "Rize");


    final static List<City> EMPTY_CITY_LIST;
    final static List<KeyValue<String, City>> EMPTY_KEY_VALUE_CITY_LIST;
    final static List<City> CITY_LIST;
    final static List<City> EARLIEST_CITY_LIST;
    final static List<City> LATEST_CITY_LIST;
    final static List<City> ESK_LIST;
    final static List<KeyValue<String, City>> KEY_VALUE_CITY_LIST;
    final static List<TopicRecord<?, ?>> EXPECTED_CITY_LIST;
    final static List<TopicRecord<?, ?>> EXPECTED_LATEST_CITY_LIST;
    final static List<TopicRecord<?, ?>> EXPECTED_EARLIEST_CITY_LIST;
    final static List<TopicRecord<?, ?>> ALL_TOPIC_RECORDS;
    final static List<TopicRecord<?, ?>> EXPECTED_EMPTY_LIST;
    final static List<ServerSentEvent<TopicRecord<?, ?>>> EXPECTED_EMPTY_SERVER_SENT_EVENT_LIST;
    final static List<TopicRecord<?, ?>> EXPECTED_ESK_LIST;
    final static List<TopicRecord<?, ?>> EXPECTED_ANKARA_LIST;
    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        EMPTY_CITY_LIST = Collections.emptyList();
        EMPTY_KEY_VALUE_CITY_LIST = Collections.emptyList();
        CITY_LIST = Arrays.asList(ADANA, AMASYA, ANKARA, ANTALYA, BOLU, BURSA, ERZURUM, ESKISEHIR,
                KASTAMONU, MALATYA, ORDU, RIZE, SAMSUN, YOZGAT, ZONGULDAK);
        EARLIEST_CITY_LIST = Arrays.asList(ADANA, AMASYA, ANKARA, ANTALYA, BOLU, BURSA, ERZURUM, ESKISEHIR,
                KASTAMONU, MALATYA, NEVSEHIR, ORDU, RIZE, SAMSUN, YOZGAT, ZONGULDAK);
        LATEST_CITY_LIST = Arrays.asList(NEVSEHIR);
        ESK_LIST = Arrays.asList(ESKISEHIR, ESKISEHIR_2, ESKISEHIR_3, ESKISEHIR_4, ESKISEHIR_5
        );
        KEY_VALUE_CITY_LIST = CITY_LIST.stream()
                .map(c -> new KeyValue<>(c.getId(), c))
                .collect(Collectors.toList());
        EXPECTED_CITY_LIST = toTopicRecord(CITY_LIST.stream());
        EXPECTED_EARLIEST_CITY_LIST = toTopicRecord(Stream.concat(
                CITY_LIST.stream(),
                LATEST_CITY_LIST.stream()
                )
        );
        EXPECTED_LATEST_CITY_LIST = toTopicRecord(LATEST_CITY_LIST.stream());
        EXPECTED_ESK_LIST = toTopicRecord(Stream.concat(
                ESK_LIST.stream(),
                Stream.of(ESKISEHIR)
                )
        );
        EXPECTED_ANKARA_LIST = toTopicRecord(CITY_LIST
                .stream()
                .filter(city -> Objects.equals(
                        city.getId(),
                        ANKARA.getId())
                )
        );
        EXPECTED_EMPTY_LIST = Collections.emptyList();
        ALL_TOPIC_RECORDS = EXPECTED_EARLIEST_CITY_LIST;
        EXPECTED_EMPTY_SERVER_SENT_EVENT_LIST = Collections.emptyList();
    }

    static Flux<ReceiverRecord<byte[], byte[]>> toReceiverRecord(String topicName, List<City> dataList) {
        return Flux.fromStream(
                dataList.stream()
                        .map(city -> {
                            try {
                                return new ReceiverRecord<>(
                                        new ConsumerRecord<>(
                                                topicName,
                                                1,
                                                0L,
                                                city.getId().getBytes(),
                                                new ObjectMapper().writeValueAsBytes(city)
                                        ),
                                        null);
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                            return null;
                        }));
    }

    public static List<TopicRecord<?, ?>> toTopicRecord(Stream<City> stream) {
        return stream
                .map(city -> new TopicRecord<>(
                                (Object) city.getId(),
                                mapToObject(city, Object.class)
                        )
                )
                .collect(Collectors.toList());
    }

    public static List<ServerSentEvent<?>> toServerEvent(List<?> list, String entity) {
        return Flux.fromStream(list.stream())
                .map(value -> ServerSentEvent
                        .builder()
                        .event(entity)
                        .data(value)
                        .build())
                .toStream()
                .collect(Collectors.toList());
    }

    public static Flux toTopicRecordFluxWithoutCasting(List<City> cityList) {
        return Flux.fromStream(cityList.stream()
                .map(city -> new TopicRecord<>(
                                (Object) city.getId(),
                                mapToObject(city, Object.class)
                        )
                )
        );
    }

    public static Flux<TopicRecord<?, ?>> toTopicRecordFlux(List<City> cityList) {
        return Flux.fromStream(cityList.stream()
                .map(city -> new TopicRecord<>(
                                (Object) city.getId(),
                                mapToObject(city, Object.class)
                        )
                )
        );
    }

    static List<TopicRecord<?, ?>> getTopicRecordsInResultString(List<TopicRecord<?, ?>> topicRecords, String result) {
        return topicRecords.stream()
                .filter(topicRecord -> result
                        .contains(Objects.requireNonNull(
                                mapToObject(
                                        topicRecord.getValue(),
                                        City.class)
                                ).getName()
                        )
                )
                .collect(Collectors.toList());
    }

    public static Stream<Arguments> getTestData_getTopicWith_TopicQueryController() {
        return Stream.of(
                arguments(
                        EARLIEST,
                        TopicQuerySampleData.EARLIEST_CITY_LIST,
                        EXPECTED_EARLIEST_CITY_LIST,
                        ALL_CITIES),
                arguments(
                        LATEST,
                        LATEST_CITY_LIST,
                        EXPECTED_LATEST_CITY_LIST,
                        NEVSEHIR.getName().toUpperCase())
        );
    }

    public static Stream<Arguments> getTestData_getFlux_TopicQueryService() {
        return getTestData_getTopicWith_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_receive_TopicQueryService() {
        return getTestData_getTopicWith_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_getTopicForClient_TopicQueryController() {
        return Stream.of(
                arguments(
                        EARLIEST,
                        EARLIEST_CITY_LIST,
                        toServerEvent(toTopicRecord(EARLIEST_CITY_LIST.stream()), CITY_TOPIC),
                        ALL_CITIES),
                arguments(
                        LATEST,
                        LATEST_CITY_LIST,
                        toServerEvent(toTopicRecord(LATEST_CITY_LIST.stream()), CITY_TOPIC),
                        NEVSEHIR.getName().toUpperCase())
        );
    }

    public static Stream<Arguments> getTestData_getTopicWithId_TopicQueryController() {
        return Stream.of(
                arguments(
                        EARLIEST,
                        "-1",
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        "100",
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        LATEST,
                        ESKISEHIR.getId(),
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        ESKISEHIR.getId(),
                        Collections.singletonList(ESKISEHIR),
                        toTopicRecord(Stream.of(ESKISEHIR)),
                        ESKISEHIR.getName().toUpperCase()),
                arguments(
                        LATEST,
                        NEVSEHIR.getId(),
                        Collections.singletonList(NEVSEHIR),
                        toTopicRecord(Stream.of(NEVSEHIR)),
                        NEVSEHIR.getName().toUpperCase()),
                arguments(
                        EARLIEST,
                        NEVSEHIR.getId(),
                        Collections.singletonList(NEVSEHIR),
                        toTopicRecord(Stream.of(NEVSEHIR)),
                        NEVSEHIR.getName().toUpperCase())
        );
    }

    public static Stream<Arguments> getTestData_getFluxWithId_TopicQueryService() {
        return getTestData_getTopicWithId_TopicQueryController();
    }

    public static Stream<Arguments> getTestData_getTopicForClientWithId_TopicQueryController() {
        return Stream.of(
                arguments(
                        EARLIEST,
                        "-1",
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_SERVER_SENT_EVENT_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        "100",
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_SERVER_SENT_EVENT_LIST,
                        NO_RESULTS),
                arguments(
                        LATEST,
                        ESKISEHIR.getId(),
                        EMPTY_CITY_LIST,
                        EXPECTED_EMPTY_SERVER_SENT_EVENT_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        ESKISEHIR.getId(),
                        Collections.singletonList(ESKISEHIR),
                        toServerEvent(toTopicRecord(Stream.of(ESKISEHIR)), CITY_TOPIC),
                        ESKISEHIR.getName().toUpperCase()),
                arguments(
                        LATEST,
                        NEVSEHIR.getId(),
                        Collections.singletonList(NEVSEHIR),
                        toServerEvent(toTopicRecord(Stream.of(NEVSEHIR)), CITY_TOPIC),
                        NEVSEHIR.getName().toUpperCase()),
                arguments(
                        EARLIEST,
                        NEVSEHIR.getId(),
                        Collections.singletonList(NEVSEHIR),
                        toServerEvent(toTopicRecord(Stream.of(NEVSEHIR)), CITY_TOPIC),
                        NEVSEHIR.getName().toUpperCase())
        );
    }

    public static Stream<Arguments> getTestData_receiveWithId_TopicQueryService() {
        return Stream.of(
                arguments(
                        EARLIEST,
                        "-1",
                        CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        "100",
                        CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        LATEST,
                        ESKISEHIR.getId(),
                        LATEST_CITY_LIST,
                        EXPECTED_EMPTY_LIST,
                        NO_RESULTS),
                arguments(
                        EARLIEST,
                        ESKISEHIR.getId(),
                        EARLIEST_CITY_LIST,
                        toTopicRecord(Stream.of(ESKISEHIR)),
                        ESKISEHIR.getName().toUpperCase()),
                arguments(
                        LATEST,
                        NEVSEHIR.getId(),
                        LATEST_CITY_LIST,
                        toTopicRecord(Stream.of(NEVSEHIR)),
                        NEVSEHIR.getName().toUpperCase()),
                arguments(
                        EARLIEST,
                        NEVSEHIR.getId(),
                        EARLIEST_CITY_LIST,
                        toTopicRecord(Stream.of(NEVSEHIR)),
                        NEVSEHIR.getName().toUpperCase())
        );
    }

    public static Stream<Arguments> getTestData_compare_TopicQueryServiceUtil() {
        return Stream.of(
                arguments(5, "5", true, "Integer"),
                arguments(5, "15", false, "Integer"),
                arguments(new Short("5"), "5", true, "Short"),
                arguments(new Short("5"), "15", false, "Short"),
                arguments("5", "5", true, "String"),
                arguments("5", "15", false, "String"),
                arguments(5L, "5", true, "Long"),
                arguments(5L, "15", false, "Long")
        );
    }

    enum SendingTime {
        BEFORE_REQUEST,
        AFTER_REQUEST
    }

    static class City {
        private String id;
        private String name;

        public City() {
        }

        public City(String id, String name) {
            Objects.requireNonNull(id);
            Objects.requireNonNull(name);
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "City{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof City)) return false;
            City city = (City) o;
            return Objects.equals(id, city.id) &&
                    Objects.equals(name, city.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    static <T> T mapToObject(Object obj, Class<T> toClass) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(obj), toClass);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    static City mapToCity(Object obj) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(obj), City.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }


    static int getExpectedDataCount(List<City> cityList, String testDataId) {
        return (int) cityList.stream().filter(city -> Objects.equals(city.getId(), testDataId)).count();
    }
}
