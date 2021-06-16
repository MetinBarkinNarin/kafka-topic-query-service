package tr.com.example.topic.query;

import reactor.core.publisher.Flux;

public interface ITopicQueryService {
    Flux<?> getFlux(String entity, String offset);

    Flux<?> getFlux(String entity, String offset, Object id);
}
