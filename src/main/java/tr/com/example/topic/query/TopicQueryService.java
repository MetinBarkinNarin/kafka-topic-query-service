package tr.com.example.topic.query;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
class TopicQueryService implements ITopicQueryService {
    private TopicQuery topicQuery;

    TopicQueryService(TopicQuery topicQuery) {
        this.topicQuery = topicQuery;
    }

    @Override
    public Flux<?> getFlux(String entity, String offset) {
        String topicName = getTopicName(entity);
        return topicQuery.receive(topicName, offset);
    }

    @Override
    public Flux<?> getFlux(String entity, String offset, Object id) {
        String topicName = getTopicName(entity);
        return topicQuery.receive(topicName, offset, id);
    }

    private String getTopicName(String entity) {
        return entity;
    }
}
