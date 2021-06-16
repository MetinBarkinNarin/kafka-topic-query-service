package tr.com.example.topic.query;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import tr.com.example.kafka.CustomKafkaReceiver;
import tr.com.example.kafka.TopicNotFoundException;
import tr.com.example.kafka.TopicQueryUtil;
import tr.com.example.kafka.TopicRecord;

@Component
@EnableAutoConfiguration
@Import(CustomKafkaReceiver.class)
class TopicQuery {
    private CustomKafkaReceiver customKafkaReceiver;

    private TopicNameProps topicNameProps;

    public TopicQuery(CustomKafkaReceiver customKafkaReceiver,
                      TopicNameProps topicNameProps) {
        this.customKafkaReceiver = customKafkaReceiver;
        this.topicNameProps = topicNameProps;
    }

    Flux<TopicRecord<?, ?>> receive(String topicName, String offset, Object id) {
        return receive(topicName, offset)
                .filter(p -> TopicQueryServiceUtil
                        .compare(
                                p.getKey(),
                                (String) id
                        )
                );
    }

    Flux<TopicRecord<?, ?>> receive(String topicName, String offset) {
        boolean topicExists = customKafkaReceiver.topicExists(topicName);
        if (!topicExists)
            throw new TopicNotFoundException(topicName);
        return customKafkaReceiver.receiveFromKafka(topicName, offset, topicNameProps.getGroupIdPrefix())
                .map(TopicQueryUtil::convert);
    }
}
