package tr.com.example.topic.query;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

import java.util.function.Supplier;

public final class TopicQueryServiceUtil {
    static Flux<ServerSentEvent<?>> mapper(Supplier<Flux<?>> supplier, String entity) {
        return supplier.get()
                .map(value -> ServerSentEvent
                        .builder()
                        .event(entity)
                        .data(value)
                        .build());
    }

    static boolean compare(Object key, String otherKey) {
        if (key instanceof String)
            return key.equals(otherKey);
        else if (key instanceof Short)
            return key.equals(Short.valueOf(String.class.cast(otherKey)));
        else if (key instanceof Integer)
            return key.equals(Integer.valueOf(String.class.cast(otherKey)));
        else if (key instanceof Long)
            return key.equals(Long.valueOf(String.class.cast(otherKey)));
        throw new IllegalArgumentException("Unsupported ID Type! " +
                "Topic Id Type = [" + key.getClass() + "], " +
                "Query Id Type =[" + otherKey.getClass() + "]");
    }
}
