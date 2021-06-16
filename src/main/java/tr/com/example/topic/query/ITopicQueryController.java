package tr.com.example.topic.query;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;

public interface ITopicQueryController {
    @GetMapping(
            value = "/{entity}",
            produces = MediaType.APPLICATION_STREAM_JSON_VALUE
    )
    Flux<?> getTopic(@PathVariable("entity") String entity,
                     @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset);

    @GetMapping(
            value = "/{entity}",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    Flux<ServerSentEvent<?>> getTopicForClient(@PathVariable("entity") String entity,
                                               @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset);

    @GetMapping(
            value = "/{entity}/{id}",
            produces = MediaType.APPLICATION_STREAM_JSON_VALUE
    )
    Flux<?> getTopic(@PathVariable("entity") String entity,
                     @PathVariable("id") Object id,
                     @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset);

    @GetMapping(
            value = "/{entity}/{id}",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    Flux<ServerSentEvent<?>> getTopicForClient(@PathVariable("entity") String entity,
                                               @PathVariable("id") Object id,
                                               @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset);
}
