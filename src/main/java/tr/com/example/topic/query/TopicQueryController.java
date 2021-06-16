package tr.com.example.topic.query;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping(path = "/topic")
class TopicQueryController implements ITopicQueryController {

    private ITopicQueryService topicQueryService;

    public TopicQueryController(ITopicQueryService topicQueryService) {
        this.topicQueryService = topicQueryService;
    }

    @Override
    @GetMapping(
            value = "/{entity}",
            produces = MediaType.APPLICATION_STREAM_JSON_VALUE
    )

    public Flux<?> getTopic(@PathVariable("entity") String entity,
                            @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset) {
        return topicQueryService.getFlux(entity, offset);
    }

    @Override
    @GetMapping(
            value = "/{entity}",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<?>> getTopicForClient(@PathVariable("entity") String entity,
                                                      @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset) {
        return TopicQueryServiceUtil.mapper(
                () -> getTopic(entity, offset),
                entity
        );
    }

    @Override
    @GetMapping(
            value = "/{entity}/{id}",
            produces = MediaType.APPLICATION_STREAM_JSON_VALUE
    )
    public Flux<?> getTopic(@PathVariable("entity") String entity,
                            @PathVariable("id") Object id,
                            @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset) {
        return topicQueryService.getFlux(entity, offset, id);
    }

    @Override
    @GetMapping(
            value = "/{entity}/{id}",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<?>> getTopicForClient(@PathVariable("entity") String entity,
                                                      @PathVariable("id") Object id,
                                                      @RequestParam(value = "offset", required = false, defaultValue = "earliest") String offset) {
        return TopicQueryServiceUtil.mapper(
                () -> getTopic(entity, id, offset),
                entity
        );
    }

}
