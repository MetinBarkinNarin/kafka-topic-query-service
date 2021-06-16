package tr.com.example.topic.query;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by sotlu on 27.08.2019.
 */

@Data
@Getter
@Component
@ConfigurationProperties(prefix = "topic")
public class TopicNameProps {
    private final static String defaultGroupIdPrefix = "topic-query-group";
//    private final static String defaultNameFormat = "{0}-topic";
//    private final static String defaultNameWithNetworkIdFormat = "{0}-{1}-topic";

    private String groupIdPrefix = defaultGroupIdPrefix;
//    private String nameFormat = defaultNameFormat;
//    private String nameWithNetworkIdFormat = defaultNameWithNetworkIdFormat;
}
