package tr.com.example.topic.query;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = {
        "classpath:features/TopicQueryService_HappyPath.feature",
        "classpath:features/TopicQueryService_AngryPath.feature"
})
public class TopicQueryServiceComponentTest {

}
