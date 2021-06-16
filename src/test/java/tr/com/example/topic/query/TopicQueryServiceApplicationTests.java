package tr.com.example.topic.query;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest
public class TopicQueryServiceApplicationTests {

    @Test
    public void contextLoads() {
        assertDoesNotThrow(() -> {
            TopicQueryServiceApplication.main(new String[]{});
        });
    }

}

