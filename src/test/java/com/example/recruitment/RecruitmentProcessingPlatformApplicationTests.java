package com.example.recruitment;

import com.example.recruitment.kafka.KafkaTopics;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

/**
 * Boots the full application context, exactly like main() would. Since
 * Step 6, that context includes Kafka autoconfiguration and our NewTopic
 * bean - without @EmbeddedKafka here, this test would have every test in
 * the suite's shared context quietly retrying a connection to a
 * non-existent localhost:9092 broker in the background.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class RecruitmentProcessingPlatformApplicationTests {

	@Test
	void contextLoads() {
	}

}
