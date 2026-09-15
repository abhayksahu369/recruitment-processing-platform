package com.example.recruitment.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the candidate-processing topic explicitly rather than relying
 * on Kafka's auto-topic-creation (which many real clusters disable, and
 * which would leave partition count to chance even when it's on). Spring
 * Boot's auto-configured KafkaAdmin picks up any NewTopic bean and creates
 * it on startup if it doesn't already exist.
 * <p>
 * 3 partitions: the ceiling on how many consumer instances in one group
 * can process this topic in parallel at once (see the kafka package's
 * Javadoc-level notes in Step 6's write-up for why). Chosen so Step 9's
 * benchmarks have real room to show 1 vs 3 consumers behaving differently
 * - easy to raise later, since partitions can only be increased, never
 * decreased, on an existing topic.
 * <p>
 * Replication factor 1: correct for a single local broker. A real
 * deployment would use at least 3, so losing one broker doesn't lose data.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic candidateProcessingTopic() {
        return new NewTopic(KafkaTopics.CANDIDATE_PROCESSING, 3, (short) 1);
    }
}
