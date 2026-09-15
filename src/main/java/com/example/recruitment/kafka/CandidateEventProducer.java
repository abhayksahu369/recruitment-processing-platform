package com.example.recruitment.kafka;

import com.example.recruitment.dto.CandidateProcessingEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * The only class in this codebase that publishes to Kafka. Everything
 * upstream (ProcessingJobService) depends on this class's method, not on
 * KafkaTemplate directly - if V2 swaps Kafka for something else, this is
 * the one seam that changes.
 */
@Component
public class CandidateEventProducer {

    // Spring Boot auto-configures exactly one KafkaTemplate bean, typed
    // KafkaTemplate<String, Object> - Java generics aren't covariant, so
    // asking for KafkaTemplate<String, CandidateProcessingEvent> here
    // instead would fail to match that bean at all ("no qualifying bean"),
    // even though it's the only KafkaTemplate in the whole application.
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CandidateEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Keyed by candidateId, not processingJobId. Kafka routes messages with
     * the same key to the same partition, and a partition is only ever
     * consumed by one consumer in a group at a time - so keying by
     * processingJobId would force every candidate in a single job onto one
     * partition, serializing that entire job onto one consumer and
     * defeating the reason Kafka is here at all. candidateId spreads a
     * job's candidates across all partitions, which is what actually lets
     * multiple consumers work on the same job in parallel.
     */
    public void publish(CandidateProcessingEvent event) {
        kafkaTemplate.send(KafkaTopics.CANDIDATE_PROCESSING, event.candidateId().toString(), event);
    }
}
