package com.example.recruitment.kafka;

import com.example.recruitment.dto.CandidateProcessingEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * The only class in this codebase that publishes to Kafka. Everything
 * upstream (ProcessingJobService) depends on this class's method, not on
 * KafkaTemplate directly - if V2 swaps Kafka for something else, this is
 * the one seam that changes.
 * <p>
 * publish() is called from inside ProcessingJobService's @Transactional
 * method, before that transaction has committed the Candidate/ProcessingJob
 * rows it just created. Sending immediately would let a fast consumer
 * receive the message and query for those rows before they're durably
 * visible - which is exactly what happened during real testing at 1000
 * candidates (see this step's write-up): the consumer's findById() came up
 * empty, threw, and after Spring Kafka's retries were exhausted, that
 * message was permanently skipped. Deferring the actual send until
 * afterCommit() closes that window entirely: the message can only reach a
 * consumer once the data it references is guaranteed to already be there.
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
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(event);
                }
            });
        } else {
            // No transaction active (e.g. called from a test or a future
            // caller outside a @Transactional method) - nothing to wait
            // for, so send immediately.
            send(event);
        }
    }

    private void send(CandidateProcessingEvent event) {
        kafkaTemplate.send(KafkaTopics.CANDIDATE_PROCESSING, event.candidateId().toString(), event);
    }
}
