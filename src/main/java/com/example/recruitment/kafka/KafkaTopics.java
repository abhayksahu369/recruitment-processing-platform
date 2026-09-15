package com.example.recruitment.kafka;

/** Single source of truth for topic names, shared by the producer and (Step 7) the consumer. */
public final class KafkaTopics {

    public static final String CANDIDATE_PROCESSING = "candidate-processing";

    private KafkaTopics() {
    }
}
