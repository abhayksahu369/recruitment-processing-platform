/**
 * Kafka transport: producers that publish CandidateProcessingEvent messages
 * and consumers that receive them. This package only moves work around —
 * it must call into the matching and repository packages rather than
 * containing business logic itself.
 */
package com.example.recruitment.kafka;
