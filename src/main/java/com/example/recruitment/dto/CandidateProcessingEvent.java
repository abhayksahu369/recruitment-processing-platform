package com.example.recruitment.dto;

import java.util.UUID;

/**
 * The Kafka message body published to the candidate-processing topic - one
 * per valid candidate. Carries only the two ids needed to look everything
 * else up, not the candidate's skills/name/email. That's deliberate:
 * <p>
 * - Message size: a job with 100,000 candidates means 100,000 messages;
 *   keeping each one to two UUIDs (32 bytes) instead of a full candidate
 *   record keeps the topic small regardless of how large a candidate's
 *   data gets.
 * - Coupling: if this event carried full candidate fields, every producer
 *   and consumer would need to agree on that exact shape forever - add a
 *   field to Candidate, and you're now versioning a wire format. An id is
 *   never wrong; the consumer re-reads whatever the Candidate row
 *   currently looks like at the moment it processes it.
 * - Schema evolution: the two ids will still mean the same thing after
 *   Candidate or ProcessingJob gain new columns in V2+. A payload shaped
 *   like today's entity would need a versioning strategy the moment either
 *   entity changes.
 * - Domain objects don't become Kafka messages automatically: this record
 *   exists specifically to be serialized onto the wire, independent of
 *   whatever the JPA entities look like internally.
 */
public record CandidateProcessingEvent(UUID processingJobId, UUID candidateId) {
}
