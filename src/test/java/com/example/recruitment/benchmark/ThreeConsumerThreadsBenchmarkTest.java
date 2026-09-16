package com.example.recruitment.benchmark;

import com.example.recruitment.kafka.KafkaTopics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The concurrency=3 half of the comparison - see
 * {@link SingleConsumerThreadBenchmarkTest}'s Javadoc. Identical workload
 * (same candidate count, same synthetic data, same 3-partition topic),
 * only the consumer thread count differs. Two separate top-level classes
 * rather than one parameterized test because each needs its own
 * @TestPropertySource-driven Spring context - see pom.xml's Surefire
 * fork-per-class note (Step 8) for why running unrelated embedded-Kafka
 * contexts back-to-back in one JVM is worth avoiding altogether.
 */
@Tag("benchmark")
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "recruitment.kafka.consumer-concurrency=3"
})
class ThreeConsumerThreadsBenchmarkTest {

    private static final int CANDIDATE_COUNT = 20_000;

    private static final String JOB_DESCRIPTION = """
            Java Backend Developer

            Skills:
            Java, Spring Boot, SQL, Kafka, Docker

            Experience:
            0-2
            """;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void measuresThroughputWithThreeConsumerThreads() throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                SyntheticCandidateWorkbook.generate(CANDIDATE_COUNT));

        long startedAt = System.currentTimeMillis();

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID jobId = UUID.fromString(objectMapper.readTree(createResponseJson).get("jobId").asText());

        await().atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                            .andExpect(status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    assertThat(objectMapper.readTree(statusJson).get("status").asText())
                            .isEqualTo("COMPLETED");
                });

        long elapsedMs = System.currentTimeMillis() - startedAt;
        String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode statusResponse = objectMapper.readTree(statusJson);

        assertThat(statusResponse.get("processed").asInt()).isEqualTo(CANDIDATE_COUNT);

        System.out.printf(
                "[BENCHMARK concurrency=3] %,d candidates: wall-clock=%,dms records/sec=%.1f%n",
                CANDIDATE_COUNT, elapsedMs, statusResponse.get("recordsPerSecond").asDouble());
    }
}
