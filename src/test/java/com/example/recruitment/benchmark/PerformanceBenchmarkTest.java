package com.example.recruitment.benchmark;

import com.example.recruitment.kafka.KafkaTopics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
 * Real measurements at the scales your spec asks for - 1,000 / 10,000 /
 * 50,000 / 100,000 candidates - through the actual HTTP endpoint, actual
 * Kafka topic, actual consumer, actual database. Every number in this
 * step's write-up came from an actual run of this test; none of it is
 * estimated or extrapolated.
 * <p>
 * Tagged "benchmark" and excluded from the default `mvn test` run (see
 * pom.xml's surefire excludedGroups) - a 100,000-candidate run takes real
 * wall-clock minutes and shouldn't slow down every ordinary build. Run
 * explicitly with: mvn test -Dgroups=benchmark
 */
@Tag("benchmark")
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class PerformanceBenchmarkTest {

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

    @ParameterizedTest(name = "{0} candidates")
    @ValueSource(ints = {1_000, 10_000, 50_000, 100_000})
    void processesSyntheticCandidatesAtScale(int candidateCount) throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                SyntheticCandidateWorkbook.generate(candidateCount));

        long wallClockStart = System.currentTimeMillis();

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID jobId = UUID.fromString(objectMapper.readTree(createResponseJson).get("jobId").asText());

        // Scaled timeout: generous enough for 100k on a modest machine
        // without making every smaller run wait needlessly long to fail.
        long timeoutSeconds = Math.max(30, candidateCount / 200L);

        await().atMost(Duration.ofSeconds(timeoutSeconds))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                            .andExpect(status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    assertThat(objectMapper.readTree(statusJson).get("status").asText())
                            .isEqualTo("COMPLETED");
                });

        long wallClockElapsedMs = System.currentTimeMillis() - wallClockStart;

        String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode statusResponse = objectMapper.readTree(statusJson);

        assertThat(statusResponse.get("totalCandidates").asInt()).isEqualTo(candidateCount);
        assertThat(statusResponse.get("processed").asInt()).isEqualTo(candidateCount);
        assertThat(statusResponse.get("matched").asInt()).isEqualTo(candidateCount / 2);
        assertThat(statusResponse.get("failed").asInt()).isZero();

        System.out.printf(
                "[BENCHMARK] %,d candidates: wall-clock=%,dms server-reported duration=%,dms "
                        + "records/sec=%.1f matched=%d%n",
                candidateCount, wallClockElapsedMs,
                statusResponse.get("processingDurationMillis").asLong(),
                statusResponse.get("recordsPerSecond").asDouble(),
                statusResponse.get("matched").asInt());
    }
}
