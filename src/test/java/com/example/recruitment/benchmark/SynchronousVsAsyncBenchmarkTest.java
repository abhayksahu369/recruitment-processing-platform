package com.example.recruitment.benchmark;

import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.kafka.KafkaTopics;
import com.example.recruitment.parser.ExcelParser;
import com.example.recruitment.parser.JobDescriptionParser;
import com.example.recruitment.repository.MatchResultRepository;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Answers a direct question: for the same 10,000 candidates, how does
 * Step 5's original synchronous approach (parse, then loop match+save,
 * all inside one request/transaction, no Kafka) compare to Steps 6-9's
 * real async Kafka pipeline (3 consumer threads)? Both phases run
 * back-to-back in the same JVM against the same generated dataset, so
 * the comparison isn't skewed by different machine load at different
 * times - only the approach itself differs.
 * <p>
 * Tagged "benchmark" for the same reason as the rest of this package:
 * run explicitly with `mvn test -DexcludedGroups= -Dgroups=benchmark
 * -Dtest=SynchronousVsAsyncBenchmarkTest`.
 */
@Tag("benchmark")
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class SynchronousVsAsyncBenchmarkTest {

    private static final int CANDIDATE_COUNT = 10_000;

    private static final String JOB_DESCRIPTION = """
            Java Backend Developer

            Skills:
            Java, Spring Boot, SQL, Kafka, Docker

            Experience:
            0-2
            """;

    @Autowired
    private JobDescriptionParser jobDescriptionParser;

    @Autowired
    private ExcelParser excelParser;

    @Autowired
    private SynchronousCandidateProcessor synchronousCandidateProcessor;

    @Autowired
    private MatchResultRepository matchResultRepository;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void comparesSynchronousProcessingAgainstTheAsyncKafkaPipeline() throws Exception {
        byte[] workbookBytes = SyntheticCandidateWorkbook.generate(CANDIDATE_COUNT);

        long syncElapsedMs = runSynchronously(workbookBytes);
        long asyncElapsedMs = runAsynchronously(workbookBytes);

        System.out.printf(
                "[SYNC vs ASYNC] %,d candidates: synchronous (no Kafka) = %,dms  |  "
                        + "async (Kafka, 3 consumer threads) = %,dms%n",
                CANDIDATE_COUNT, syncElapsedMs, asyncElapsedMs);
    }

    private long runSynchronously(byte[] workbookBytes) throws Exception {
        JobRequirements requirements = jobDescriptionParser.parse(JOB_DESCRIPTION);
        ExcelParseResult parseResult = excelParser.parse(new ByteArrayInputStream(workbookBytes));
        assertThat(parseResult.errors()).isEmpty();

        long start = System.currentTimeMillis();
        UUID jobId = synchronousCandidateProcessor.processSynchronously(requirements, parseResult);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId))
                .hasSize(CANDIDATE_COUNT);
        return elapsed;
    }

    private long runAsynchronously(byte[] workbookBytes) throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                workbookBytes);

        long start = System.currentTimeMillis();

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID jobId = UUID.fromString(objectMapper.readTree(createResponseJson).get("jobId").asText());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                            .andExpect(status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    assertThat(objectMapper.readTree(statusJson).get("status").asText())
                            .isEqualTo("COMPLETED");
                });

        long elapsed = System.currentTimeMillis() - start;

        assertThat(matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId))
                .hasSize(CANDIDATE_COUNT);
        return elapsed;
    }
}
