package com.example.recruitment.benchmark;

import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.kafka.KafkaTopics;
import com.example.recruitment.matching.CandidateMatcher;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests a specific hypothesis: SynchronousVsAsyncBenchmarkTest found sync
 * beating async because matching itself is near-free (microseconds) -
 * parallelizing near-zero work parallelizes nothing, while async's extra
 * per-candidate database round-trips are pure overhead on top. If
 * matching became genuinely expensive, the parallelizable work should
 * start to dominate, and 3 consumer threads should start winning.
 * <p>
 * @MockitoSpyBean wraps the REAL CandidateMatcher bean and adds a fixed
 * artificial delay before delegating to the real match() call - the
 * scores it returns are still genuine, correct results (doAnswer calls
 * through via invocation.callRealMethod()), only the timing is
 * artificial. This is a test-only injection, exactly like
 * CandidateProcessingFailureTest's approach in Step 8 - nothing in
 * src/main changes. Because it's the same singleton bean, the delay
 * applies uniformly to both the synchronous helper and the real
 * asynchronous consumer, so the comparison stays fair.
 */
@Tag("benchmark")
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class SlowMatchingCrossoverBenchmarkTest {

    private static final int CANDIDATE_COUNT = 3_000;
    private static final long SIMULATED_MATCHING_DELAY_MS = 10;

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

    @MockitoSpyBean
    private CandidateMatcher candidateMatcher;

    @Test
    void findsWhereParallelismStartsToWin() throws Exception {
        doAnswer(invocation -> {
            Thread.sleep(SIMULATED_MATCHING_DELAY_MS);
            return invocation.callRealMethod();
        }).when(candidateMatcher).match(any(), any());

        byte[] workbookBytes = SyntheticCandidateWorkbook.generate(CANDIDATE_COUNT);

        long syncElapsedMs = runSynchronously(workbookBytes);
        long asyncElapsedMs = runAsynchronously(workbookBytes);

        System.out.printf(
                "[SLOW MATCHING, %dms/candidate] %,d candidates: synchronous = %,dms  |  "
                        + "async (3 threads) = %,dms  |  async is %.2fx %s%n",
                SIMULATED_MATCHING_DELAY_MS, CANDIDATE_COUNT, syncElapsedMs, asyncElapsedMs,
                (double) Math.max(syncElapsedMs, asyncElapsedMs) / Math.min(syncElapsedMs, asyncElapsedMs),
                asyncElapsedMs < syncElapsedMs ? "FASTER than sync" : "SLOWER than sync");
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

        await().atMost(Duration.ofSeconds(120))
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
