package com.example.recruitment.kafka;

import com.example.recruitment.domain.MatchResult;
import com.example.recruitment.repository.MatchResultRepository;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proves the async pipeline actually works at scale, not just for 3 hand
 * -written candidates: upload real spreadsheets of 10, 100, and 1000
 * candidates through the real HTTP endpoint, let the real
 * CandidateProcessingConsumer (running with concurrency = 3, genuinely
 * parallel) drain the real embedded Kafka topic, and check what actually
 * happened - not what should happen in theory.
 * <p>
 * The primary assertion waits on MatchResultRepository, not on the job's
 * status/counters. That's deliberate: a MatchResult row existing for every
 * candidate is a deterministic fact (the unique constraint + idempotency
 * check make it so), whereas ProcessingJob's processedCandidates counter
 * is updated with a plain read-modify-write under real concurrent access
 * (see CandidateProcessingConsumer's Javadoc) and is not guaranteed to be
 * race-free yet - that's Step 8's job. This test reports the counters it
 * actually observed rather than asserting a value that might legitimately
 * not hold today.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class CandidateProcessingConsumerTest {

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

    @Autowired
    private MatchResultRepository matchResultRepository;

    @ParameterizedTest(name = "{0} candidates")
    @ValueSource(ints = {10, 100, 1000})
    void everyValidCandidateEventuallyGetsExactlyOneMatchResult(int candidateCount) throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                workbookOf(candidateCount));

        long startedAt = System.currentTimeMillis();

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        UUID jobId = UUID.fromString(objectMapper.readTree(createResponseJson).get("jobId").asText());

        // The deterministic, race-free signal: wait until every candidate
        // has a MatchResult row, how ever long that genuinely takes.
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() ->
                        assertThat(matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId))
                                .hasSize(candidateCount));

        long elapsedMs = System.currentTimeMillis() - startedAt;

        List<MatchResult> results = matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId);
        long actuallyMatched = results.stream().filter(r -> r.getFinalScore() >= 0.5).count();

        String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode statusResponse = objectMapper.readTree(statusJson);

        // Reported, not asserted: whether processed/matched match
        // candidateCount exactly depends on whether the counter race
        // (documented in CandidateProcessingConsumer) actually manifested
        // during this run. Either way, every candidate WAS correctly
        // scored - see the assertion above.
        System.out.printf(
                "[%d candidates] elapsed=%dms job.status=%s job.processed=%d job.matched=%d "
                        + "actualMatchResultRows=%d actualMatchedByScore=%d%n",
                candidateCount, elapsedMs, statusResponse.get("status").asText(),
                statusResponse.get("processed").asInt(), statusResponse.get("matched").asInt(),
                results.size(), actuallyMatched);

        assertThat(results).hasSize(candidateCount);
    }

    /**
     * Alternates every other candidate between a skill set that fully
     * matches the job (skillScore 1.0) and one that shares nothing with it
     * (skillScore 0.0), with experience always inside the job's [0,2]
     * range - isolating skill overlap as the only thing driving whether a
     * candidate clears the 0.5 match threshold, so the ~50/50 matched
     * split is predictable without hand-listing every row.
     */
    private static byte[] workbookOf(int count) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Candidates");
            writeRow(sheet, 0, "candidate_id", "name", "email", "skills", "experience");
            for (int i = 1; i <= count; i++) {
                boolean fullMatch = i % 2 == 0;
                String skills = fullMatch ? "Java, Spring Boot, SQL, Kafka, Docker" : "Python, Django";
                writeRow(sheet, i,
                        "C" + i, "Candidate" + i, "candidate" + i + "@test.com", skills, String.valueOf(i % 3));
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            workbook.write(out);
            return out.toByteArray();
        }
    }

    private static void writeRow(Sheet sheet, int rowIndex, String... values) {
        Row row = sheet.createRow(rowIndex);
        for (int col = 0; col < values.length; col++) {
            row.createCell(col).setCellValue(values[col]);
        }
    }
}
