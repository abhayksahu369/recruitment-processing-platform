package com.example.recruitment.kafka;

import com.example.recruitment.matching.CandidateMatcher;
import com.example.recruitment.matching.CandidateProfile;
import com.example.recruitment.repository.MatchResultRepository;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proves the "matching exception" failure mode from the spec: one
 * candidate's data is perfectly valid (it passed Excel validation, has a
 * real row in the database, and a real Kafka message), but the matcher
 * itself throws while scoring it - simulating a bug in the matching
 * algorithm, not bad input. @MockitoSpyBean wraps the real
 * SimpleCandidateMatcher bean; every candidate except the one poisoned
 * below is scored by the actual, unmodified algorithm - only this one
 * call is rigged to fail, so this is a real end-to-end run with exactly
 * one deliberately broken link, not a fully mocked pipeline.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class CandidateProcessingFailureTest {

    private static final String JOB_DESCRIPTION = """
            Java Backend Developer

            Skills:
            Java, Spring Boot, SQL, Kafka, Docker

            Experience:
            0-2
            """;

    // A sentinel that only the one candidate we want to poison uses -
    // every other row has ordinary in-range experience.
    private static final int POISONED_EXPERIENCE = 99;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MatchResultRepository matchResultRepository;

    @MockitoSpyBean
    private CandidateMatcher candidateMatcher;

    @Test
    void aMatchingExceptionOnOneCandidate_isRecordedAsFailed_andTheJobStillCompletes() throws Exception {
        doThrow(new RuntimeException("Simulated matching failure"))
                .when(candidateMatcher)
                .match(argThat((CandidateProfile p) -> p.experience() == POISONED_EXPERIENCE), any());

        // 5 candidates: C1-C4 are ordinary (2 matched, 2 not, by skill set),
        // C5 carries the poisoned experience value and will throw.
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                workbookWithOnePoisonedRow());

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        UUID jobId = UUID.fromString(objectMapper.readTree(createResponseJson).get("jobId").asText());

        // The job must still reach COMPLETED despite one candidate failing -
        // that's the whole point: one bad candidate doesn't strand the job.
        // @MockitoSpyBean gives this class its own Spring context (a
        // different bean definition than every other test class here), so
        // it can't reuse the context cache - when run after another Kafka
        // -heavy test class, this context's embedded broker starts while
        // the previous one is still tearing down, which is measurably
        // slower on Windows. 60s (matching CandidateProcessingConsumerTest)
        // gives that real startup cost room rather than tuning the test to
        // this machine's fastest possible run.
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                            .andExpect(status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    assertThat(objectMapper.readTree(statusJson).get("status").asText())
                            .isEqualTo("COMPLETED");
                });

        String statusJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode statusResponse = objectMapper.readTree(statusJson);

        assertThat(statusResponse.get("totalCandidates").asInt()).isEqualTo(5);
        assertThat(statusResponse.get("processed").asInt()).isEqualTo(5); // all 5 attempted, including the failure
        assertThat(statusResponse.get("failed").asInt()).isEqualTo(1);    // exactly the poisoned one
        assertThat(statusResponse.get("matched").asInt()).isEqualTo(2);   // C1-C4's real, unmocked split

        // The failed candidate never got a MatchResult row at all - there
        // was nothing valid to save.
        assertThat(matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId)).hasSize(4);
    }

    private static byte[] workbookWithOnePoisonedRow() throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Candidates");
            writeRow(sheet, 0, "candidate_id", "name", "email", "skills", "experience");
            writeRow(sheet, 1, "C1", "Rahul", "rahul@test.com", "Java, Spring Boot, SQL, Kafka, Docker", "1");
            writeRow(sheet, 2, "C2", "Amit", "amit@test.com", "Python, Django", "1");
            writeRow(sheet, 3, "C3", "Neha", "neha@test.com", "Java, Spring Boot, SQL, Kafka, Docker", "1");
            writeRow(sheet, 4, "C4", "Priya", "priya@test.com", "Python, Django", "1");
            writeRow(sheet, 5, "C5", "Vikram", "vikram@test.com", "Java, Spring Boot, SQL, Kafka, Docker",
                    String.valueOf(POISONED_EXPERIENCE));
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
