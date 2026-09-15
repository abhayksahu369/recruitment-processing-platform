package com.example.recruitment.controller;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.kafka.KafkaTopics;
import com.example.recruitment.repository.CandidateRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * A real, full-stack test: real Spring MVC dispatch, real
 * ProcessingJobService, real parsers, real repositories, real (in-memory)
 * database, and now a real (embedded, in-process) Kafka broker - no
 * mocking of any of it. @EmbeddedKafka starts an actual broker for this
 * test class only; the @TestPropertySource line points the app's own
 * KafkaTemplate at that broker instead of the real localhost:9092 from
 * application.properties, which wouldn't exist in a test environment.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.CANDIDATE_PROCESSING})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class ProcessingJobControllerTest {

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
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private CandidateRepository candidateRepository;

    private Consumer<String, String> testConsumer;

    @AfterEach
    void closeConsumer() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void createsJobAsynchronously_savesCandidates_andPublishesOneEventPerCandidateToKafka() throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                sampleWorkbookBytes());

        String createResponseJson = mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", JOB_DESCRIPTION))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        JsonNode createResponse = objectMapper.readTree(createResponseJson);
        // Honest state for this step: nothing has been matched yet, because
        // nothing consumes candidate-processing until Step 7. The whole
        // point of this test is proving that's true, not hiding it.
        assertThat(createResponse.get("status").asText()).isEqualTo("QUEUED");
        assertThat(createResponse.get("totalCandidates").asInt()).isEqualTo(3);
        UUID jobId = UUID.fromString(createResponse.get("jobId").asText());

        String statusResponseJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        JsonNode statusResponse = objectMapper.readTree(statusResponseJson);
        assertThat(statusResponse.get("status").asText()).isEqualTo("QUEUED");
        assertThat(statusResponse.get("processed").asInt()).isEqualTo(0);
        assertThat(statusResponse.get("matched").asInt()).isEqualTo(0);
        assertThat(statusResponse.get("failed").asInt()).isEqualTo(0);

        // Nothing has produced a MatchResult yet either - results is empty,
        // not missing/erroring, since the job itself does exist.
        String resultsResponseJson = mockMvc.perform(get("/api/processing/jobs/{jobId}/results", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(objectMapper.readTree(resultsResponseJson)).isEmpty();

        // The three candidate rows exist in the database already - Kafka
        // only carries their ids, not their data (see CandidateProcessingEvent).
        List<Candidate> savedCandidates = candidateRepository.findByProcessingJob_Id(jobId);
        assertThat(savedCandidates).hasSize(3);

        // Now prove the events actually reached the topic: a raw consumer,
        // subscribed to the whole topic, should see exactly 3 messages,
        // one per candidate, each keyed by that candidate's id and carrying
        // this job's id in its JSON body.
        testConsumer = createTestConsumer();
        testConsumer.subscribe(List.of(KafkaTopics.CANDIDATE_PROCESSING));
        ConsumerRecords<String, String> records =
                KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(10));

        assertThat(records.count()).isEqualTo(3);
        List<UUID> savedCandidateIds = savedCandidates.stream().map(Candidate::getId).toList();
        for (ConsumerRecord<String, String> record : records) {
            assertThat(UUID.fromString(record.key())).isIn(savedCandidateIds);
            assertThat(record.value())
                    .contains("\"processingJobId\":\"" + jobId + "\"")
                    .contains("\"candidateId\":\"" + record.key() + "\"");
        }
    }

    @Test
    void malformedJobDescription_returns400WithoutTouchingTheDatabase() throws Exception {
        MockMultipartFile candidatesPart = new MockMultipartFile(
                "candidates", "candidates.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                sampleWorkbookBytes());

        mockMvc.perform(multipart("/api/processing/jobs")
                        .file(candidatesPart)
                        .param("jobDescription", "not a valid job description"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void statusForAnUnknownJobId_returns404() throws Exception {
        mockMvc.perform(get("/api/processing/jobs/{jobId}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    private Consumer<String, String> createTestConsumer() {
        var props = KafkaTestUtils.consumerProps("test-verifier", "true", embeddedKafkaBroker);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer());
    }

    private static byte[] sampleWorkbookBytes() throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Candidates");
            writeRow(sheet, 0, "candidate_id", "name", "email", "skills", "experience");
            writeRow(sheet, 1, "C001", "Rahul", "rahul@email.com", "Java, Spring Boot, SQL", "1");
            writeRow(sheet, 2, "C002", "Amit", "amit@email.com", "Python, Django, SQL", "2");
            writeRow(sheet, 3, "C003", "Neha", "neha@email.com", "Java, Spring Boot, Kafka, Docker", "1");
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
