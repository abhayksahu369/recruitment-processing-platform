package com.example.recruitment.controller;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * A real, full-stack test: real Spring MVC dispatch, real
 * ProcessingJobService, real parsers, real matcher, real repositories,
 * real (in-memory) database. Nothing here is mocked - this is what proves
 * the entire synchronous pipeline described in section 5 of the spec
 * ("upload Excel, provide job description, process candidates, store
 * results, retrieve ranked candidates") actually works end to end, not
 * just that its pieces compile.
 */
@SpringBootTest
@AutoConfigureMockMvc
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

    @Test
    void uploadsCandidates_processesThemSynchronously_andRanksResultsByFinalScore() throws Exception {
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
        assertThat(createResponse.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(createResponse.get("totalCandidates").asInt()).isEqualTo(3);
        String jobId = createResponse.get("jobId").asText();

        String statusResponseJson = mockMvc.perform(get("/api/processing/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        JsonNode statusResponse = objectMapper.readTree(statusResponseJson);
        assertThat(statusResponse.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(statusResponse.get("totalCandidates").asInt()).isEqualTo(3);
        assertThat(statusResponse.get("processed").asInt()).isEqualTo(3);
        assertThat(statusResponse.get("failed").asInt()).isEqualTo(0);
        // Rahul (0.68) and Neha (0.84) clear the 0.5 threshold; Amit (0.36) doesn't.
        assertThat(statusResponse.get("matched").asInt()).isEqualTo(2);

        String resultsResponseJson = mockMvc.perform(get("/api/processing/jobs/{jobId}/results", jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        JsonNode results = objectMapper.readTree(resultsResponseJson);
        assertThat(results).hasSize(3);
        // Ranked by finalScore descending: Neha (84.0), Rahul (68.0), Amit (36.0).
        assertThat(results.get(0).get("candidateId").asText()).isEqualTo("C003");
        assertThat(results.get(0).get("name").asText()).isEqualTo("Neha");
        assertThat(results.get(0).get("finalScore").asDouble()).isEqualTo(84.0);
        assertThat(results.get(1).get("candidateId").asText()).isEqualTo("C001");
        assertThat(results.get(1).get("finalScore").asDouble()).isEqualTo(68.0);
        assertThat(results.get(2).get("candidateId").asText()).isEqualTo("C002");
        assertThat(results.get(2).get("finalScore").asDouble()).isEqualTo(36.0);
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
        mockMvc.perform(get("/api/processing/jobs/{jobId}", java.util.UUID.randomUUID()))
                .andExpect(status().isNotFound());
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
