package com.example.recruitment.parser;

import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.ParsedCandidate;
import com.example.recruitment.exception.MalformedExcelException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Builds workbooks with POI directly in each test rather than committing
 * binary .xlsx fixture files - every row under test is visible right here
 * in the source, and there's nothing to keep in sync with a separate file.
 */
class ExcelParserTest {

    private final ExcelParser parser = new ExcelParser();

    @Test
    void parsesAllValidRowsFromTheSampleTable() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "rahul@email.com", "Java, Spring Boot, SQL", "1"},
                new String[] {"C002", "Amit", "amit@email.com", "Python, Django, SQL", "2"},
                new String[] {"C003", "Neha", "neha@email.com", "Java, Spring Boot, Kafka, Docker", "1"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.errors()).isEmpty();
        assertThat(result.candidates()).hasSize(3);
        ParsedCandidate neha = result.candidates().get(2);
        assertThat(neha.candidateId()).isEqualTo("C003");
        assertThat(neha.skills()).containsExactly("Java", "Spring Boot", "Kafka", "Docker");
        assertThat(neha.experience()).isEqualTo(1);
    }

    @Test
    void missingCandidateId_isReportedAsARowErrorNotAnException() {
        InputStream excel = workbookOf(
                new String[] {"", "Rahul", "rahul@email.com", "Java", "1"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.candidates()).isEmpty();
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0).reason()).contains("missing candidate ID");
        assertThat(result.errors().get(0).rowNumber()).isEqualTo(2); // header is row 1
    }

    @Test
    void invalidEmail_isReportedAsARowError() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "not-an-email", "Java", "1"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.errors().get(0).reason()).contains("invalid email");
    }

    @Test
    void nonNumericExperience_isReportedAsARowError() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "rahul@email.com", "Java", "two"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.errors().get(0).reason()).contains("invalid experience");
    }

    @Test
    void negativeExperience_isReportedAsARowError() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "rahul@email.com", "Java", "-1"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.errors().get(0).reason()).contains("must not be negative");
    }

    @Test
    void aRowWithMultipleProblems_reportsAllOfThem() {
        InputStream excel = workbookOf(
                new String[] {"", "", "not-an-email", "", "-1"}
        );

        ExcelParseResult result = parser.parse(excel);

        String reason = result.errors().get(0).reason();
        assertThat(reason)
                .contains("missing candidate ID")
                .contains("missing name")
                .contains("invalid email")
                .contains("missing skills")
                .contains("must not be negative");
    }

    @Test
    void oneBadRowDoesNotStopTheGoodRowsFromParsing() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "rahul@email.com", "Java", "1"},
                new String[] {"", "Amit", "amit@email.com", "Python", "2"}, // bad: missing id
                new String[] {"C003", "Neha", "neha@email.com", "Kafka", "1"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.candidates()).hasSize(2);
        assertThat(result.errors()).hasSize(1);
    }

    @Test
    void completelyBlankRows_areSkippedSilently() {
        InputStream excel = workbookOf(
                new String[] {"C001", "Rahul", "rahul@email.com", "Java", "1"},
                new String[] {"", "", "", "", ""},
                new String[] {"C002", "Amit", "amit@email.com", "Python", "2"}
        );

        ExcelParseResult result = parser.parse(excel);

        assertThat(result.candidates()).hasSize(2);
        assertThat(result.errors()).isEmpty();
    }

    @Test
    void aWorkbookWithNoHeaderRow_isRejectedAsMalformed() {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            workbook.createSheet("Candidates"); // no rows at all
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            workbook.write(out);
            InputStream excel = new ByteArrayInputStream(out.toByteArray());

            assertThrows(MalformedExcelException.class, () -> parser.parse(excel));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void aFileThatIsNotAnExcelWorkbookAtAll_isRejectedAsMalformed() {
        InputStream notExcel = new ByteArrayInputStream("just some plain text".getBytes());

        assertThrows(MalformedExcelException.class, () -> parser.parse(notExcel));
    }

    private static InputStream workbookOf(String[]... dataRows) {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Candidates");
            writeRow(sheet, 0, "candidate_id", "name", "email", "skills", "experience");
            for (int i = 0; i < dataRows.length; i++) {
                writeRow(sheet, i + 1, dataRows[i]);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            workbook.write(out);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeRow(Sheet sheet, int rowIndex, String... values) {
        Row row = sheet.createRow(rowIndex);
        for (int col = 0; col < values.length; col++) {
            row.createCell(col).setCellValue(values[col]);
        }
    }
}
