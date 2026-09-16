package com.example.recruitment.benchmark;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * "A simple way to generate synthetic candidates" (per the spec) - a
 * test-scoped utility, not a production API. Manufacturing fake candidate
 * data is a benchmarking concern, not something a real recruiter ever
 * asks the running application to do, so it has no business being an
 * endpoint; it only exists to feed the benchmark tests in this package.
 * <p>
 * Every other row alternates between a skill set that fully matches the
 * standard benchmark job description and one that shares nothing with
 * it, with experience always in-range - the same deterministic ~50/50
 * matched split used in the Step 7/8 scale tests, so a benchmark's
 * correctness (not just its speed) can still be checked exactly at any N.
 */
public final class SyntheticCandidateWorkbook {

    private SyntheticCandidateWorkbook() {
    }

    public static byte[] generate(int candidateCount) {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Candidates");
            writeRow(sheet, 0, "candidate_id", "name", "email", "skills", "experience");
            for (int i = 1; i <= candidateCount; i++) {
                boolean fullMatch = i % 2 == 0;
                String skills = fullMatch ? "Java, Spring Boot, SQL, Kafka, Docker" : "Python, Django";
                writeRow(sheet, i,
                        "C" + i, "Candidate" + i, "candidate" + i + "@test.com", skills, String.valueOf(i % 3));
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            workbook.write(out);
            return out.toByteArray();
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
