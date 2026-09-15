package com.example.recruitment.parser;

import com.example.recruitment.dto.CandidateRowError;
import com.example.recruitment.dto.CsvSkills;
import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.ParsedCandidate;
import com.example.recruitment.exception.MalformedExcelException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads the candidate spreadsheet: candidate_id | name | email | skills | experience.
 * Row 1 is the header; data starts at row 2. Every row is validated
 * independently - one bad row becomes a {@code CandidateRowError} and
 * parsing continues, so a typo in row 4,000 of a 5,000-row file doesn't
 * cost you the other 4,999 candidates.
 */
@Component
public class ExcelParser {

    private static final int COL_CANDIDATE_ID = 0;
    private static final int COL_NAME = 1;
    private static final int COL_EMAIL = 2;
    private static final int COL_SKILLS = 3;
    private static final int COL_EXPERIENCE = 4;

    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    public ExcelParseResult parse(InputStream excelInputStream) {
        // Workbook: the whole spreadsheet file. Sheet: one tab inside it -
        // we only ever read the first one. Row/Cell: exactly what they sound
        // like. try-with-resources: Workbook holds file handles/buffers that
        // must be closed once we're done reading.
        try (Workbook workbook = WorkbookFactory.create(excelInputStream)) {
            if (workbook.getNumberOfSheets() == 0) {
                throw new MalformedExcelException("Excel file has no sheets");
            }
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet.getRow(0) == null) {
                throw new MalformedExcelException("Excel file has no header row");
            }

            // DataFormatter reads any cell - text, number, whatever the
            // original type was - as the string a human would see in Excel.
            // That means we validate/parse everything as text uniformly,
            // instead of branching on Cell.getCellType() for every column.
            DataFormatter formatter = new DataFormatter();

            List<ParsedCandidate> candidates = new ArrayList<>();
            List<CandidateRowError> errors = new ArrayList<>();

            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (isBlankRow(row, formatter)) {
                    continue; // an empty row is noise, not an error
                }

                int excelRowNumber = rowIndex + 1; // 1-based, matches what a human sees in Excel
                List<String> issues = new ArrayList<>();
                ParsedCandidate candidate = tryParseRow(row, formatter, issues);

                if (issues.isEmpty()) {
                    candidates.add(candidate);
                } else {
                    errors.add(new CandidateRowError(excelRowNumber, String.join("; ", issues)));
                }
            }

            return new ExcelParseResult(candidates, errors);

        } catch (IOException | EncryptedDocumentException e) {
            throw new MalformedExcelException("Unable to read the uploaded file as an Excel workbook", e);
        }
    }

    /**
     * Validates one row and builds its ParsedCandidate. Every problem found
     * is appended to {@code issues} rather than thrown, so we can report
     * "missing name; invalid experience" instead of stopping at the first
     * issue. Returns null when {@code issues} ends up non-empty; the caller
     * checks that before touching the return value.
     */
    private ParsedCandidate tryParseRow(Row row, DataFormatter formatter, List<String> issues) {
        String candidateId = cellText(row, COL_CANDIDATE_ID, formatter);
        String name = cellText(row, COL_NAME, formatter);
        String email = cellText(row, COL_EMAIL, formatter);
        String skillsRaw = cellText(row, COL_SKILLS, formatter);
        String experienceRaw = cellText(row, COL_EXPERIENCE, formatter);

        if (candidateId.isBlank()) {
            issues.add("missing candidate ID");
        }
        if (name.isBlank()) {
            issues.add("missing name");
        }
        if (email.isBlank()) {
            issues.add("missing email");
        } else if (!EMAIL_PATTERN.matcher(email).matches()) {
            issues.add("invalid email: '" + email + "'");
        }
        if (skillsRaw.isBlank()) {
            issues.add("missing skills");
        }

        int experience = 0;
        if (experienceRaw.isBlank()) {
            issues.add("missing experience");
        } else {
            try {
                experience = Integer.parseInt(experienceRaw);
                if (experience < 0) {
                    issues.add("invalid experience: must not be negative");
                }
            } catch (NumberFormatException e) {
                issues.add("invalid experience: '" + experienceRaw + "' is not a whole number");
            }
        }

        if (!issues.isEmpty()) {
            return null;
        }
        return new ParsedCandidate(candidateId, name, email, CsvSkills.parse(skillsRaw), experience);
    }

    private static boolean isBlankRow(Row row, DataFormatter formatter) {
        if (row == null) {
            return true;
        }
        for (int col = COL_CANDIDATE_ID; col <= COL_EXPERIENCE; col++) {
            if (!cellText(row, col, formatter).isBlank()) {
                return false;
            }
        }
        return true;
    }

    private static String cellText(Row row, int columnIndex, DataFormatter formatter) {
        Cell cell = row.getCell(columnIndex);
        return cell == null ? "" : formatter.formatCellValue(cell).trim();
    }
}
