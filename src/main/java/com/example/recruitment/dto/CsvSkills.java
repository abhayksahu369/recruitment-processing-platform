package com.example.recruitment.dto;

import java.util.Arrays;
import java.util.List;

/**
 * Every skills-like field in this system - a job's required skills, a
 * candidate's skills, a match result's matched/missing skills - is stored
 * as a comma-separated string but handled as a List&lt;String&gt;
 * everywhere it's actually used (parsing, matching, API responses). This
 * is the one place that conversion happens in either direction, so every
 * caller trims and filters blanks the same way. Extracted here once a
 * third call site needed it (Steps 3 and 4 each had their own copy of the
 * "parse" half, small enough not to be worth sharing yet at two).
 */
public final class CsvSkills {

    private CsvSkills() {
    }

    public static List<String> parse(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    public static String format(List<String> skills) {
        return String.join(", ", skills);
    }
}
