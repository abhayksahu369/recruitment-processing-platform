package com.example.recruitment.parser;

import com.example.recruitment.dto.CsvSkills;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.exception.JobDescriptionParseException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the fixed-format job description text:
 *
 * <pre>
 * Java Backend Developer
 *
 * Skills:
 * Java, Spring Boot, SQL, Kafka, Docker
 *
 * Experience:
 * 0-2
 * </pre>
 *
 * This is a deterministic, position/label-based parser, not NLP - the
 * first non-blank line is the title, and each "Skills:"/"Experience:"
 * label is followed by the line that holds its value. Anything that
 * doesn't match this shape fails the whole request (see
 * {@link JobDescriptionParseException}'s Javadoc for why that's the right
 * call here, unlike candidate-row validation).
 */
@Component
public class JobDescriptionParser {

    private static final String SKILLS_LABEL = "Skills:";
    private static final String EXPERIENCE_LABEL = "Experience:";
    private static final Pattern EXPERIENCE_RANGE = Pattern.compile("^(\\d+)\\s*-\\s*(\\d+)$");

    public JobRequirements parse(String jobDescriptionText) {
        if (jobDescriptionText == null || jobDescriptionText.isBlank()) {
            throw new JobDescriptionParseException("Job description is empty");
        }

        List<String> lines = jobDescriptionText.lines().map(String::trim).toList();

        String jobTitle = lines.stream()
                .filter(line -> !line.isEmpty())
                .findFirst()
                .orElseThrow(() -> new JobDescriptionParseException("Job description has no title line"));

        List<String> requiredSkills = extractLineAfterLabel(lines, SKILLS_LABEL)
                .map(CsvSkills::parse)
                .filter(skills -> !skills.isEmpty())
                .orElseThrow(() -> new JobDescriptionParseException(
                        "Job description must have a '" + SKILLS_LABEL + "' line "
                                + "followed by a comma-separated skill list"));

        String experienceLine = extractLineAfterLabel(lines, EXPERIENCE_LABEL)
                .orElseThrow(() -> new JobDescriptionParseException(
                        "Job description must have an '" + EXPERIENCE_LABEL + "' line "
                                + "followed by a range like '0-2'"));

        Matcher matcher = EXPERIENCE_RANGE.matcher(experienceLine);
        if (!matcher.matches()) {
            throw new JobDescriptionParseException(
                    "Experience must be a range like '0-2', found: '" + experienceLine + "'");
        }

        int minExperience = Integer.parseInt(matcher.group(1));
        int maxExperience = Integer.parseInt(matcher.group(2));
        if (minExperience > maxExperience) {
            throw new JobDescriptionParseException(
                    "Minimum experience (" + minExperience + ") cannot exceed maximum (" + maxExperience + ")");
        }

        return new JobRequirements(jobTitle, requiredSkills, minExperience, maxExperience);
    }

    /** Finds a "Label:" line and returns the next non-blank line after it, if any. */
    private static Optional<String> extractLineAfterLabel(List<String> lines, String label) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).equalsIgnoreCase(label)) {
                for (int j = i + 1; j < lines.size(); j++) {
                    if (!lines.get(j).isEmpty()) {
                        return Optional.of(lines.get(j));
                    }
                }
            }
        }
        return Optional.empty();
    }
}
