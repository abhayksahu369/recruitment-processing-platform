package com.example.recruitment.parser;

import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.exception.JobDescriptionParseException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobDescriptionParserTest {

    private final JobDescriptionParser parser = new JobDescriptionParser();

    @Test
    void parsesTheSpecExampleExactly() {
        String text = """
                Java Backend Developer

                Skills:
                Java, Spring Boot, SQL, Kafka, Docker

                Experience:
                0-2
                """;

        JobRequirements requirements = parser.parse(text);

        assertThat(requirements.jobTitle()).isEqualTo("Java Backend Developer");
        assertThat(requirements.requiredSkills())
                .containsExactly("Java", "Spring Boot", "SQL", "Kafka", "Docker");
        assertThat(requirements.minExperience()).isEqualTo(0);
        assertThat(requirements.maxExperience()).isEqualTo(2);
    }

    @Test
    void blankDescription_throws() {
        assertThrows(JobDescriptionParseException.class, () -> parser.parse("   "));
    }

    @Test
    void missingSkillsSection_throws() {
        String text = """
                Java Backend Developer

                Experience:
                0-2
                """;

        JobDescriptionParseException ex =
                assertThrows(JobDescriptionParseException.class, () -> parser.parse(text));
        assertThat(ex.getMessage()).contains("Skills:");
    }

    @Test
    void missingExperienceSection_throws() {
        String text = """
                Java Backend Developer

                Skills:
                Java, SQL
                """;

        assertThrows(JobDescriptionParseException.class, () -> parser.parse(text));
    }

    @Test
    void malformedExperienceRange_throws() {
        String text = """
                Java Backend Developer

                Skills:
                Java, SQL

                Experience:
                not-a-range
                """;

        assertThrows(JobDescriptionParseException.class, () -> parser.parse(text));
    }

    @Test
    void minExperienceGreaterThanMax_throws() {
        String text = """
                Java Backend Developer

                Skills:
                Java, SQL

                Experience:
                5-2
                """;

        assertThrows(JobDescriptionParseException.class, () -> parser.parse(text));
    }
}
