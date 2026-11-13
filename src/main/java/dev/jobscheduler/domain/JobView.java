package dev.jobscheduler.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record JobView(
        UUID id,
        String name,
        JobStatus status,
        Instant createdAt,
        Instant finishedAt,
        List<TaskView> tasks) {

    public static JobView of(JobSummary s, List<TaskView> tasks) {
        return new JobView(s.id(), s.name(), s.status(), s.createdAt(), s.finishedAt(), tasks);
    }
}
