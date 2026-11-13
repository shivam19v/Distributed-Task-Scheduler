package dev.jobscheduler.handler;

import java.util.UUID;

public record TaskContext(UUID taskId, UUID jobId, String name, int attempt, Params params) {
}
