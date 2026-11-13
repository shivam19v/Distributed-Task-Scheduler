package dev.jobscheduler.domain;

import java.time.Instant;
import java.util.UUID;

public record EventView(long id, UUID taskId, String event, String worker, String detail, Instant at) {
}
