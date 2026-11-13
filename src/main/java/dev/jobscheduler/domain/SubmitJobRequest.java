package dev.jobscheduler.domain;

import java.time.Instant;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

/** {@code runAt} is optional: no task starts before this instant. */
public record SubmitJobRequest(
        @NotBlank @Size(max = 200) String name,
        Instant runAt,
        @NotEmpty @Size(max = 500) List<@Valid TaskSpec> tasks) {
}
