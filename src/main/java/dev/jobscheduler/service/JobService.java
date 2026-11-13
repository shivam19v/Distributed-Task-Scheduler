package dev.jobscheduler.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import dev.jobscheduler.dag.DagValidator;
import dev.jobscheduler.domain.EventView;
import dev.jobscheduler.domain.JobStatus;
import dev.jobscheduler.domain.JobSummary;
import dev.jobscheduler.domain.JobView;
import dev.jobscheduler.domain.SchedulerException;
import dev.jobscheduler.domain.SubmitJobRequest;
import dev.jobscheduler.domain.TaskSpec;
import dev.jobscheduler.domain.TaskStatus;
import dev.jobscheduler.repo.JobRepository;
import tools.jackson.databind.json.JsonMapper;

@Service
public class JobService {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final int DEFAULT_BACKOFF_SECONDS = 5;

    private final JobRepository jobs;
    private final TransactionTemplate tx;
    private final JsonMapper json;

    public JobService(JobRepository jobs, TransactionTemplate tx, JsonMapper json) {
        this.jobs = jobs;
        this.tx = tx;
        this.json = json;
    }

    /** Submits a job atomically. With an idempotency key, resubmitting returns the original job. */
    public JobView submit(SubmitJobRequest req, String idempotencyKey) {
        String key = (idempotencyKey == null || idempotencyKey.isBlank()) ? null : idempotencyKey;
        if (key != null) {
            var existing = jobs.findIdByIdempotencyKey(key);
            if (existing.isPresent()) {
                return get(existing.get());
            }
        }

        DagValidator.validate(req.tasks().stream()
                .map(t -> new DagValidator.Node(t.name(), t.dependsOn()))
                .toList());

        UUID jobId = UUID.randomUUID();
        try {
            tx.executeWithoutResult(status -> persist(jobId, req, key));
        } catch (DuplicateKeyException race) {
            if (key == null) throw race;
            return get(jobs.findIdByIdempotencyKey(key).orElseThrow(() -> race));
        }
        return get(jobId);
    }

    private void persist(UUID jobId, SubmitJobRequest req, String key) {
        var notBefore = req.runAt() != null ? req.runAt() : java.time.Instant.now();
        jobs.insertJob(jobId, req.name(), key);

        Map<String, UUID> ids = new HashMap<>();
        req.tasks().forEach(t -> ids.put(t.name(), UUID.randomUUID()));

        int ord = 0;
        for (TaskSpec t : req.tasks()) {
            boolean root = t.dependsOn() == null || t.dependsOn().isEmpty();
            jobs.insertTask(
                    ids.get(t.name()), jobId, ord++, t.name(), t.type(),
                    json.writeValueAsString(t.params() == null ? Map.of() : t.params()),
                    root ? TaskStatus.READY : TaskStatus.PENDING,
                    t.priority() == null ? 0 : t.priority(),
                    t.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : t.maxAttempts(),
                    t.backoffSeconds() == null ? DEFAULT_BACKOFF_SECONDS : t.backoffSeconds(),
                    notBefore);
        }
        for (TaskSpec t : req.tasks()) {
            if (t.dependsOn() == null) continue;
            for (String dep : t.dependsOn().stream().distinct().toList()) {
                jobs.insertDependency(ids.get(t.name()), ids.get(dep));
            }
        }
    }

    public JobView get(UUID id) {
        JobSummary s = jobs.find(id).orElseThrow(() -> new SchedulerException.NotFound("Job not found: " + id));
        return JobView.of(s, jobs.findTasks(id));
    }

    public List<JobSummary> list(JobStatus status, int limit) {
        return jobs.list(status, Math.max(1, Math.min(limit, 200)));
    }

    public List<EventView> events(UUID id) {
        jobs.find(id).orElseThrow(() -> new SchedulerException.NotFound("Job not found: " + id));
        return jobs.findEvents(id, 1000);
    }

    public JobView cancel(UUID id) {
        JobSummary s = jobs.find(id).orElseThrow(() -> new SchedulerException.NotFound("Job not found: " + id));
        if (s.status().isTerminal()) {
            throw new SchedulerException.Conflict("Job is already " + s.status());
        }
        tx.executeWithoutResult(st -> jobs.cancel(id));
        return get(id);
    }

    public JobView retry(UUID id) {
        JobSummary s = jobs.find(id).orElseThrow(() -> new SchedulerException.NotFound("Job not found: " + id));
        if (s.status() != JobStatus.FAILED && s.status() != JobStatus.CANCELLED) {
            throw new SchedulerException.Conflict("Only FAILED or CANCELLED jobs can be retried (job is " + s.status() + ")");
        }
        tx.executeWithoutResult(st -> jobs.retry(id));
        return get(id);
    }
}
