package dev.jobscheduler.api;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dev.jobscheduler.domain.EventView;
import dev.jobscheduler.domain.JobStatus;
import dev.jobscheduler.domain.JobSummary;
import dev.jobscheduler.domain.JobView;
import dev.jobscheduler.domain.SubmitJobRequest;
import dev.jobscheduler.service.JobService;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /** Submit a DAG of tasks. Send an {@code Idempotency-Key} header to make retries safe. */
    @PostMapping
    public ResponseEntity<JobView> submit(@Valid @RequestBody SubmitJobRequest request,
                                          @RequestHeader(value = "Idempotency-Key", required = false) String key) {
        JobView job = service.submit(request, key);
        return ResponseEntity.accepted().location(URI.create("/api/v1/jobs/" + job.id())).body(job);
    }

    @GetMapping("/{id}")
    public JobView get(@PathVariable UUID id) {
        return service.get(id);
    }

    @GetMapping
    public List<JobSummary> list(@RequestParam(required = false) JobStatus status,
                                 @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, limit);
    }

    @GetMapping("/{id}/events")
    public List<EventView> events(@PathVariable UUID id) {
        return service.events(id);
    }

    @PostMapping("/{id}/cancel")
    public JobView cancel(@PathVariable UUID id) {
        return service.cancel(id);
    }

    @PostMapping("/{id}/retry")
    public JobView retry(@PathVariable UUID id) {
        return service.retry(id);
    }
}
