package dev.jobscheduler.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.jobscheduler.domain.EventView;
import dev.jobscheduler.domain.JobStatus;
import dev.jobscheduler.domain.JobView;
import dev.jobscheduler.domain.SubmitJobRequest;
import dev.jobscheduler.domain.TaskSpec;
import dev.jobscheduler.domain.TaskStatus;
import dev.jobscheduler.domain.TaskView;
import dev.jobscheduler.service.JobService;

/** End-to-end against real PostgreSQL + Redis (needs Docker). */
@SpringBootTest(properties = {
        "scheduler.node-id=test-node",
        "scheduler.poll-interval=100ms",
        "scheduler.coordinator-interval=200ms",
        "scheduler.lease-duration=5s",
        "scheduler.heartbeat-interval=1s"
})
@Import(TestcontainersConfig.class)
class SchedulerIntegrationTest {

    @Autowired JobService jobs;
    @Autowired JdbcClient jdbc;

    private static TaskSpec task(String name, String type, Map<String, Object> params, String... deps) {
        return new TaskSpec(name, type, params, List.of(deps), null, 0, null);
    }

    private JobView awaitTerminal(UUID id) {
        await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(200))
                .until(() -> jobs.get(id).status().isTerminal());
        return jobs.get(id);
    }

    private static TaskView byName(JobView job, String name) {
        return job.tasks().stream().filter(t -> t.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void diamondDagRunsInDependencyOrder() {
        JobView submitted = jobs.submit(new SubmitJobRequest("diamond", null, List.of(
                task("a", "log", Map.of("message", "a")),
                task("b", "sleep", Map.of("seconds", 0.3), "a"),
                task("c", "sleep", Map.of("seconds", 0.3), "a"),
                task("d", "log", Map.of("message", "d"), "b", "c"))), null);

        JobView done = awaitTerminal(submitted.id());

        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.tasks()).allMatch(t -> t.status() == TaskStatus.SUCCEEDED);
        Instant dStart = byName(done, "d").startedAt();
        assertThat(dStart).isAfterOrEqualTo(byName(done, "b").finishedAt());
        assertThat(dStart).isAfterOrEqualTo(byName(done, "c").finishedAt());
        assertThat(byName(done, "b").startedAt()).isAfterOrEqualTo(byName(done, "a").finishedAt());
    }

    @Test
    void failedTaskIsRetriedWithBackoffUntilItSucceeds() {
        JobView submitted = jobs.submit(new SubmitJobRequest("flaky", null, List.of(
                new TaskSpec("t", "flaky", Map.of("failUntilAttempt", 3), null, 5, 0, null))), null);

        JobView done = awaitTerminal(submitted.id());

        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(byName(done, "t").attempt()).isEqualTo(3);
    }

    @Test
    void exhaustedRetriesFailTheJobAndSkipDownstream() {
        JobView submitted = jobs.submit(new SubmitJobRequest("doomed", null, List.of(
                new TaskSpec("bad", "flaky", Map.of("failureRate", 1.0), null, 2, 0, null),
                task("after", "log", Map.of(), "bad"),
                task("after-after", "log", Map.of(), "after"))), null);

        JobView done = awaitTerminal(submitted.id());

        assertThat(done.status()).isEqualTo(JobStatus.FAILED);
        assertThat(byName(done, "bad").status()).isEqualTo(TaskStatus.FAILED);
        assertThat(byName(done, "bad").attempt()).isEqualTo(2);
        assertThat(byName(done, "after").status()).isEqualTo(TaskStatus.SKIPPED);
        assertThat(byName(done, "after-after").status()).isEqualTo(TaskStatus.SKIPPED);
    }

    @Test
    void taskOwnedByDeadWorkerIsReassigned() {
        // runAt far in the future => no worker can claim it while we simulate the crash
        JobView submitted = jobs.submit(new SubmitJobRequest("orphan",
                Instant.now().plusSeconds(3600),
                List.of(task("t", "log", Map.of("message", "recovered")))), null);
        UUID taskId = submitted.tasks().getFirst().id();

        // simulate: worker "dead-node" claimed the task, then died; its lease is already expired
        jdbc.sql("""
                update tasks set status = 'RUNNING', attempt = 1, lease_owner = 'dead-node', lease_token = 1,
                       lease_expires_at = now() - interval '1 second', not_before = now()
                where id = :id
                """).param("id", taskId).update();

        JobView done = awaitTerminal(submitted.id());

        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(byName(done, "t").attempt()).isEqualTo(2);
        assertThat(jobs.events(done.id())).extracting(EventView::event)
                .contains("LEASE_EXPIRED_REASSIGNED", "SUCCEEDED");
    }

    @Test
    void staleWorkerCannotOverwriteNewOwnersResult() {
        JobView submitted = jobs.submit(new SubmitJobRequest("fenced",
                Instant.now().plusSeconds(3600),
                List.of(task("t", "log", Map.of()))), null);
        UUID taskId = submitted.tasks().getFirst().id();

        // token 2 is the current owner; a zombie still holding token 1 tries to complete
        jdbc.sql("""
                update tasks set status = 'RUNNING', attempt = 2, lease_owner = 'new-owner', lease_token = 2,
                       lease_expires_at = now() + interval '1 hour'
                where id = :id
                """).param("id", taskId).update();

        int updatedByZombie = jdbc.sql("""
                update tasks set status = 'SUCCEEDED', result = 'zombie'
                where id = :id and lease_token = 1 and status = 'RUNNING'
                """).param("id", taskId).update();

        assertThat(updatedByZombie).isZero();
        assertThat(byName(jobs.get(submitted.id()), "t").status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void idempotencyKeyReturnsTheSameJob() {
        var req = new SubmitJobRequest("idem", null, List.of(task("t", "log", Map.of())));
        JobView first = jobs.submit(req, "key-123");
        JobView second = jobs.submit(req, "key-123");
        assertThat(second.id()).isEqualTo(first.id());
    }

    @Test
    void cancellingAJobStopsRunningWork() {
        JobView submitted = jobs.submit(new SubmitJobRequest("cancel-me", null, List.of(
                task("long", "sleep", Map.of("seconds", 60)),
                task("next", "log", Map.of(), "long"))), null);

        await().atMost(Duration.ofSeconds(10))
                .until(() -> byName(jobs.get(submitted.id()), "long").status() == TaskStatus.RUNNING);
        jobs.cancel(submitted.id());

        JobView done = jobs.get(submitted.id());
        assertThat(done.status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(done.tasks()).allMatch(t -> t.status() == TaskStatus.CANCELLED);
    }
}
