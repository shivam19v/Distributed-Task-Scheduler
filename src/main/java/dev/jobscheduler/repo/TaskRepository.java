package dev.jobscheduler.repo;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import dev.jobscheduler.domain.ClaimedTask;
import dev.jobscheduler.domain.TaskStatus;

/**
 * All queue / lease operations. Each is a single atomic SQL statement.
 * <p>
 * Fencing: every transition made by a worker is guarded by {@code lease_token = :token AND status = 'RUNNING'}.
 * The token is incremented on each claim, so a "zombie" worker (paused JVM, network partition) that wakes up
 * after its lease was reaped can never overwrite the state written by the new owner.
 */
@Repository
public class TaskRepository {

    private static final RowMapper<ClaimedTask> CLAIMED = (rs, i) -> new ClaimedTask(
            rs.getObject("id", UUID.class),
            rs.getObject("job_id", UUID.class),
            rs.getString("name"),
            rs.getString("type"),
            rs.getString("params"),
            rs.getInt("attempt"),
            rs.getInt("max_attempts"),
            rs.getInt("backoff_seconds"),
            rs.getLong("lease_token"));

    private final JdbcClient jdbc;

    public TaskRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    private static double secs(Duration d) {
        return d.toMillis() / 1000.0;
    }

    // ------------------------------------------------------------------ worker side

    /** Atomically leases the highest-priority READY task using FOR UPDATE SKIP LOCKED. */
    public Optional<ClaimedTask> claimNext(String node, Duration lease) {
        return jdbc.sql("""
                with c as (
                    select id from tasks
                    where status = 'READY' and not_before <= now()
                    order by priority desc, created_at, ord
                    limit 1
                    for update skip locked
                )
                update tasks t set
                    status           = 'RUNNING',
                    attempt          = t.attempt + 1,
                    lease_owner      = :node,
                    lease_token      = t.lease_token + 1,
                    lease_expires_at = now() + make_interval(secs => :lease),
                    started_at       = coalesce(t.started_at, now()),
                    updated_at       = now()
                from c
                where t.id = c.id
                returning t.id, t.job_id, t.name, t.type, t.params::text as params,
                          t.attempt, t.max_attempts, t.backoff_seconds, t.lease_token
                """)
                .param("node", node)
                .param("lease", secs(lease))
                .query(CLAIMED)
                .optional();
    }

    /** Heartbeat. Returns false if the lease was lost (reaped, cancelled, re-claimed). */
    public boolean renewLease(UUID id, long token, Duration lease) {
        return jdbc.sql("""
                update tasks set lease_expires_at = now() + make_interval(secs => :lease), updated_at = now()
                where id = :id and lease_token = :token and status = 'RUNNING'
                """)
                .param("lease", secs(lease)).param("id", id).param("token", token)
                .update() == 1;
    }

    public boolean complete(UUID id, long token, String result) {
        return jdbc.sql("""
                update tasks set status = 'SUCCEEDED', result = :result, error = null,
                       lease_owner = null, lease_expires_at = null,
                       finished_at = now(), updated_at = now()
                where id = :id and lease_token = :token and status = 'RUNNING'
                """)
                .param("result", result, java.sql.Types.VARCHAR).param("id", id).param("token", token)
                .update() == 1;
    }

    /**
     * Records a failure. Task goes back to READY (after {@code retryDelay}) or to FAILED when attempts are
     * exhausted / the failure is not retryable.
     *
     * @return the new status, or empty if the lease was already lost
     */
    public Optional<String> fail(UUID id, long token, String error, boolean retryable, Duration retryDelay) {
        return jdbc.sql("""
                update tasks set
                    status = case when attempt >= max_attempts or not :retryable then 'FAILED' else 'READY' end,
                    error = :error,
                    not_before = case when attempt >= max_attempts or not :retryable
                                      then not_before else now() + make_interval(secs => :delay) end,
                    finished_at = case when attempt >= max_attempts or not :retryable then now() else null end,
                    lease_owner = null, lease_expires_at = null, updated_at = now()
                where id = :id and lease_token = :token and status = 'RUNNING'
                returning status
                """)
                .param("retryable", retryable)
                .param("error", error, java.sql.Types.VARCHAR)
                .param("delay", secs(retryDelay))
                .param("id", id).param("token", token)
                .query(String.class)
                .optional();
    }

    /** Gives a claimed task back without burning an attempt (shutdown, Redis lock unavailable). */
    public boolean releaseClaim(UUID id, long token) {
        return jdbc.sql("""
                update tasks set status = 'READY', attempt = greatest(attempt - 1, 0),
                       not_before = now() + interval '1 second',
                       lease_owner = null, lease_expires_at = null, updated_at = now()
                where id = :id and lease_token = :token and status = 'RUNNING'
                """)
                .param("id", id).param("token", token)
                .update() == 1;
    }

    // ------------------------------------------------------------------ coordinator side

    /** Lease expired => worker presumed dead. Re-queue (or fail if out of attempts). */
    public int reapExpired() {
        return jdbc.sql("""
                with r as (
                    update tasks set
                        status = case when attempt >= max_attempts then 'FAILED' else 'READY' end,
                        error = 'Lease expired; worker ' || coalesce(lease_owner, '?') || ' presumed dead',
                        finished_at = case when attempt >= max_attempts then now() else null end,
                        not_before = now(),
                        lease_owner = null, lease_expires_at = null, updated_at = now()
                    where status = 'RUNNING' and lease_expires_at < now()
                    returning id, job_id, status, error
                )
                insert into task_events (task_id, job_id, event, detail)
                select id, job_id,
                       case when status = 'FAILED' then 'LEASE_EXPIRED_FAILED' else 'LEASE_EXPIRED_REASSIGNED' end,
                       error
                from r
                """).update();
    }

    /** PENDING -> READY once every upstream task has SUCCEEDED. */
    public int promoteReady() {
        return jdbc.sql("""
                with p as (
                    update tasks t set status = 'READY', updated_at = now()
                    where t.status = 'PENDING'
                      and not exists (
                          select 1 from task_dependencies d
                          join tasks dep on dep.id = d.depends_on_task_id
                          where d.task_id = t.id and dep.status <> 'SUCCEEDED')
                    returning t.id, t.job_id
                )
                insert into task_events (task_id, job_id, event) select id, job_id, 'READY' from p
                """).update();
    }

    /** PENDING -> SKIPPED when any upstream task ended FAILED / CANCELLED / SKIPPED. */
    public int skipBlocked() {
        return jdbc.sql("""
                with s as (
                    update tasks t set status = 'SKIPPED', error = 'Upstream task did not succeed',
                           finished_at = now(), updated_at = now()
                    where t.status = 'PENDING'
                      and exists (
                          select 1 from task_dependencies d
                          join tasks dep on dep.id = d.depends_on_task_id
                          where d.task_id = t.id and dep.status in ('FAILED', 'CANCELLED', 'SKIPPED'))
                    returning t.id, t.job_id
                )
                insert into task_events (task_id, job_id, event) select id, job_id, 'SKIPPED' from s
                """).update();
    }

    // ------------------------------------------------------------------ misc

    public long count(TaskStatus status) {
        return jdbc.sql("select count(*) from tasks where status = :s")
                .param("s", status.name()).query(Long.class).single();
    }

    public void recordEvent(UUID taskId, UUID jobId, String event, String worker, String detail) {
        jdbc.sql("""
                insert into task_events (task_id, job_id, event, worker, detail)
                values (:task, :job, :event, :worker, :detail)
                """)
                .param("task", taskId).param("job", jobId).param("event", event)
                .param("worker", worker, java.sql.Types.VARCHAR)
                .param("detail", detail == null ? null : truncate(detail, 2000), java.sql.Types.VARCHAR)
                .update();
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }
}
