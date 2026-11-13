package dev.jobscheduler.repo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import dev.jobscheduler.domain.EventView;
import dev.jobscheduler.domain.JobStatus;
import dev.jobscheduler.domain.JobSummary;
import dev.jobscheduler.domain.TaskStatus;
import dev.jobscheduler.domain.TaskView;

@Repository
public class JobRepository {

    private static final RowMapper<JobSummary> SUMMARY = (rs, i) -> new JobSummary(
            rs.getObject("id", UUID.class),
            rs.getString("name"),
            JobStatus.valueOf(rs.getString("status")),
            instant(rs, "created_at"),
            instant(rs, "finished_at"));

    private final JdbcClient jdbc;

    public JobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        OffsetDateTime t = rs.getObject(col, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    private static OffsetDateTime odt(Instant i) {
        return OffsetDateTime.ofInstant(i, ZoneOffset.UTC);
    }

    // ------------------------------------------------------------------ writes

    public void insertJob(UUID id, String name, String idempotencyKey) {
        jdbc.sql("insert into jobs (id, name, status, idempotency_key) values (:id, :name, 'PENDING', :key)")
                .param("id", id).param("name", name).param("key", idempotencyKey, Types.VARCHAR)
                .update();
    }

    public void insertTask(UUID id, UUID jobId, int ord, String name, String type, String paramsJson,
                           TaskStatus status, int priority, int maxAttempts, int backoffSeconds, Instant notBefore) {
        jdbc.sql("""
                insert into tasks (id, job_id, ord, name, type, params, status, priority,
                                   max_attempts, backoff_seconds, not_before)
                values (:id, :job, :ord, :name, :type, cast(:params as jsonb), :status, :priority,
                        :maxAttempts, :backoff, :notBefore)
                """)
                .param("id", id).param("job", jobId).param("ord", ord)
                .param("name", name).param("type", type).param("params", paramsJson)
                .param("status", status.name()).param("priority", priority)
                .param("maxAttempts", maxAttempts).param("backoff", backoffSeconds)
                .param("notBefore", odt(notBefore))
                .update();
    }

    public void insertDependency(UUID taskId, UUID dependsOn) {
        jdbc.sql("insert into task_dependencies (task_id, depends_on_task_id) values (:t, :d)")
                .param("t", taskId).param("d", dependsOn).update();
    }

    /** Cancels everything that has not finished. Running tasks notice on their next heartbeat. */
    public void cancel(UUID jobId) {
        jdbc.sql("""
                update tasks set status = 'CANCELLED', finished_at = now(),
                       lease_owner = null, lease_expires_at = null, updated_at = now()
                where job_id = :id and status in ('PENDING', 'READY', 'RUNNING')
                """).param("id", jobId).update();
        jdbc.sql("""
                update jobs set status = 'CANCELLED', finished_at = now(), updated_at = now()
                where id = :id and status not in ('SUCCEEDED', 'FAILED', 'CANCELLED')
                """).param("id", jobId).update();
    }

    /** Resets FAILED / SKIPPED / CANCELLED tasks so the job can be re-run; succeeded work is kept. */
    public void retry(UUID jobId) {
        jdbc.sql("""
                update tasks set status = 'PENDING', attempt = 0, error = null, result = null,
                       finished_at = null, not_before = now(), updated_at = now()
                where job_id = :id and status in ('FAILED', 'SKIPPED', 'CANCELLED')
                """).param("id", jobId).update();
        jdbc.sql("update jobs set status = 'PENDING', finished_at = null, updated_at = now() where id = :id")
                .param("id", jobId).update();
    }

    /** Derives job status from its tasks. Called by the coordinator leader. */
    public int refreshStatuses() {
        return jdbc.sql("""
                update jobs j set
                    status = s.new_status,
                    finished_at = case when s.new_status in ('SUCCEEDED', 'FAILED', 'CANCELLED') then now() else null end,
                    updated_at = now()
                from (
                    select job_id,
                           case
                             when count(*) filter (where status in ('PENDING', 'READY', 'RUNNING')) > 0 then
                               case when count(*) filter (where status not in ('PENDING', 'READY')) > 0
                                    then 'RUNNING' else 'PENDING' end
                             when count(*) filter (where status = 'FAILED') > 0 then 'FAILED'
                             when count(*) filter (where status = 'CANCELLED') > 0 then 'CANCELLED'
                             else 'SUCCEEDED'
                           end as new_status
                    from tasks
                    where job_id in (select id from jobs where status in ('PENDING', 'RUNNING'))
                    group by job_id
                ) s
                where j.id = s.job_id
                  and j.status in ('PENDING', 'RUNNING')
                  and j.status <> s.new_status
                """).update();
    }

    // ------------------------------------------------------------------ reads

    public Optional<UUID> findIdByIdempotencyKey(String key) {
        return jdbc.sql("select id from jobs where idempotency_key = :k")
                .param("k", key).query(UUID.class).optional();
    }

    public Optional<JobSummary> find(UUID id) {
        return jdbc.sql("select id, name, status, created_at, finished_at from jobs where id = :id")
                .param("id", id).query(SUMMARY).optional();
    }

    public List<JobSummary> list(JobStatus status, int limit) {
        String sql = "select id, name, status, created_at, finished_at from jobs "
                + (status != null ? "where status = :status " : "")
                + "order by created_at desc limit :limit";
        JdbcClient.StatementSpec spec = jdbc.sql(sql).param("limit", limit);
        if (status != null) {
            spec = spec.param("status", status.name());
        }
        return spec.query(SUMMARY).list();
    }

    public List<TaskView> findTasks(UUID jobId) {
        Map<UUID, List<String>> deps = new HashMap<>();
        jdbc.sql("""
                select d.task_id, dep.name
                from task_dependencies d
                join tasks dep on dep.id = d.depends_on_task_id
                join tasks t on t.id = d.task_id
                where t.job_id = :id
                order by dep.ord
                """)
                .param("id", jobId)
                .query((rs, i) -> {
                    deps.computeIfAbsent(rs.getObject("task_id", UUID.class), k -> new ArrayList<>())
                            .add(rs.getString("name"));
                    return 0;
                }).list();

        return jdbc.sql("""
                select id, name, type, status, attempt, max_attempts, lease_owner, result, error,
                       started_at, finished_at
                from tasks where job_id = :id order by ord
                """)
                .param("id", jobId)
                .query((rs, i) -> {
                    UUID id = rs.getObject("id", UUID.class);
                    return new TaskView(
                            id,
                            rs.getString("name"),
                            rs.getString("type"),
                            TaskStatus.valueOf(rs.getString("status")),
                            rs.getInt("attempt"),
                            rs.getInt("max_attempts"),
                            rs.getString("lease_owner"),
                            rs.getString("result"),
                            rs.getString("error"),
                            deps.getOrDefault(id, List.of()),
                            instant(rs, "started_at"),
                            instant(rs, "finished_at"));
                }).list();
    }

    public List<EventView> findEvents(UUID jobId, int limit) {
        return jdbc.sql("""
                select id, task_id, event, worker, detail, at
                from task_events where job_id = :id order by id limit :limit
                """)
                .param("id", jobId).param("limit", limit)
                .query((rs, i) -> new EventView(
                        rs.getLong("id"),
                        rs.getObject("task_id", UUID.class),
                        rs.getString("event"),
                        rs.getString("worker"),
                        rs.getString("detail"),
                        instant(rs, "at")))
                .list();
    }
}
