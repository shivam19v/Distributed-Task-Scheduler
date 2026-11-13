package dev.jobscheduler.engine;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import dev.jobscheduler.config.SchedulerProperties;
import dev.jobscheduler.domain.TaskStatus;
import dev.jobscheduler.repo.JobRepository;
import dev.jobscheduler.repo.TaskRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Leader-only housekeeping, run every {@code coordinator-interval}:
 * <ol>
 *   <li>reap tasks whose lease expired (dead worker) -> re-queue or fail</li>
 *   <li>skip tasks whose upstream failed</li>
 *   <li>promote tasks whose dependencies all succeeded (the DAG engine)</li>
 *   <li>roll task states up into job states</li>
 * </ol>
 */
@Component
public class Coordinator implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final SchedulerProperties props;
    private final LeaderElector leader;
    private final TaskRepository tasks;
    private final JobRepository jobs;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("coordinator").daemon(true).factory());
    private volatile boolean running;

    public Coordinator(SchedulerProperties props, LeaderElector leader, TaskRepository tasks,
                       JobRepository jobs, MeterRegistry meters) {
        this.props = props;
        this.leader = leader;
        this.tasks = tasks;
        this.jobs = jobs;
        Gauge.builder("scheduler.leader", () -> leader.isLeader() ? 1 : 0)
                .description("1 if this node is the coordinator leader").register(meters);
        Gauge.builder("scheduler.tasks.ready", tasks, t -> t.count(TaskStatus.READY))
                .description("Tasks waiting for a worker").register(meters);
        Gauge.builder("scheduler.tasks.running", tasks, t -> t.count(TaskStatus.RUNNING))
                .description("Tasks currently leased (cluster-wide)").register(meters);
    }

    @Override
    public void start() {
        running = true;
        executor.scheduleWithFixedDelay(this::tick, 0, props.coordinatorInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        running = false;
        executor.shutdownNow();
        leader.resign();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    void tick() {
        try {
            if (!leader.tryLead()) {
                return;
            }
            int reaped = tasks.reapExpired();
            int skipped = 0;
            int n;
            do {
                n = tasks.skipBlocked();
                skipped += n;
            } while (n > 0);
            int promoted = tasks.promoteReady();
            int jobUpdates = jobs.refreshStatuses();
            if (reaped + skipped + promoted + jobUpdates > 0) {
                log.debug("coordinator: reaped={} skipped={} promoted={} jobsUpdated={}",
                        reaped, skipped, promoted, jobUpdates);
            }
            if (reaped > 0) {
                log.warn("Reaped {} task(s) with expired leases", reaped);
            }
        } catch (Exception e) {
            log.error("Coordinator tick failed", e);
        }
    }
}
