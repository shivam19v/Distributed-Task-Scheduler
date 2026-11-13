package dev.jobscheduler.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import dev.jobscheduler.config.SchedulerProperties;
import dev.jobscheduler.domain.ClaimedTask;
import dev.jobscheduler.handler.NonRetryableException;
import dev.jobscheduler.handler.Params;
import dev.jobscheduler.handler.TaskContext;
import dev.jobscheduler.handler.TaskHandler;
import dev.jobscheduler.lock.RedisLockService;
import dev.jobscheduler.repo.TaskRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Executes tasks on virtual threads.
 *
 * <pre>
 * poll loop   : claim READY tasks (FOR UPDATE SKIP LOCKED) while free slots exist
 * run         : take Redis lock -> run handler -> fenced complete()/fail()
 * heartbeat   : extend DB lease + Redis lock for every running task; interrupt the handler if the lease is lost
 * shutdown    : stop claiming, drain in-flight tasks, hand back whatever is left (no attempt burned)
 * </pre>
 */
@Component
public class Worker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final class Execution {
        final ClaimedTask task;
        volatile Thread runner;
        volatile boolean leaseLost;

        Execution(ClaimedTask task) {
            this.task = task;
        }
    }

    private final SchedulerProperties props;
    private final TaskRepository tasks;
    private final RedisLockService locks;
    private final Map<String, TaskHandler> handlers;
    private final JsonMapper json;
    private final MeterRegistry meters;

    private final Semaphore slots;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService ticker = Executors.newScheduledThreadPool(2,
            Thread.ofPlatform().name("worker-ticker-", 0).daemon(true).factory());
    private final Map<UUID, Execution> running = new ConcurrentHashMap<>();

    private volatile boolean accepting;
    private volatile boolean started;

    public Worker(SchedulerProperties props, TaskRepository tasks, RedisLockService locks,
                  List<TaskHandler> handlerBeans, JsonMapper json, MeterRegistry meters) {
        this.props = props;
        this.tasks = tasks;
        this.locks = locks;
        this.handlers = handlerBeans.stream().collect(Collectors.toMap(TaskHandler::type, Function.identity()));
        this.json = json;
        this.meters = meters;
        this.slots = new Semaphore(props.workerConcurrency());
        Gauge.builder("scheduler.worker.running", running, Map::size)
                .description("Tasks executing on this node").register(meters);
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void start() {
        log.info("Worker {} starting: concurrency={}, lease={}, handlers={}",
                props.nodeId(), props.workerConcurrency(), props.leaseDuration(), handlers.keySet());
        accepting = true;
        started = true;
        ticker.scheduleWithFixedDelay(this::poll, 0, props.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
        ticker.scheduleWithFixedDelay(this::heartbeat, props.heartbeatInterval().toMillis(),
                props.heartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        accepting = false;
        started = false;
        executor.shutdown();
        try {
            log.info("Worker {} draining {} in-flight task(s)...", props.nodeId(), running.size());
            if (!executor.awaitTermination(props.drainTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Drain timeout; interrupting {} task(s) and handing them back", running.size());
                executor.shutdownNow(); // handlers see InterruptedException -> claim released, attempt not burned
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ticker.shutdownNow(); // heartbeat kept running during the drain on purpose
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    // ------------------------------------------------------------------ claim loop

    void poll() {
        while (accepting && slots.tryAcquire()) {
            Optional<ClaimedTask> claimed;
            try {
                claimed = tasks.claimNext(props.nodeId(), props.leaseDuration());
            } catch (Exception e) {
                slots.release();
                log.warn("Claim failed: {}", e.toString());
                return;
            }
            if (claimed.isEmpty()) {
                slots.release();
                return;
            }
            ClaimedTask task = claimed.get();
            Execution ex = new Execution(task);
            running.put(task.id(), ex);
            try {
                executor.execute(() -> run(ex));
            } catch (RuntimeException rejected) { // executor shut down between check and submit
                running.remove(task.id());
                slots.release();
                tasks.releaseClaim(task.id(), task.leaseToken());
                return;
            }
        }
    }

    // ------------------------------------------------------------------ execution

    private void run(Execution ex) {
        ClaimedTask t = ex.task;
        ex.runner = Thread.currentThread();
        String lockKey = lockKey(t);
        String lockOwner = lockOwner(t);
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            tasks.recordEvent(t.id(), t.jobId(), "STARTED", props.nodeId(), "attempt " + t.attempt());
            if (!tryLock(lockKey, lockOwner)) {
                // Redis unavailable, or a previous owner's lock is still alive: don't run, don't burn an attempt.
                tasks.releaseClaim(t.id(), t.leaseToken());
                outcome = "deferred";
                return;
            }
            try {
                outcome = execute(ex);
            } finally {
                unlock(lockKey, lockOwner);
            }
        } catch (Throwable e) {
            log.error("Unexpected error running task {}", t.id(), e);
        } finally {
            sample.stop(Timer.builder("scheduler.task.duration")
                    .tag("type", t.type()).tag("outcome", outcome).register(meters));
            running.remove(t.id());
            slots.release();
        }
    }

    private String execute(Execution ex) {
        ClaimedTask t = ex.task;
        TaskHandler handler = handlers.get(t.type());
        if (handler == null) {
            return fail(t, "No handler registered for task type '" + t.type() + "'", false);
        }

        String result = null;
        Exception error = null;
        boolean interrupted = false;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = json.readValue(t.params(), Map.class);
            result = handler.execute(new TaskContext(t.id(), t.jobId(), t.name(), t.attempt(), new Params(params)));
        } catch (InterruptedException e) {
            interrupted = true;
        } catch (Exception e) {
            error = e;
        }
        Thread.interrupted(); // clear the flag so the JDBC calls below work

        if (interrupted) {
            if (ex.leaseLost) {
                log.warn("Task {} ({}) interrupted: lease lost / job cancelled", t.name(), t.id());
                return "lease_lost";
            }
            tasks.releaseClaim(t.id(), t.leaseToken()); // node shutting down: hand back immediately
            return "released";
        }
        if (error != null) {
            return fail(t, describe(error), !(error instanceof NonRetryableException));
        }

        if (tasks.complete(t.id(), t.leaseToken(), truncate(result, 4000))) {
            tasks.recordEvent(t.id(), t.jobId(), "SUCCEEDED", props.nodeId(), null);
            return "success";
        }
        log.warn("Task {} finished but lease was lost; result discarded (fenced)", t.id());
        return "fenced";
    }

    private String fail(ClaimedTask t, String message, boolean retryable) {
        long exp = (long) t.backoffSeconds() << Math.min(Math.max(t.attempt() - 1, 0), 10);
        Duration delay = Duration.ofSeconds(Math.min(exp, 3600));
        Optional<String> status = tasks.fail(t.id(), t.leaseToken(), truncate(message, 4000), retryable, delay);
        if (status.isEmpty()) {
            log.warn("Task {} failed but lease was lost; ignoring (fenced)", t.id());
            return "fenced";
        }
        boolean terminal = "FAILED".equals(status.get());
        tasks.recordEvent(t.id(), t.jobId(), terminal ? "FAILED" : "RETRY_SCHEDULED", props.nodeId(),
                message + (terminal ? "" : " (retry in " + delay.toSeconds() + "s)"));
        return terminal ? "failed" : "retry";
    }

    // ------------------------------------------------------------------ heartbeat

    void heartbeat() {
        for (Execution ex : running.values()) {
            ClaimedTask t = ex.task;
            try {
                boolean ok = tasks.renewLease(t.id(), t.leaseToken(), props.leaseDuration());
                if (!ok) {
                    ex.leaseLost = true;
                    Thread runner = ex.runner;
                    if (runner != null) runner.interrupt();
                    continue;
                }
                if (!locks.renew(lockKey(t), lockOwner(t), props.leaseDuration())) {
                    // DB fencing still protects correctness; this only means the extra Redis guard lapsed.
                    log.warn("Redis lock for task {} expired while running", t.id());
                }
            } catch (Exception e) {
                log.warn("Heartbeat for task {} failed: {}", t.id(), e.toString());
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private boolean tryLock(String key, String owner) {
        try {
            return locks.tryAcquire(key, owner, props.leaseDuration());
        } catch (RuntimeException e) {
            log.warn("Redis lock unavailable: {}", e.toString());
            return false;
        }
    }

    private void unlock(String key, String owner) {
        try {
            locks.release(key, owner);
        } catch (RuntimeException e) {
            log.debug("Lock release failed (will expire): {}", e.toString());
        }
    }

    private static String lockKey(ClaimedTask t) {
        return "scheduler:task-lock:" + t.id();
    }

    private String lockOwner(ClaimedTask t) {
        return props.nodeId() + "#" + t.leaseToken();
    }

    private static String describe(Exception e) {
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    private static String truncate(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }
}
