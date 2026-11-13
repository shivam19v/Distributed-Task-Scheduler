package dev.jobscheduler.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.jobscheduler.config.SchedulerProperties;
import dev.jobscheduler.lock.RedisLockService;

/**
 * Redis-based leader election for the coordinator loop (promote / skip / reap / roll-up).
 * Safe to lose: the coordinator SQL is idempotent, so a brief double-leader window is harmless.
 */
@Component
public class LeaderElector {

    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);
    private static final String KEY = "scheduler:leader";

    private final RedisLockService locks;
    private final SchedulerProperties props;
    private volatile boolean leader;

    public LeaderElector(RedisLockService locks, SchedulerProperties props) {
        this.locks = locks;
        this.props = props;
    }

    public boolean tryLead() {
        boolean now;
        try {
            now = locks.renew(KEY, props.nodeId(), props.leaderTtl())
                    || locks.tryAcquire(KEY, props.nodeId(), props.leaderTtl());
        } catch (RuntimeException e) {
            log.warn("Leader election failed (Redis unavailable?): {}", e.toString());
            now = false;
        }
        if (now != leader) {
            log.info("Node {} {}", props.nodeId(), now ? "became LEADER" : "lost leadership");
        }
        leader = now;
        return now;
    }

    public boolean isLeader() {
        return leader;
    }

    public void resign() {
        try {
            if (leader) {
                locks.release(KEY, props.nodeId());
            }
        } catch (RuntimeException e) {
            log.debug("Resign failed: {}", e.toString());
        }
        leader = false;
    }
}
