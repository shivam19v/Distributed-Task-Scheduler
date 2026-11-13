package dev.jobscheduler.lock;

import java.time.Duration;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Minimal single-node-Redis lock: SET NX PX to acquire, owner-checked Lua scripts to renew/release.
 * <p>
 * Locks are an <em>efficiency</em> mechanism (avoid duplicate work, elect a coordinator).
 * Correctness never depends on them: every state transition in PostgreSQL is fenced by a lease token.
 */
@Component
public class RedisLockService {

    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end
            """, Long.class);

    private static final RedisScript<Long> RENEW = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end
            """, Long.class);

    private final StringRedisTemplate redis;

    public RedisLockService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean tryAcquire(String key, String owner, Duration ttl) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, owner, ttl));
    }

    public boolean renew(String key, String owner, Duration ttl) {
        Long r = redis.execute(RENEW, List.of(key), owner, String.valueOf(ttl.toMillis()));
        return r != null && r == 1L;
    }

    public boolean release(String key, String owner) {
        Long r = redis.execute(RELEASE, List.of(key), owner);
        return r != null && r == 1L;
    }
}
