# Distributed Job Scheduler

DAG-aware distributed job scheduler. **Java 25 · Spring Boot 4.1 · PostgreSQL 18 · Redis 8 · Docker.**
Run any number of identical nodes; each one serves the API, executes tasks, and competes to be the coordinator.

## Run it

```bash
docker compose up --build            # postgres + redis + 3 scheduler nodes (:8081 :8082 :8083)

# submit a diamond DAG (extract -> transform-a/transform-b -> load)
curl -s -X POST localhost:8081/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d @examples/diamond-job.json

curl -s localhost:8082/api/v1/jobs/<id> | jq        # any node can answer
curl -s localhost:8083/api/v1/jobs/<id>/events | jq # audit trail: who ran what, retries, lease expiry
```

**Kill a worker mid-task** (the headline feature):
```bash
# submit a job with {"type":"sleep","params":{"seconds":60}} and see which node owns it
docker kill job-scheduler-scheduler-2-1
# ~30s later (lease expiry) the task shows LEASE_EXPIRED_REASSIGNED and finishes on another node
```

Local dev: `docker compose up postgres redis`, then `mvn spring-boot:run`.
Tests: `mvn verify` (integration tests use Testcontainers, so Docker must be running).
Metrics: `/actuator/prometheus` (`scheduler_task_duration`, `scheduler_tasks_ready`, `scheduler_leader`, ...).

## API

| Method | Path | |
|---|---|---|
| POST | `/api/v1/jobs` | submit a DAG (`Idempotency-Key` header supported, optional `runAt`) |
| GET | `/api/v1/jobs?status=&limit=` | list |
| GET | `/api/v1/jobs/{id}` | job + per-task status, attempts, owner, errors |
| GET | `/api/v1/jobs/{id}/events` | event log |
| POST | `/api/v1/jobs/{id}/cancel` | cancels pending and running tasks |
| POST | `/api/v1/jobs/{id}/retry` | re-run failed/skipped/cancelled tasks, keep succeeded ones |

Task fields: `name, type, params, dependsOn[], maxAttempts (3), backoffSeconds (5, exponential), priority`.
Built-in types: `log`, `sleep`, `flaky` (retry demo), `http`. Add your own by implementing `TaskHandler` as a Spring bean.

## How it works

```
 client ─► any node ─► PostgreSQL (source of truth)
                         jobs / tasks / task_dependencies / task_events
 every node:
   Worker       claim READY task  ──► Redis lock ──► run handler ──► fenced complete/fail
                heartbeat extends DB lease + Redis lock; lease lost => handler interrupted
   Coordinator  (only the Redis-elected leader) reap expired leases ► skip blocked ► promote ready ► roll up jobs
```

* **Dependency-aware execution** – `PENDING -> READY` happens in one SQL statement: all upstream tasks `SUCCEEDED`.
  Failed upstream => downstream `SKIPPED`. Cycles / unknown deps rejected at submit (Kahn's algorithm).
* **Claiming** – `SELECT ... FOR UPDATE SKIP LOCKED` inside a single `UPDATE ... RETURNING`: workers never block each other
  and never double-claim, with no Redis involved.
* **Leases & recovery** – a claim sets `lease_expires_at`; workers heartbeat. The leader's reaper re-queues (or fails,
  if attempts are exhausted) any `RUNNING` task whose lease expired.
* **Fencing tokens** – `lease_token` is bumped on every claim and checked on every worker write. A zombie worker (GC pause,
  partition) that wakes up late cannot overwrite the new owner's result. This is why correctness doesn't hinge on Redis timing.
* **Redis's role** – leader election for the coordinator, and a per-task execution lock as a second guard. If Redis is down,
  workers stop starting tasks (safe), and nothing is lost; work resumes when it's back.
* **Retries** – exponential backoff (`backoffSeconds * 2^(attempt-1)`, capped at 1h); handlers can throw
  `NonRetryableException` to fail fast.
* **Graceful shutdown** – stop claiming, drain in-flight tasks, hand back the rest *without* burning an attempt.
* **Delivery is at-least-once** – handlers should be idempotent.

## Beyond the original resume bullets (additions)

`FOR UPDATE SKIP LOCKED` claiming, fencing tokens, leader-elected coordinator, idempotent submission, scheduled start (`runAt`),
priorities, exponential backoff, job cancel/retry, audit event log, virtual-thread workers, Prometheus metrics, Flyway migrations,
Testcontainers tests (diamond DAG, retries, skip propagation, dead-worker recovery, fencing, cancellation).

## Known limits / next steps

* Not compiled in the environment this was generated in (no Maven Central access). Run `mvn verify` once and fix any
  small API drift; versions are pinned in `pom.xml` (bump Boot to the latest 4.1.x/4.2 patch).
* `http` task type is SSRF-prone: set `scheduler.http-allowed-hosts` before exposing the API.
* No authentication on the API; add Spring Security before real use.
* Single Redis node (not Redlock) is intentional since fencing covers correctness. Cron/recurring jobs, task-to-task result passing,
  per-queue worker pools and a small UI are natural next steps.
* Old `task_events`/finished jobs are never purged; add a retention job.
