create table jobs (
    id              uuid primary key,
    name            text        not null,
    status          text        not null check (status in ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    idempotency_key text unique,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    finished_at     timestamptz
);
create index jobs_status_idx on jobs (status, created_at desc);

create table tasks (
    id               uuid primary key,
    job_id           uuid        not null references jobs (id) on delete cascade,
    ord              int         not null default 0,
    name             text        not null,
    type             text        not null,
    params           jsonb       not null default '{}'::jsonb,
    status           text        not null check (status in
                       ('PENDING','READY','RUNNING','SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
    priority         int         not null default 0,
    attempt          int         not null default 0,
    max_attempts     int         not null default 3,
    backoff_seconds  int         not null default 5,
    not_before       timestamptz not null default now(),
    -- lease + fencing
    lease_owner      text,
    lease_token      bigint      not null default 0,
    lease_expires_at timestamptz,
    result           text,
    error            text,
    started_at       timestamptz,
    finished_at      timestamptz,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    unique (job_id, name)
);
-- hot paths: worker claim, lease reaper, DAG promotion
create index tasks_ready_idx   on tasks (priority desc, created_at) where status = 'READY';
create index tasks_lease_idx   on tasks (lease_expires_at)          where status = 'RUNNING';
create index tasks_pending_idx on tasks (job_id)                    where status = 'PENDING';
create index tasks_job_idx     on tasks (job_id, ord);

create table task_dependencies (
    task_id            uuid not null references tasks (id) on delete cascade,
    depends_on_task_id uuid not null references tasks (id) on delete cascade,
    primary key (task_id, depends_on_task_id),
    check (task_id <> depends_on_task_id)
);
create index task_deps_reverse_idx on task_dependencies (depends_on_task_id);

create table task_events (
    id      bigint generated always as identity primary key,
    task_id uuid        not null,
    job_id  uuid        not null,
    event   text        not null,
    worker  text,
    detail  text,
    at      timestamptz not null default now()
);
create index task_events_job_idx on task_events (job_id, id);
