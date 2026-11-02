-- Схема накочується автоматично під час першого старту бази на порожньому томі
-- (docker-entrypoint-initdb.d). Повторний запуск скрипта безпечний.

CREATE TABLE IF NOT EXISTS studies (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_code       text         NOT NULL CHECK (char_length(patient_code) BETWEEN 1 AND 64),
    modality           text         NOT NULL CHECK (char_length(modality) BETWEEN 1 AND 16),
    slice_count        integer      NOT NULL CHECK (slice_count > 0),
    slice_thickness_mm numeric(5,3) NOT NULL CHECK (slice_thickness_mm BETWEEN 0.5 AND 10),
    device             text         NOT NULL CHECK (char_length(device) BETWEEN 1 AND 128),
    study_date         date         NOT NULL,
    created_at         timestamptz  NOT NULL DEFAULT now(),
    updated_at         timestamptz  NOT NULL DEFAULT now()
);

-- Фільтр за модальністю + стабільне сортування за id для пагінації
CREATE INDEX IF NOT EXISTS studies_modality_id_idx ON studies (modality, id);


-- ЛР4, крок 1: черга задач обробки і результати. Скрипт ідемпотентний (міграція ЛР3 запускається повторно).
-- status: queued -> running -> done | failed. token збільшується на 1 під час кожного захоплення (fencing token).
-- run_after потрібен для Д2: задачі зі значенням у майбутньому не захоплюються.
CREATE TABLE IF NOT EXISTS tasks (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id    bigint      NOT NULL REFERENCES studies (id) ON DELETE CASCADE,
    status       text        NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done', 'failed')),
    seed         integer     NOT NULL CHECK (seed >= 0),
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 3,
    token        bigint      NOT NULL DEFAULT 0,
    lease_until  timestamptz,
    worker_id    text,
    run_after    timestamptz NOT NULL DEFAULT now(),
    created_at   timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    finished_at  timestamptz
);
CREATE INDEX IF NOT EXISTS tasks_status_created_idx ON tasks (status, created_at);

-- Первинний ключ task_id робить запис результату ідемпотентним (INSERT ... ON CONFLICT DO NOTHING).
CREATE TABLE IF NOT EXISTS task_results (
    task_id    bigint      NOT NULL PRIMARY KEY,
    token      bigint      NOT NULL,
    worker_id  text        NOT NULL,
    result     jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
