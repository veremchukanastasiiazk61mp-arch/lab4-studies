"""Робота з чергою в PostgreSQL (ЛР4). Усі команди, що змінюють задачу, повертають рядки через RETURNING,
тому кількість оновлених рядків дорівнює кількості повернених. З'єднання працюють в режимі autocommit;
транзакції явні (BEGIN / COMMIT / ROLLBACK)."""
import json
import logging

import psycopg

log = logging.getLogger("worker.db")

# ---- крок 3: наївне захоплення (SELECT, потім окремий UPDATE, без транзакції і без блокувань)
SQL_NAIVE_SELECT = (
    "SELECT id, record_id, seed, attempts, token FROM tasks "
    "WHERE status = 'queued' AND run_after <= now() ORDER BY created_at, id LIMIT 1"
)
SQL_NAIVE_UPDATE = (
    "UPDATE tasks SET status = 'running', worker_id = %s, started_at = now(), attempts = attempts + 1 "
    "WHERE id = %s RETURNING id"
)


def _claim_sql(skip_locked: bool) -> str:
    # крок 4: захоплення однією командою (Д2: задачі з run_after у майбутньому пропускаються)
    return (
        "UPDATE tasks SET status = 'running', worker_id = %s, token = token + 1, "
        "lease_until = now() + make_interval(secs => %s), attempts = attempts + 1, started_at = now() "
        "WHERE id = (SELECT id FROM tasks WHERE status = 'queued' AND run_after <= now() "
        "ORDER BY created_at, id LIMIT 1 FOR UPDATE" + (" SKIP LOCKED" if skip_locked else "") + ") "
        "RETURNING id, record_id, seed, attempts, token"
    )


SQL_REQUEUE_FAILED = (
    "UPDATE tasks SET status = 'failed', worker_id = NULL, finished_at = now() "
    "WHERE status = 'running' AND lease_until < now() AND attempts >= max_attempts RETURNING id"
)
SQL_REQUEUE = (
    "UPDATE tasks SET status = 'queued', worker_id = NULL "
    "WHERE status = 'running' AND lease_until < now() RETURNING id"
)
SQL_HEARTBEAT = (
    "UPDATE tasks SET lease_until = now() + make_interval(secs => %s) "
    "WHERE id = %s AND token = %s RETURNING id"
)
SQL_FINISH = "UPDATE tasks SET status = 'done', finished_at = now() WHERE id = %s AND token = %s RETURNING id"
SQL_RESULT = (
    "INSERT INTO task_results (task_id, token, worker_id, result) VALUES (%s, %s, %s, %s::jsonb) "
    "ON CONFLICT (task_id) DO NOTHING RETURNING task_id"
)
# наївний режим: без токена, без ON CONFLICT (на цьому кроці первинний ключ task_results замінено індексом)
SQL_FINISH_NAIVE = "UPDATE tasks SET status = 'done', finished_at = now() WHERE id = %s RETURNING id"
SQL_RESULT_NAIVE = (
    "INSERT INTO task_results (task_id, token, worker_id, result) VALUES (%s, %s, %s, %s::jsonb) RETURNING task_id"
)
SQL_RELEASE = "UPDATE tasks SET status = 'queued', worker_id = NULL WHERE id = %s AND token = %s RETURNING id"


def connect(s) -> "psycopg.Connection":
    return psycopg.connect(
        host=s.db_host, port=s.db_port, dbname=s.db_name, user=s.db_user, password=s.db_password,
        autocommit=True, connect_timeout=5, application_name=f"worker:{s.worker_id}",
    )


def rows(conn, sql: str, params: tuple = ()) -> list:
    cur = conn.execute(sql, params)
    return cur.fetchall() if cur.description else []


class Queue:
    """Одне з'єднання для основного потоку воркера. Heartbeat має власне з'єднання (див. Heartbeat)."""

    def __init__(self, s):
        self.s = s
        self.conn = None

    def ensure(self):
        if self.conn is None or self.conn.closed:
            self.conn = connect(self.s)
            log.info("з'єднання з базою встановлено")

    def reset(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001
                pass
        self.conn = None

    def claim(self):
        """Повертає (id, record_id, seed, attempts, token) або None, якщо черга порожня."""
        self.ensure()
        s = self.s
        if s.claim_mode == "naive":
            r = rows(self.conn, SQL_NAIVE_SELECT)
            if not r:
                return None
            tid, rec, seed, attempts, token = (int(x) for x in r[0])
            # між SELECT і UPDATE інший воркер міг прочитати той самий рядок
            rows(self.conn, SQL_NAIVE_UPDATE, (s.worker_id, tid))
            return tid, rec, seed, attempts + 1, token
        r = rows(self.conn, _claim_sql(s.claim_mode == "skip_locked"), (s.worker_id, float(s.lease_s)))
        if not r:
            return None
        tid, rec, seed, attempts, token = (int(x) for x in r[0])
        return tid, rec, seed, attempts, token

    def requeue_expired(self):
        """Задачі зі спливлим lease: у failed, якщо attempts досягло max_attempts, інакше назад у чергу."""
        self.ensure()
        failed = rows(self.conn, SQL_REQUEUE_FAILED)
        back = rows(self.conn, SQL_REQUEUE)
        for (tid,) in failed:
            log.warning("[failed_max_attempts] задачу %s переведено у failed: lease спливло, спроби вичерпано", tid)
        for (tid,) in back:
            log.info("[requeued] задачу %s повернуто в чергу: lease спливло", tid)
        return len(back), len(failed)

    def finish(self, task_id: int, token: int, result: dict) -> bool:
        """True, якщо задачу завершено цим воркером; False, якщо токен застарів (нуль оновлених рядків)."""
        self.ensure()
        s = self.s
        payload = json.dumps(result)
        if s.claim_mode == "naive":
            rows(self.conn, SQL_RESULT_NAIVE, (task_id, token, s.worker_id, payload))
            rows(self.conn, SQL_FINISH_NAIVE, (task_id,))
            return True
        self.conn.execute("BEGIN")
        try:
            done = rows(self.conn, SQL_FINISH, (task_id, token))
            if not done:
                self.conn.execute("ROLLBACK")
                log.warning("finish: 0 rows updated, задача %s токен %s (нуль оновлених рядків: токен застарів)",
                            task_id, token)
                return False
            rows(self.conn, SQL_RESULT, (task_id, token, s.worker_id, payload))
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            try:
                self.conn.execute("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            raise

    def release(self, task_id: int, token: int) -> bool:
        self.ensure()
        return bool(rows(self.conn, SQL_RELEASE, (task_id, token)))


class Heartbeat:
    """Один фоновий потік на воркер із власним постійним з'єднанням (нове з'єднання на кожну задачу коштувало б
    дорожче за саму задачу). Поки задача в обробці (set_task), потік кожні L / 3 с виконує команду з умовою за
    токеном; якщо вона оновила нуль рядків, задачу вже забрав інший воркер (lost)."""

    def __init__(self, s):
        import threading
        self.s = s
        self.cond = threading.Condition()
        self.task = None        # (task_id, token) або None
        self.gen = 0            # змінюється під час set_task/clear: таймер L / 3 відлічується від захоплення
        self.lost = False
        self.stopped = False
        self.thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)

    def start(self):
        self.thread.start()

    def set_task(self, task_id: int, token: int):
        with self.cond:
            self.task, self.gen, self.lost = (task_id, token), self.gen + 1, False
            self.cond.notify_all()

    def clear(self):
        with self.cond:
            self.task, self.gen = None, self.gen + 1
            self.cond.notify_all()

    def _run(self):
        s = self.s
        interval = s.lease_s / 3
        conn = None
        while True:
            with self.cond:
                while self.task is None and not self.stopped:
                    self.cond.wait()
                if self.stopped:
                    break
                gen0 = self.gen
                self.cond.wait_for(lambda: self.stopped or self.gen != gen0, timeout=interval)
                if self.stopped:
                    break
                if self.gen != gen0 or self.task is None:
                    continue
                task_id, token = self.task
            try:
                if conn is None or conn.closed:
                    conn = connect(s)
                r = rows(conn, SQL_HEARTBEAT, (float(s.lease_s), task_id, token))
            except Exception as exc:  # noqa: BLE001
                log.warning("heartbeat задачі %s: %r", task_id, exc)
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                conn = None
                continue
            if not r:
                with self.cond:
                    if self.gen == gen0:
                        self.lost = True
                        self.task = None   # більше не продовжуємо чужу задачу
                log.warning("heartbeat: 0 rows updated, задача %s токен %s (нуль оновлених рядків: токен застарів)",
                            task_id, token)
