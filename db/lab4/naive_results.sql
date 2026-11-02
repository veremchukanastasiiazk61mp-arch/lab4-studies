-- ЛР4, крок 3: на час наївного захоплення первинний ключ task_results замінюється звичайним індексом,
-- щоб дублікати результатів потрапляли в таблицю і їх можна було порахувати.
BEGIN;
TRUNCATE task_results;
ALTER TABLE task_results DROP CONSTRAINT IF EXISTS task_results_pkey;
CREATE INDEX IF NOT EXISTS task_results_task_idx ON task_results (task_id);
COMMIT;
