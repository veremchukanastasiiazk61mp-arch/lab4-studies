-- ЛР4, крок 4: повернення первинного ключа task_results 
BEGIN;
TRUNCATE task_results;
DROP INDEX IF EXISTS task_results_task_idx;
ALTER TABLE task_results DROP CONSTRAINT IF EXISTS task_results_pkey;
ALTER TABLE task_results ADD CONSTRAINT task_results_pkey PRIMARY KEY (task_id);
COMMIT;
