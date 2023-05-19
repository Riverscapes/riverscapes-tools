


``` sql
SELECT
    j.name, t.cpu, t.memory, t.name, t.status,
    -- Calculate the duration of the task in seconds
    round(((t.ended_on - t.started_on) / 1000.0), 0) AS durationS,
    -- Calculate the estimated cost for the task
    round(((t.cpu / 1024.0 * 0.04048) + (t.memory / 1000.0 * 0.004445)) * ((t.ended_on - t.started_on) / 3600000.0), 2) AS estimated_cost
FROM cc_jobs j
JOIN cc_tasks t ON j.jid = t.jid
WHERE j.task_script_id = 'vbet' AND j.task_def_id = 'riverscapesTools';
```