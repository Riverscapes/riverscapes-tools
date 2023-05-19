
## Cybercastor Cost Estimates

* `SELECT j.name, t.cpu, t.memory, t.name, t.status, ...`: Selecting the relevant columns from the cc_jobs and cc_tasks tables to be included in the result.
* `round(((t.ended_on - t.started_on) / 1000.0), 0) AS durationS`: Calculates the duration of the task in seconds by subtracting started_on from ended_on and dividing by 1000 to convert from milliseconds to seconds. The round() function is used to round the result to 0 decimal places.
* `round(((t.cpu / 1024.0 * 0.04048) + (t.memory / 1000.0 * 0.004445)) * ((t.ended_on - t.started_on) / 3600000.0), 2) AS estimated_cost`: Calculates the estimated cost for the task. Here's the breakdown:

* `(t.cpu / 1024.0 * 0.04048)`: Calculates the cost for the allocated vCPU resources. t.cpu / 1024.0 converts the CPU value from kilobytes to megabytes, and then multiplied by the cost per vCPU per hour.
* `(t.memory / 1000.0 * 0.004445)`: Calculates the cost for the allocated memory resources. t.memory / 1000.0 converts the memory value from kilobytes to gigabytes, and then multiplied by the cost per GB per hour.
* `((t.ended_on - t.started_on) / 3600000.0)`: Calculates the duration of the task in hours by dividing the difference between ended_on and started_on by 3600000 to convert from milliseconds to hours.
* The calculated cost for vCPUs and memory is multiplied by the duration in hours to get the estimated cost for the task. The round() function is used to round the result to 2 decimal places.
* `FROM cc_jobs j JOIN cc_tasks t ON j.jid = t.jid`: Specifies the join between the cc_jobs and cc_tasks tables based on the jid column.

WHERE j.task_script_id = 'vbet' AND j.task_def_id = 'riverscapesTools': Filters the tasks based on the given conditions, where the task_script_id is 'vbet' and task_def_id is 'riverscapesTools'.

``` sql
SELECT
    j.name, t.cpu, t.memory, t.name, t.status,
    -- Calculate the duration of the task in seconds
    round(((t.ended_on - t.started_on) / 1000.0), 0) AS durationS,
    -- Calculate the estimated cost for the task
    round(((t.cpu / 1024.0 * 0.04048) + (t.memory / 1000.0 * 0.004445)) * ((t.ended_on - t.started_on) / 3600000.0), 2) AS estimated_cost
FROM cc_jobs j
JOIN cc_tasks t ON j.jid = t.jid
-- Filter the tasks based on the given conditions
WHERE j.task_script_id = 'vbet' AND j.task_def_id = 'riverscapesTools';
```