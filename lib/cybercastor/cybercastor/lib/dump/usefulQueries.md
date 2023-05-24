
## Cybercastor Cost Estimates

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

* `SELECT j.name, t.cpu, t.memory, t.name, t.status, ...`: Selecting the relevant columns from the cc_jobs and cc_tasks tables to be included in the result.
* `round(((t.ended_on - t.started_on) / 1000.0), 0) AS durationS`: Calculates the duration of the task in seconds by subtracting started_on from ended_on and dividing by 1000 to convert from milliseconds to seconds. The round() function is used to round the result to 0 decimal places.
* `round(((t.cpu / 1024.0 * 0.04048) + (t.memory / 1000.0 * 0.004445)) * ((t.ended_on - t.started_on) / 3600000.0), 2) AS estimated_cost`: Calculates the estimated cost for the task. Here's the breakdown:

* `(t.cpu / 1024.0 * 0.04048)`: Calculates the cost for the allocated vCPU resources. t.cpu / 1024.0 converts the CPU value from kilobytes to megabytes, and then multiplied by the cost per vCPU per hour.
* `(t.memory / 1000.0 * 0.004445)`: Calculates the cost for the allocated memory resources. t.memory / 1000.0 converts the memory value from kilobytes to gigabytes, and then multiplied by the cost per GB per hour.
* `((t.ended_on - t.started_on) / 3600000.0)`: Calculates the duration of the task in hours by dividing the difference between ended_on and started_on by 3600000 to convert from milliseconds to hours.
* The calculated cost for vCPUs and memory is multiplied by the duration in hours to get the estimated cost for the task. The round() function is used to round the result to 2 decimal places.
* `FROM cc_jobs j JOIN cc_tasks t ON j.jid = t.jid`: Specifies the join between the cc_jobs and cc_tasks tables based on the jid column.
* `WHERE j.task_script_id = 'vbet' AND j.task_def_id = 'riverscapesTools'`: Filters the tasks based on the given conditions, where the task_script_id is 'vbet' and task_def_id is 'riverscapesTools'.

*NOTE: This is fargate only and does not include the tilerizer or S3 transfer costs*

## Processing time for Projects

Show the max, min and avg processing time (in minutes) of a model grouped by owner and project type.

```SQL
SELECT p.owner_by_name, p.project_type_id,
       round(MAX(m.value)/60.0,1) AS max_ProcTimeM,
       round(MIN(m.value)/60.0,1) AS min_ProcTimeM,
       round(AVG(m.value)/60.0,1) AS avg_ProcTimeM
FROM rs_projects p
INNER JOIN rs_project_meta m ON p.pid = m.project_id
WHERE m.key = 'ProcTimeS'
GROUP BY p.owner_by_name, p.project_type_id;
```

## Find duplicate projects

```sql
SELECT
    p.id,  -- Selecting the 'id' column from the 'rs_projects' table
    strftime('%Y-%m-%d %H:%M:%S', datetime(p.created_on / 1000, 'unixepoch')) AS createdDate,  -- Converting the 'created_on' timestamp to a human-readable format
    p.owner_by_name,  -- Selecting the 'owner_by_name' column from the 'rs_projects' table
    p.project_type_id,  -- Selecting the 'project_type_id' column from the 'rs_projects' table
    m.value AS HUC,  -- Selecting the 'value' column from the 'rs_project_meta' table with the 'key' 'HUC' and aliasing it as 'HUC'
    m2.value AS "Model Version",  -- Selecting the 'value' column from the 'rs_project_meta' table with the 'key' 'Model Version' and aliasing it as 'Model Version'
    CASE
        WHEN (p.created_on, p.pid) IN (  -- Checking if the combination of 'created_on' and 'pid' exists in the subquery result
            SELECT MAX(created_on), pid  -- Selecting the maximum 'created_on' value and 'pid' from the 'rs_projects' table
            FROM rs_projects  -- Subquery: selecting from the 'rs_projects' table
            GROUP BY owner_by_name, project_type_id, (
                SELECT value FROM rs_project_meta  -- Subquery: selecting the 'value' from 'rs_project_meta' table
                WHERE project_id = rs_projects.pid AND key = 'HUC'  -- Joining with 'rs_projects' table and filtering by 'pid' and 'HUC' key
            ),
            (
                SELECT value FROM rs_project_meta  -- Subquery: selecting the 'value' from 'rs_project_meta' table
                WHERE project_id = rs_projects.pid AND key = 'Model Version'  -- Joining with 'rs_projects' table and filtering by 'pid' and 'Model Version' key
            )
        ) THEN 'KEEP'  -- If the combination exists, assign 'KEEP' to the 'status' column
        ELSE 'DELETE'  -- If the combination doesn't exist, assign 'DELETE' to the 'status' column
    END AS status  -- Naming the CASE statement result column as 'status'
FROM rs_projects p  -- Selecting from the 'rs_projects' table and aliasing it as 'p'
JOIN rs_project_meta m ON p.pid = m.project_id AND m.key = 'HUC'  -- Joining the 'rs_projects' table with the 'rs_project_meta' table on 'pid' and 'HUC' key
JOIN rs_project_meta m2 ON p.pid = m2.project_id AND m2.key = 'Model Version'  -- Joining the 'rs_projects' table with the 'rs_project_meta' table on 'pid' and 'Model Version' key
ORDER BY HUC, project_type_id, created_on DESC;  -- Sorting the result by 'HUC' in ascending order, then by 'project_type_id' in ascending order, and finally by 'created_on' in descending order
```


### Find HUC10 Geoms with corresponding Projects


This one is a work in progress... Better, faster query coming soon!

```sql
SELECT H.HUC10, M.key, m.value, P.project_type_id, P.id, P.owner_by_name,
       CASE WHEN M.value IS NOT NULL THEN 1 ELSE 0 END AS has_corresponding_row
FROM Huc10_conus H
LEFT JOIN rs_project_meta M on M.value = H.HUC10
LEFT JOIN rs_projects P on M.project_id = P.pid
WHERE has_corresponding_row = 1
  AND M.key = 'HUC'
  AND P.project_type_id = 'rscontext'
```