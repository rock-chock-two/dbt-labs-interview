/*
Intro, set up

I've provided all the logic and calculations in a single query file because I wanted to organize my thoughts linearly.

Also, the imported csv data is immutable, an ad-hoc query could work fine and would require less effort than a couple of dbt models.
Such approach also allows to easily modify and share the single file.

For visualization tool I've used Google Data Studio as it easily integrates both with Google Spreadsheets and with Big Query,
it's free and it allows to easily share the results.

I've used Google Spreadsheets to store results of some CTEs because it improves performance for Google Data Studio 
(compared to Big Query) and allows to save Google Cloud resources for free tier users.

However, I could also organize this data in a dbt project with Google BigQuery to remove some repetitive code,
to better organize and document the query, and to test tables more easily.

Resources:
- Query at Google BigQuery
https://console.cloud.google.com/bigquery?sq=1036490985404:2b7f44109d4a4740b59756714c89949c
- Aggregated data at Google Spreadsheet
https://docs.google.com/spreadsheets/d/1ToQS2-o-xP-YtwsDvd-MDgQDYsRAA7q4iZGVPAEXTOw/edit?usp=sharing
- Visualization
https://datastudio.google.com/reporting/861f9c0f-5663-4c5b-8cbf-9dc871fd49c6

*/

-- Analysis

/*
To better understand the dataset, we can start with checking the stats aggregated per multiple levels:
- 1. overall dataset stats
- 2. stats aggregated per account level
- 3. stats aggregated per job definition level
*/
with base as (
    select 
        job_definition_id,
        run_id,
        account_id,
        status as run_status,
        started_at,
        finished_at,
        created_at,
        updated_at
    -- imported csv
    from `dbt-project-347923.data_analysis.job_details`
),
base_with_run_sequence as (
    select
        count(*) over (partition by job_definition_id) as runs_in_job,
        row_number() over (partition by job_definition_id order by started_at asc) as run_sequence,
        *
    from base

),

-- 1. overall dataset stats

-- Check distinct counts of the dataset and basic stats
/*
Results: 
The job in the dataset were run from 2020-02-21 till 2022-03-30
The majority of accounts have only 1 job definition, and only 1 account has 2 jobs. 
Therefore, aggregating per account level is redundant 
as the results of grouping by account vs by job will be similar.

Visualization: Page 2 of Google Data Studio report
*/
dataset_basic_stats AS (
    select 
        count(*) as records,
        count(distinct run_id) as runs,
        count(distinct job_definition_id) as job_definitions,
        count(distinct account_id) as accounts,

        -- run  status
        count(distinct run_status) as run_statuses,
        string_agg(distinct run_status) as unique_run_statuses,
        sum(if (run_status = 'error', 1, 0)) as runs_with_error,
        sum(if (run_status = 'complete', 1, 0)) as runs_complete,
        sum(if (run_status = 'cancelled', 1, 0)) as runs_canceled,

        -- dates details
        date_diff(max(started_at), min(started_at), day) as period_in_days,
        min(started_at) as min_started_at,
        max(started_at) as max_started_at,
        min(finished_at) as min_finished_at,
        max(finished_at) as max_finished_at,
        min(created_at) as min_created_at,
        max(created_at) as max_created_at,
        min(updated_at) as min_updated_at,
        max(updated_at) as max_updated_at

    from base
),

-- check data quality
/*
I'd rather use dbt tests for simplicity, 
but I wanted to keep steps in the linear representation and be able to visualize the results of data quelity 

Visualization: page 2 of Google Data Studio report
*/
dataset_quality AS (
    -- check for nulls
    (
        select
            'job_definition_id is null' as condition_name,
            sum(if(job_definition_id is null, 1, 0)) as records_count

        from base
    )
    union all 
    (
        select
            'run_id is null' as condition_name,
            sum(if(run_id is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'account_id is null' as condition_name,
            sum(if(account_id is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'run_status is null' as condition_name,
            sum(if(run_status is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'started_at is null' as condition_name,
            sum(if(started_at is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'finished_at is null' as condition_name,
            sum(if(finished_at is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'created_at is null' as condition_name,
            sum(if(created_at is null, 1, 0)) as records_count

        from base
    )
    union all
    (
        select
            'updated_at is null' as condition_name,
            sum(if(updated_at is null, 1, 0)) as records_count

        from base
    )
    union all
    -- check for unique values
    (
        select 
            'run_id is not unique' as condition_name,
            if(
                count(run_id)!= count(distinct run_id), 
                count(run_id) - count(distinct run_id),
                0
            ) as records_count

        from base
    )
    union all 
    -- custom test
    (
        -- check if consequent job's runs overlap: e.g. the new run started before the current run finished
        select
            'job runs overlap' as condition_name,
            sum(
                if(base_with_run_sequence.finished_at > base2.started_at, 1, 0)
            ) as records_count

        from base_with_run_sequence
        left join base_with_run_sequence AS base2
            on (
                base_with_run_sequence.job_definition_id = base2.job_definition_id
                and base_with_run_sequence.run_sequence = base2.run_sequence-1
                )
    )
),

-- 2. stats aggregated per account level
-- (Skipped aggregation because it's redundant)

-- Check all records for the unique account that has 2 associated jobs
runs_from_account_with_2_jobs as (
    select 
        *
    from base
    qualify 
        count(distinct job_definition_id) over (partition by account_id) > 1
),

-- 3. stats per job definition

/* get last job run details per each job:
- find max started at
- inner join to the base table on job id and started at
*/
max_run_started as (
    select 
        job_definition_id,
        max(started_at) as max_run_started_at

    from base
    group by 1
),
last_run_details as (
    select 
        base.job_definition_id,
        base.started_at as last_run_started_at,
        base.finished_at as last_run_finished_at,
        date_diff(base.finished_at, base.started_at, minute) as last_run_duration_mins,
        base.run_status as last_run_status

    from base 
    inner join max_run_started
        on (base.job_definition_id = max_run_started.job_definition_id
            and base.started_at = max_run_started.max_run_started_at)
),
-- aggregation per job
job_stats_helper as (
    select 
        job_definition_id,
        string_agg(distinct account_id) as account_ids,
        -- ensure that this job definition is connected only to a single account
        count(distinct account_id) as accounts, 
        count(run_id) as runs,
        
        -- runs per status
        string_agg(distinct run_status) as unique_run_statuses,
        sum(if (run_status = 'complete', 1, 0)) as runs_complete,
        sum(if (run_status = 'cancelled', 1, 0)) as runs_canceled,
        sum(if (run_status = 'error', 1, 0)) as runs_with_error,
        round( 
            (
                sum(if (run_status = 'error', 1, 0)) /
                count(run_id)
            ), 
            2
        ) as runs_with_error_rate,

        -- dates & date differences
        date_diff(max(started_at), min(started_at), day) as job_active_days,
        min(started_at) as min_started_at,
        min(finished_at) as min_finished_at,
        max(started_at) as last_run_started_at,
        max(finished_at) as last_run_finished_at,

        -- last 3 runs aggregated details
        string_agg(run_status order by started_at desc limit 3) as last_3_runs_statuses,
        string_agg(string(date(started_at)) order by  started_at desc limit 3) as last_3_runs_started
        
    from base
    group by 1
),

job_stats_joined_with_last_run as (
    select 
        job_stats_helper.*,
        
        -- last run details
        last_run_details.last_run_status,
        last_run_details.last_run_duration_mins,

        percentile_cont(last_run_details.last_run_duration_mins, 0.25) over () as last_run_duration_per_job_25_pctl,
        percentile_cont(last_run_details.last_run_duration_mins, 0.5) over () as last_run_duration_per_job_median,
        percentile_cont(last_run_details.last_run_duration_mins, 0.75) over () as last_run_duration_per_job_75_pctl,

        -- percentiles & max of number of runs with errors per job_id
        percentile_cont(runs_with_error, 0.25) over () as runs_with_error_per_job_25_pctl,
        percentile_cont(runs_with_error, 0.5) over () as runs_with_error_per_job_median,
        percentile_cont(runs_with_error, 0.75) over () as runs_with_error_per_job_75_pctl,
        max(runs_with_error) over() as runs_with_error_per_job_max,

        -- percentiles & max number of runs per job_id
        percentile_cont(runs, 0.25) over () as runs_per_job_25_pctl,
        percentile_cont(runs, 0.5) over () as runs_per_job_median,
        percentile_cont(runs, 0.75) over () as runs_per_job_75_pctl,
        max(runs) over() as runs_per_job_max

    from job_stats_helper
    left join last_run_details
        on job_stats_helper.job_definition_id = last_run_details.job_definition_id
),

/*
Results: 
I've created multiple data points, converted them to flags and tried to find dependencies.
Decided that the best solution is the one with the simplest rules.

- We identify a job as abandoned if it’s last 3 runs have status "error" and if it ran in last month

Multiple data points: job_stats sheet of the Google Sheet
https://docs.google.com/spreadsheets/d/1ToQS2-o-xP-YtwsDvd-MDgQDYsRAA7q4iZGVPAEXTOw/edit#gid=1822760964&range=A2
Visualization: 
page 1 of Google Data Studio report
*/

job_stats_with_flags as (
    select 
        -- we identify a job as abandoned if it’s last 3 runs have status "error" and if it ran in last month
        if(
            (
                last_3_runs_statuses = 'error,error,error'
                and date(last_run_started_at) between date_sub('2022-03-30', interval 1 month) and '2022-03-30'
            ), 
            1, 
            0
        ) as is_abandoned,

        *,

        -- flags for monitoring jobs: possibly abandoned, taking a lot of resources
        if(unique_run_statuses = 'error', 1, 0) as are_all_runs_with_error,
        if(last_3_runs_statuses = 'error,error,error', 1, 0) as are_last_3_runs_with_errors,
        if(last_run_status = 'error', 1, 0) as is_last_run_with_error,
        if(
            date(last_run_started_at) between date_sub('2022-03-30', interval 1 month) and '2022-03-30', 
            1, 
            0
        ) as is_last_run_in_last_month,
        if(
            date(last_run_started_at) between date_sub('2022-03-30', interval 6 month) and '2022-03-30', 
            1, 
            0
        ) as is_last_run_in_last_6_months,
        if(last_run_duration_mins > last_run_duration_per_job_75_pctl, 1, 0) as is_last_run_longer_than_75_pctl_run_duration,
        if(runs_with_error > runs_with_error_per_job_75_pctl, 1, 0) as has_more_than_75_pctl_runs_with_error

    from job_stats_joined_with_last_run
    order by
        is_abandoned desc,
        is_last_run_with_error desc,
        last_run_started_at desc,
        last_run_duration_mins desc
),

-- check all records for runs sequence of abandoned jobs
abandoned_job_runs as (
    select 
        *

    from base_with_run_sequence
    where job_definition_id IN (
            
            -- abandoned jobs, they have 3 last runs with error and were run this months
            -- details show that these jobs have more than 3 runs that have status "error"
            
            'd9d8c0620d14c209c85472f25dd6fd4d',
            '6ed94d74189ebcdb131007c4babee442',
            'ef9bcc6541b9b7302d6e152b4da645e0'


            -- last 3 runs with errors, but were not run last month: most probably they are already turned off. 
            /*
            'eeccf3dcdfa0ee5bd3a283e146b06626',
            '0e44b9cabecbb5af04558a5c49a5d51e',
            'edef0d9bd8c8efab754d2fabd3105338'
            */
            

            -- last runs with errors, but less than 3 runs - probably jobs that only had manual runs or that were disabled, not abandoned  
            /*
            '6eb291eedcb0b82a3ee3ddea97598a7c',
            '70c549b28626adf8c0d6349053a844af',
            '43b5c2f6a840417e79ed8bb3feeeeaf5'
            */
        )
    order by 
        job_definition_id,
        started_at desc
)

select 
    *
from job_stats_with_flags
    
/* 
CTEs:
    -- base
    -- base_with_run_sequence
    -- dataset_basic_stats
    -- dataset_quality
    -- runs_from_account_with_2_jobs
    -- last_run_details
    -- job_stats_with_flags
    -- abandoned_job_runs
*/

-- Answering questions
/*
a. What are the characteristics of an abandoned job?

- It's last 3 runs' statuses are "error"
- It was run last month, meaning it consumed resources recently

If a job only has 2 runs that resulted in error, it's not yet abandoned and will become abandoned on the next run if it finishes with status error.

If a job has last 2 runs with errors and 1 canceled/ complete, it's not abandoned until the third consequent error.
Status "canceled" might mean active user's participation in resolving the issue.

If a job with 3 consequently started runs that did not run this month, then it's not abandoned because it does not 
require resources and it's probably already turned off. 

If a job with 3 runs with errors that ran this month is already turned off, then turning it off again won't hurt much.

However, it would be helpful to get job's current status (cron enabled vs manual trigger), schedule info, 
and also if run was triggered manually or automatically. 
This would allow to turn off only the currently enabled jobs and get more insights about schedule because
custom cron jobs can be set to run once in 6 months, yearly, etc.


Possible adjustments to the concept of abandoned job: 

- consider all jobs that had 3 or less runs with errors as abandoned
Turning off already turned off jobs won't affect customer's experience, but since we don't have enough data 
on job's current status, this approach will ensure that these runs won't unexpectedly run because of their schedule.

- consider widening the time period window to 6 months / one year or completely removing the time period condition
Since we don't have information about schedule nor job's current status, the cron can be set to run once in 6 months or less frequently
and still result in errors. 
It means that the resources might still be consumed unnecessarily, and the current restrictions won't necessarily 
classify such jobs as abandoned.


b. What is the threshold that we want to use to cut off abandoned jobs? Why?

- I'd say that last 3 jobs could be a sufficient threshold to cut off abandoned jobs. 

The first erroneous run can be caused by a glitch.
The second run can correct the first one.
If the second run results into the error, then we can give it a second try.
If the third run is also unsuccessful, then we can safely turn off the job 
and notify account representatives.

For notifying accout representatives, I'd advocate for both sending an email 
and showing an in-app banner with a link to the job that was turned off. 
I'd also consider sending reminder notifications to users who have not dismissed such banner.

The time threshold is subject to further discussion, but I'd start off with disabling qualifying jobs
that ran in current month because jobs running regularly and failing are consuming significantly more resources 
than failing jobs that run once or twice a year.


c. If you wanted to take the analysis a step further, what information would you
want the analytics engineer to bring into this table? What other datasets would
you want to add to enrich the current analysis?

- I'd want to have data on the job's current schedule:
- is the job enabled to run via a manual trigger vs cron job
- if cron, what is the current schedule

If available, I would also check summary of run results (e.g. PASS 2, ERRORS 2, WARN 0, SKIP 10, TOTAL 14, etc). 
If it's in JSON format, I'd try to parse it to separate columns.

I could also check what commands are used in job.
(e.g. I could think further of how to improve user experience if multiple accounts have dbt run command complete, 
but dbt test constantly providing errors)

I'd also like to check historical numbers of resources spent per each run.
It would allows me to set monitoring for jobs that inefficiently consume a lot of resources 
and I'd consider reaching out customers that might need help optimizing the project.

Maybe the solution to help them optimize the query could be as simple as changing 
to incremental materialization,
changing a view to table materialization, removing delete/insert/update statements, 
optimizing the number of threads, replacing subqueries with CTEs or materialized models


d. How would your approach change if the dataset was 100x?

- Select jobs that ran last day/week/month
- Take last 3/5/10 jobs from those that ran this month
- Materialize complex transformations as a table to improve efficiency of dependent queries
- Move some window functions (e.g. percentiles) to separate CTEs and join them to jobs aggregation

*/