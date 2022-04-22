-- initial check: select and display all columns
SELECT pub_date, job_id, salary_range, technology, salary_lower_bound, salary_upper_bound, salary_avg, request_date
FROM public.dev_jobs_1;

-- get data points with tech tags missing
SELECT pub_date, job_id, salary_range, technology, salary_lower_bound, salary_upper_bound, salary_avg, request_date
FROM public.dev_jobs_1
where technology='';

-- select current ditinct tech tags
SELECT distinct technology
FROM public.dev_jobs_1;

-- count current distinct tech tags
SELECT count(distinct technology)
FROM public.dev_jobs_1;

-- ranking of tech tags existing in db
select technology, count(*) as num
from public.dev_jobs_1
group by technology
order by num desc;

-- checking aws lambda functionality
select count(technology)
from public.dev_jobs_1

select count(distinct job_id)
from public.dev_jobs_1;

-- for initial db load only
select *
from public.dev_jobs_1
where pub_date = '03.04.2022';

-- check how many times request was proceeded
SELECT distinct request_date
FROM public.dev_jobs_1;


select count(*) from public.dev_jobs_1 dj 
where request_date::date = date '2022-04-13'

-- for initial db load only (clean up)
-- DELETE FROM public.dev_jobs_1 WHERE pub_date = '29.03.2022';

-- delete table
-- drop table public.dev_jobs_1;

--delete from public.dev_jobs_1
--where request_date::date = date '2022-04-09';