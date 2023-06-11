select b.department,c.job,
SUM(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-01-01'::date) THEN 1 ELSE 0 END) AS Q1,
SUM(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-04-01'::date) THEN 1 ELSE 0 END) AS Q2,
SUM(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-07-01'::date) THEN 1 ELSE 0 END) AS Q3,
SUM(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-10-01'::date) THEN 1 ELSE 0 END) AS Q4

from bronze.hired_employees a
left join bronze.departments b on a.department_id = b.id
left join bronze.jobs c on a.job_id = c.id
WHERE DATE_TRUNC('year', a.datetime::date) = DATE_TRUNC('year', '2021-01-01'::date)
group by 1,2 order by 1,2