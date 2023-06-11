select b.id, b.department, count(1) q
from bronze.hired_employees a
left join bronze.departments b on a.department_id = b.id
left join bronze.jobs c on a.job_id = c.id
group by 1,2
having count(1) >= (select avg(a.q)
from (
select department_id, count(1) q from bronze.hired_employees a where DATE_TRUNC('year', a.datetime::date) = DATE_TRUNC('year', '2021-01-01'::date)
group by 1) a )
order by count(1) desc,1