
USE h1b_project;

SELECT worksite, year, COUNT(*) AS app_count FROM h1b_app4 WHERE case_status = 'CERTIFIED' AND job_title LIKE '%DATA ENGINEER%' AND year = ${hiveconf:required_year} GROUP BY worksite, year ORDER BY app_count DESC LIMIT 1;
