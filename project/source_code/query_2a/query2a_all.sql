
USE h1b_project;

SELECT worksite, COUNT(*) AS app_count FROM h1b_app4 WHERE case_status = 'CERTIFIED' AND job_title LIKE '%DATA ENGINEER%' GROUP BY worksite ORDER BY app_count DESC LIMIT 1;
