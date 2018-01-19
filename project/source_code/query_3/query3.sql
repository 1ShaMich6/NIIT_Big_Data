
USE h1b_project;

SELECT soc_name, COUNT(*) AS app_count FROM h1b_app4 WHERE case_status = 'CERTIFIED' AND job_title LIKE '%DATA SCIENTIST%' GROUP BY soc_name ORDER BY app_count DESC LIMIT 1;
