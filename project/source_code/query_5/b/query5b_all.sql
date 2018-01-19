USE h1b_project;

SELECT year, job_title, COUNT(*) AS app_count FROM h1b_app4 WHERE case_status = 'CERTIFIED' GROUP BY year, job_title ORDER BY app_count DESC LIMIT 10;
