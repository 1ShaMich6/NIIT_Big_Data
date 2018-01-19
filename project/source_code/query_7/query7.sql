USE h1b_project;

SELECT year, COUNT(*) FROM h1b_app4 GROUP BY year ORDER BY year;
