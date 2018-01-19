--Batch File 'professioncount.sql'

USE retail;

SET myage = 45;

--SELECT profession, COUNT(*) FROM customer WHERE age >= ${hiveconf:myage} GROUP BY profession ORDER BY profession;

SELECT profession, COUNT(*) FROM customer WHERE age >= 45 GROUP BY profession ORDER BY profession;

--SELECT * FROM customer WHERE age >= ${hiveconf:myage};

