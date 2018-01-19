--QUERY: To calculate the average of medical claims per user



medical_bag = LOAD 'medical' AS (name, department, claim_amount:double);

--describe medical_bag;
--dump medical_bag;


groupbyname = GROUP medical_bag BY name;

--describe groupbyname;
--dump groupbyname;



avgclaim = FOREACH groupbyname GENERATE group, ROUND_TO(AVG(medical_bag.claim_amount), 2) as average;

--dump avgclaim;

STORE avgclaim INTO 'medical_output';



