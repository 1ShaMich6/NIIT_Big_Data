bag1 = LOAD 'hdfs://localhost:54310/niit_final_project/cleaned_data' USING PigStorage('\t') AS (s_no:chararray, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:double, year:int, worksite:chararray, longitude:double, latitude:double);

------------------------------------------------------

bag2 = FOREACH bag1 GENERATE case_status, job_title;

groupbyjob = GROUP bag2 BY job_title;

totalcountbyjob = FOREACH groupbyjob GENERATE group, COUNT(bag2) as totalcount;

certcountbyjob = FOREACH groupbyjob 
{
ftest = FILTER bag2 BY case_status == 'CERTIFIED';
GENERATE group, COUNT(ftest) AS certified_count;
};

certwithcountbyjob = FOREACH groupbyjob 
{
ftest = FILTER bag2 BY case_status == 'CERTIFIED-WITHDRAWN';
GENERATE group, COUNT(ftest) AS certified_withdrawn_count;
};

withcountbyjob = FOREACH groupbyjob
{
ftest = FILTER bag2 BY case_status == 'WITHDRAWN';
GENERATE group, COUNT(ftest) AS withdrawn_count;
};

deniedcountbyjob = FOREACH groupbyjob 
{
ftest = FILTER bag2 BY case_status == 'DENIED';
GENERATE group, COUNT(ftest) AS denied_count;
};

--------------------------------------------------------

joinbag = JOIN certcountbyjob BY $0, certwithcountbyjob BY $0, withcountbyjob BY $0, deniedcountbyjob BY $0, totalcountbyjob BY $0;

finaljobbag = FOREACH joinbag GENERATE $0, $1, $3, $5, $7, $9;

above1000filter = FILTER finaljobbag BY $5 >= 1000;

---------------------------------------------------------

successrate_bag = FOREACH above1000filter GENERATE $0, $1, $2, $5, ROUND_TO(((double)($1 + $2)*100 / $5), 3) AS success_rate;

above70filter = FILTER successrate_bag BY success_rate > 70.000;


final_bag = ORDER above70filter BY success_rate DESC;

STORE final_bag INTO 'hdfs://localhost:54310/niit_final_project/query_10';

