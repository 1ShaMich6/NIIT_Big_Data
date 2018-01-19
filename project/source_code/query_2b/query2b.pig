bag1 = LOAD 'hdfs://localhost:54310/niit_final_project/cleaned_data' USING PigStorage('\t') AS (s_no:chararray, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:double, year:int, worksite:chararray, longitude:double, latitude:double);


bag2 = FOREACH bag1 GENERATE case_status, soc_name, job_title, worksite, year;


certified_bag = FILTER bag2 BY case_status == 'CERTIFIED';


groupby2 = GROUP certified_bag BY (worksite, case_status, year);

countby2 = FOREACH groupby2 GENERATE FLATTEN(group), COUNT(certified_bag) as count;


filterbyyear = FILTER countby2 BY year == $required_year;


orderbycount = ORDER filterbyyear BY count DESC;


max = LIMIT orderbycount 5;


STORE max INTO 'hdfs://localhost:54310/niit_final_project/query_2b';

