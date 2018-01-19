bag1 = LOAD 'hdfs://localhost:54310/niit_final_project/cleaned_data' USING PigStorage('\t') AS (s_no:chararray, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:double, year:int, worksite:chararray, longitude:double, latitude:double);


groupby2 = GROUP bag1 BY (year, employer_name);

countby2 = FOREACH groupby2 GENERATE FLATTEN(group), COUNT(bag1) as count;


orderbycount = ORDER countby2 BY count DESC;

final_bag = LIMIT orderbycount 5;


STORE final_bag INTO 'hdfs://localhost:54310/niit_final_project/query_4';
