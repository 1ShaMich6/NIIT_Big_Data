logbag = LOAD 'hadoop-hduser-datanode-ubuntu.log' USING TextLoader() AS (line:chararray);

--dump logbag;


--grouplog = GROUP logbag ALL;

--totallog = FOREACH grouplog GENERATE 'TOTAL', COUNT(logbag.line) as total;

--dump totallog;


SPLIT logbag INTO loginfo IF SUBSTRING(line, 24, 28) == 'INFO', logerr IF SUBSTRING(line, 24, 29) == 'ERROR', logwarn IF SUBSTRING(line, 24, 28) == 'WARN';

--dump loginfo;
--dump logerr;
--dump logwarn;


grouploginfo = GROUP loginfo ALL;
grouplogerr = GROUP logerr ALL;
grouplogwarn = GROUP logwarn ALL;


countloginfo = FOREACH grouploginfo GENERATE 'INFO', COUNT(loginfo);
countlogerr = FOREACH grouplogerr GENERATE 'ERROR', COUNT(logerr);
countlogwarn = FOREACH grouplogwarn GENERATE 'WARN', COUNT(logwarn);


--dump countloginfo;
--dump countlogerr;
--dump countlogwarn;


result1 = UNION countloginfo, countlogerr, countlogwarn;

--dump result1;
--describe result1;


--STORE result1 INTO 'logsplit_output'; 



infodategroup = GROUP loginfo BY SUBSTRING(line, 0, 10);

infodatecount = FOREACH infodategroup GENERATE group, COUNT(loginfo);

--dump infodatecount;



errdategroup = GROUP logerr BY SUBSTRING(line, 0, 10);

errdatecount = FOREACH errdategroup GENERATE group, COUNT(logerr);



warndategroup = GROUP logwarn BY SUBSTRING(line, 0, 10);

warndatecount = FOREACH warndategroup GENERATE group, COUNT(logwarn);



result2 = UNION infodatecount, errdatecount, warndatecount;

--dump result2;


STORE result2 INTO 'count_by_date_output';
