REGISTER /home/hduser/LowerToUpper.jar

DEFINE ToUpper LowerToUpper();

bag1 = Load 'lowertext.txt' USING PigStorage() AS (name:chararray);

bag2 = FOREACH bag1 GENERATE ToUpper(name);

--dump bag2; 


STORE bag2 INTO 'udf_output';
