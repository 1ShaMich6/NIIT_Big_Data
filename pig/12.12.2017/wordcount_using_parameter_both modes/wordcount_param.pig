--book = load '$inp' using PigStorage() as (lines:chararray);
book = LOAD '$hdfs_inp' USING TextLoader() AS (lines:chararray);

--dump book;


--On command prompt:
--pig -x local -p inp=textfile1.txt -f PigScriptWithParam.pig


book_tokens = FOREACH book GENERATE FLATTEN(TOKENIZE(LOWER(lines))) as tokens;

--dump book_tokens;


tokencount = FOREACH book_tokens GENERATE tokens, 1;

--dump tokencount;


grouptokens = GROUP tokencount BY tokens;

--dump grouptokens;


wordcount = FOREACH grouptokens GENERATE group, SUM(tokencount.$1) as count;

--dump wordcount;


--STORE wordcount INTO '$output';
STORE wordcount INTO '$hdfs_output';
