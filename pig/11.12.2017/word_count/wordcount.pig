--QUERY: Count the frequency of each distinct word in a text file



textbag = LOAD 'textfile' USING TextLoader() as (word:chararray);

--describe textbag;
--dump textbag;


--tokenbag = FOREACH textbag GENERATE TOKENIZE(word);
tokenbag = FOREACH textbag GENERATE FLATTEN(TOKENIZE(LOWER(word)));
--describe tokenbag;
--dump tokenbag;


groupword = GROUP tokenbag BY token;
--describe groupword
--dump groupword;


wordcount = FOREACH groupword GENERATE group, COUNT(tokenbag) AS count;
describe wordcount;
--dump wordcount;


countorder = ORDER wordcount BY count DESC;

--dump countorder;

--STORE countorder INTO 'wordcount_output';
STORE countorder INTO 'wordcount_output_nocase';
