textbag = LOAD 'textfile2.txt' USING PigStorage(',') AS (id:int, name:chararray);

--dump textbag;


ranked_bag = RANK textbag;

--describe ranked_bag;

--dump ranked_bag;


skipheader = FILTER ranked_bag BY rank_textbag > 1;

--dump skipheader;


final = FOREACH skipheader GENERATE id, name;

--dump final;


STORE final INTO 'rankqueries_output';
