book1 = LOAD 'book1.txt' USING TextLoader() AS (lines:chararray);

book2 = LOAD 'book2.txt' USING TextLoader() AS (lines:chararray);

--dump book1;
--dump book2;


bookcombined = 	UNION book1, book2;

--dump bookcombined;


SPLIT bookcombined INTO book3 IF SUBSTRING(lines, 5, 7) == 'is', book4 IF SUBSTRING(lines, 17, 22) == 'three', book5 IF SUBSTRING(lines, 0, 4) == 'this';

--dump book3;
dump book4;
--dump book5;
