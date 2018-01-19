--Query: You have to find the list of authors whose name starts with 'J' and the price of their books is greater than $200.



book_bag = LOAD 'book-data' USING PigStorage(',') AS (bookid:int, price:double, authorid:int);

--describe book_bag;
--dump book_bag;


author_bag = LOAD 'author-data' USING PigStorage(',') AS (authorid:int, author_name);

--dump author_bag;


aname_j = FILTER author_bag BY SUBSTRING(author_name, 0, 1) == 'J' OR SUBSTRING(author_name, 0, 1) == 'j';

--dump aname_j;


book200 = FILTER book_bag BY price > 200.0;

--dump book200;


bookjoin = JOIN book200 BY authorid, aname_j BY authorid;

--dump bookjoin;

STORE bookjoin INTO 'book_author_output';
