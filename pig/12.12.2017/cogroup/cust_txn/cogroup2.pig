--QUERY: Find the total count of transactions, the total value of these transactions and the first name of the customer.


customer = LOAD 'custs' USING PigStorage(',') AS (custid:chararray, firstname:chararray, lastname:chararray, age:int, profession:chararray);

--describe customer;
--dump customer;


txns = LOAD 'txns1.txt' USING PigStorage(',') AS (txnid:chararray, date, custid:chararray, amount:double, category, product, city, state, type);

--describe txns;
--dump txns;


customer = FOREACH customer GENERATE custid, firstname, lastname;

txns = FOREACH txns GENERATE custid, amount;

--dump customer;
--dump txns;


joined = COGROUP customer BY $0, txns BY $0;

--describe joined;
--dump joined;


queryresult = FOREACH joined GENERATE group, FLATTEN(customer.firstname), COUNT(txns) AS totalcount, ROUND_TO(SUM(txns.amount), 2) AS totalamt;

--dump queryresult;


STORE queryresult INTO 'cogroup2_output';
