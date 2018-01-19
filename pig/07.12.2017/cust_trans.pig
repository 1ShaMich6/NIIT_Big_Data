--CUSTOMER - TRANSACTION DATASET:
--===============================

cust = load '/home/hduser/custs' using PigStorage(',') as (custid, firstname, lastname, age:long, profession);

--describe cust;

--dump cust;


teacher_bag = filter cust by profession == 'Teacher';

--describe teacher_bag;

--dump teacher_bag;


limit_bag = LIMIT cust 10;

--describe limit_bag;

--dump limit_bag;


groupbyprofession = group cust by profession;

--describe groupbyprofession;

--dump groupbyprofession;


countbyprofession = foreach groupbyprofession generate group as profession, COUNT(cust) as headcount;

--Ensure that the function COUNT() must be in caps
--Else you will be given a ParseException

--describe countbyprofession;

--dump countbyprofession;


orderbyprofession = order countbyprofession by $0;

--dump orderbyprofession;

orderbycount = order countbyprofession by $1 desc;

--dump orderbycount;


txn_bag = load '/home/hduser/txns1.txt' using PigStorage(',') as (txnid, date, custid, amount:double, category, product, city, state, type);

--describe txn_bag;

--dump txn_bag;


txnbycust = group txn_bag by custid;

--dump txnbycust;


spendbycust = foreach txnbycust generate group as customer_id, ROUND_TO(SUM(txn_bag.amount), 2) as totalsales;

--dump spendbycust;


custorder = order spendbycust by totalsales desc;

--dump custorder;


top10cust = limit custorder 10;

--dump top10cust;


top10join = join top10cust by $0, cust by $0;

--describe top10join;

--dump top10join;


top10join_select = foreach top10join generate $0, $3, $4, $5, $6, $1;

--dump top10join_select;


top10order = order top10join_select by $5 desc;

describe top10order;

--dump top10order;


store top10order into 'cust_trans_pig_result';
