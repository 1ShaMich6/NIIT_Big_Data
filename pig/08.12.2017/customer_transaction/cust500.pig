txn_bag = load 'txns1.txt' using PigStorage(',') as (txnid, date, custid, amount:double, category, product, city, state, type);

--describe txn_bag;

--dump txn_bag;


txnbycust = group txn_bag by custid;

--dump txnbycust;


spendbycust = foreach txnbycust generate group as custid, ROUND_TO(SUM(txn_bag.amount), 2) as totalsales;

--dump spendbycust;


cust500 = filter spendbycust by totalsales > 500;

--dump cust500;


cust_bag = load 'custs' using PigStorage(',') as (custid, firstname, lastname, age:long, profession);

--dump cust_bag;


join_bag = join cust500 by custid, cust_bag by custid;

--dump join_bag;


filter_join_bag = filter join_bag by age < 50;

--dump filter_join_bag;


store filter_join_bag into 'cust500_result';

