txn_bag = load 'txns1.txt' using PigStorage(',') as (txnid, date, custid, amount:double, category, product, city, state, type);

--dump txn_bag;


txngroup = group txn_bag all;

--dump txngroup;


totalsales = foreach txngroup generate ROUND_TO(SUM(txn_bag.amount), 2) as totsales;

dump totalsales;

--store totalsales into 'totalsales';


txn_cash = filter txn_bag by type == 'cash';

totalcashgroup = group txn_cash all;

--dump totalcashgroup;

--describe totalcashgroup;

final = foreach totalcashgroup generate ROUND_TO(SUM(txn_cash.amount) * 100 / totalsales.totsales, 2);

--dump final;


store final into 'totalsales';
