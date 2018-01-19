txn = load 'txns1.txt' using PigStorage(',') as (txnid, date, custid, amount:double, category, product, city, state, type);

txnbytype = group txn by type;

spendbytype = foreach txnbytype generate group as type, ROUND_TO(SUM(txn.amount), 2) as typesales;

--dump spendbytype;


groupall = group spendbytype all;

--dump groupall;


totalsales = foreach groupall generate ROUND_TO(SUM(spendbytype.typesales), 2) as totsales;

--dump totalsales;


final = foreach spendbytype generate type, typesales, ROUND_TO((typesales * 100 / totalsales.totsales), 2);

dump final;


store final into 'sales_percentage_payment_type';

