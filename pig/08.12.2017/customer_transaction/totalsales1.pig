txn_bag = load 'txns1.txt' using PigStorage(',') as (txnid, date, custid, amount:double, category, product, city, state, type);


txngroup = group txn_bag by type;

describe txngroup;

--dump txngroup;


totalsales = foreach txn_bag generate SUM(txn_bag.amount);


result = foreach txngroup {
typesales = SUM(txn_bag.amount);

generate FLATTEN(group) as type, typesales as TYPE_SALES, totalsales as TOTAL_SALES;
}

dump result;



SAMPLE FORMAT:
==========================================================
inpt = load '....' as (col1 : chararray, col2 : chararray);
grp = group inpt by col1; -- creates bags for each value in col1
result = foreach grp {
    total = COUNT(inpt);
    t = filter inpt by col2 == 'T'; --create a bag which contains only T values
    generate flatten(group) as col1, total as  TOTAL_ROWS_IN_INPUT_TABLE, 100*(double)COUNT(t)/(double)total as PERCENTAGE_TRUE_IN_INPUT_TABLE;
};

dump result;

==========================================================
