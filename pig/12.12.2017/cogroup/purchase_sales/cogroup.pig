A = LOAD 'purchase.txt' USING PigStorage(',') AS (pid:int, purchaseqty:int);

--dump A;

B = LOAD 'sales.txt' USING PigStorage(',') AS (pid:int, salesqty:int);

--dump B;


C = COGROUP A BY $0, B BY $0; 

--describe C;
--dump C;


D = FOREACH C GENERATE group, SUM(A.purchaseqty), SUM(B.salesqty);

--dump D;


E = FOREACH C GENERATE group, SUM(A.purchaseqty), COUNT(A), SUM(B.salesqty), COUNT(B);

--dump E;


STORE E INTO 'cogroup_output';
