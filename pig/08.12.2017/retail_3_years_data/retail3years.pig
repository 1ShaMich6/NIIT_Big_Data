data00 = load '/home/hduser/2000.txt' using PigStorage(',') AS (category_id, category_name, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

data01 = load '/home/hduser/2001.txt' using PigStorage(',') AS (category_id, category_name, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

data02 = load '/home/hduser/2002.txt' using PigStorage(',') AS (category_id, category_name, jan:double, feb:double, mar:double, apr:double, may:double, jun:double, jul:double, aug:double, sep:double, oct:double, nov:double, dec:double);

--describe data00;
--dump data00;

--describe data01;
--dump data01;

--describe data02;
--dump data02;



sales00 = foreach data00 generate $0, $1, ($2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 + $10 + $11 + $12 + $13) as totalsales;

--dump sales00;

sales01 = foreach data01 generate $0, $1, ($2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 + $10 + $11 + $12 + $13) as totalsales; 

--dump sales01;

sales02 = foreach data02 generate $0, $1, ($2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 + $10 + $11 + $12 + $13) as totalsales;

--dump sales02;


joinallbag = join sales00 by $0, sales01 by $0, sales02 by $0;

--describe joinallbag;

--dump joinallbag;


noduplicates = foreach joinallbag generate $0, $1, $2, $5, $8;

--describe noduplicates;

--dump noduplicates;


growthpercent = foreach noduplicates generate $0, $1, $2, $3, $4, ROUND_TO((($3 - $2) * 100 / $2), 2) as growth_cycle_1, ROUND_TO((($4 - $3) * 100 / $3), 2) as growth_cycle_2;

--describe growthpercent;

--dump growthpercent;


avggrowth = foreach growthpercent generate $0, $1, $2, $3, $4, $5, $6, ROUND_TO((growth_cycle_1 + growth_cycle_2) / 2, 2) as avggrw;

--describe avggrowth;

--dump avggrowth;


query1 = filter avggrowth by avggrw > 10;

--dump query1;

--store query1 into 'query1';


query2 = filter avggrowth by avggrw < -5;

--dump query2;

--store query2 into 'query2';


overallsales = foreach noduplicates generate $0, $1, $2, $3, $4, ($2 + $3 + $4) as tsales;

--describe overallsales;

--dump overallsales;


orderall = order overallsales by tsales desc;

--dump orderall;


query3part1 = limit orderall 5;

--dump query3part1;

--store query3part1 into 'query3part1';


query3part2 = limit (order overallsales by tsales) 5;

--dump query3part2;

--store query3part2 into 'query3part2';
