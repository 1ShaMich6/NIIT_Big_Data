--QUERY: To identify the users who use "Reliable" payment gateways per User



weblog_bag = LOAD 'weblog' AS (username, payment_gateway, access_time);

gateway_bag = LOAD 'gateway' AS (payment_gateway, success_rate:double);

--dump weblog_bag;
--dump gateway_bag;


joinbag = JOIN weblog_bag BY payment_gateway, gateway_bag BY payment_gateway;

--describe joinbag;
--dump joinbag;


joinbag = FOREACH joinbag GENERATE $0, $1, $4;

--dump joinbag;
--describe joinbag;


groupbyname = GROUP joinbag BY username;

--dump groupbyname;


avgsuccessrate = FOREACH groupbyname GENERATE group, AVG(joinbag.success_rate) AS average;

--dump avgsuccessrate;


reliablefilter = FILTER avgsuccessrate BY average > 90.0;

--dump reliablefilter;


STORE reliablefilter INTO 'reliable_gateway_output';
