-- task2c

-- sample execution:
-- pig -f task2c.pig -param AccessLog=/pigdata/sampleAccessLog.csv -param output=sampleOutput

-- load data
data = LOAD '$AccessLog' using PigStorage(',') as (id:int, bywho:int, whatpage:int, typeofaccess: chararray, accesstime:int);

-- group logs by whatpage ie the page id that people are accessing
grouped_log = GROUP data BY whatpage;

-- count the number of times that page id was accesesed
counted_log = FOREACH grouped_log GENERATE group AS whatpage, COUNT(data) as count;

-- order the page ids by decending order of the number of times it appeared
ordered_log = ORDER counted_log BY count DESC;

-- grab the top 10
top10_log = LIMIT ordered_log 10;

-- output processed data
STORE top10_log INTO '$output';


