-- task2e

-- sample execution:
-- pig -f task2e.pig -param AccessLog=/pigdata/sampleAccessLog.csv -param output1=sampleOutput1 -param output2=sampleOutput2

access_log = LOAD '$AccessLog' using PigStorage(',') as (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime: int);

-- group bywho ids
grouped_log = GROUP access_log BY bywho;

-- count the number of accesses made for each bywho id
counted_log = FOREACH grouped_log GENERATE group AS bywho, COUNT(access_log) AS count;

-- find unique pages from visited pages and count the accesses
count_distinct_log = FOREACH grouped_log{uniqe_page = DISTINCT access_log.whatpage; GENERATE group AS bywho, COUNT(uniqe_page) AS d_count;};

-- output processed data
STORE counted_log INTO '$output1';
STORE count_distinct_log INTO '$output2';

