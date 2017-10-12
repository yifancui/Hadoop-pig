-- sample execution:
-- pig -f task2g.pig -param AccessLog=/pigdata/sampleAccessLog.csv -param time_unit=80 -param output=sampleOutput

/* Note: Since we dont know when a person created their account, this query really doesnt
show people that haven't accessed their account in awhile, but rather that haven't accessed
after a certain date. Ie, people may have made their account at time 3 and used their account
at time 4, but would be considered lost interest if cutoff was 5
*/

-- load data
access_log = LOAD '$AccessLog' using PigStorage(',') as (id:int, bywho:int, whatpage:int, typeofaccess:chararray, accesstime:int);

-- group bywho ids 
grouped_log = GROUP access_log BY bywho;

-- get last time each person accessed facebook
max_log = FOREACH grouped_log GENERATE group AS bywho, MAX(access_log.accesstime) as max_time;

-- only keep those that haven't accessed facebook after the defined time unit
filtered_log = FILTER max_log BY max_time < $time_unit;

-- output processed data
STORE filtered_log INTO '$output';


