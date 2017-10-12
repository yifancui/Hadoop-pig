-- sample execution:
-- pig -f task2h.pig -param Friends=/pigdata/sampleFriends.csv -param output=sampleOutput


-- load data
data = LOAD '$Friends' using PigStorage(',') as (friendrel:int, personid:int, myfriend:int, dateoffriendship:int, desc:chararray);

-- group data by person id
grouped_friends = GROUP data by personid;

-- get number of friends
counted_friends = FOREACH grouped_friends GENERATE group AS personid, COUNT(data) as count;

-- group all touples into one group
grouped_counted = GROUP counted_friends ALL;

-- get the average count of friends frorm the aggregated group all
grouped_avg = FOREACH grouped_counted GENERATE AVG(counted_friends.count) as avg;

-- keep only those with higher than average number of friends
filtered_friends = FILTER counted_friends BY count > grouped_avg.avg;

-- output processed data
STORE filtered_friends INTO '$output';


