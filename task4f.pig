-- sample execution:
-- pig -f task2f.pig -param Friends=/pigdata/sampleFriends.csv -param AccessLog=/pigdata/sampleAccessLog.csv -param output=sampleOutput

friends = LOAD '$Friends' using PigStorage(',') as (FriendRel: int, PersonID:int, MyFriend:int, DateofFriendship: int, Desc: chararray);

accessLog = LOAD '$AccessLog' using PigStorage(',') as (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);


sFriends = FOREACH friends GENERATE PersonID, MyFriend;
sAccessLog = FOREACH accessLog GENERATE ByWho, WhatPage; 
distinctSLog = DISTINCT sAccessLog;
cogroup_data = COGROUP sFriends by PersonID, distinctSLog by ByWho;
sub_data = FOREACH cogroup_data GENERATE SUBTRACT(sFriends, distinctSLog);
store sub_data into '$output';

