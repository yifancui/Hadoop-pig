myPage = LOAD '/user/hadoop/Proj1/data/MyPage.csv' using PigStorage(',') as (ID: int, Name:chararray, Nationality:chararray, CountryCode: int, Hobby: chararray);
friends = LOAD '/user/hadoop/Proj1/data/Friends.csv' using PigStorage(',') as (FriendRel: int, PersonID:int, MyFriend:int, DateofFriendship: int, Desc: chararray);

groupedFriends = GROUP friends by MyFriend;
countedFriends = FOREACH groupedFriends GENERATE group AS MyFriend, COUNT(friends) as count;
joinFL = JOIN myPage by ID LEFT OUTER, countedFriends by MyFriend;
selectedFL = FOREACH joinFL GENERATE ID, Name,((count IS NULL)? 0:count);
store selectedFL into 'pig_task4d.out';
