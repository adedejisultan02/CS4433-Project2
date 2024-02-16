Friends = LOAD 'Friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriends: int, DateOfFriendship:int, Desc: chararray);
grouph = GROUP Friends BY PersonID;
counth = FOREACH grouph GENERATE group AS PersonID, COUNT(Friends) AS numFriends;
averageh = FOREACH (GROUP counth ALL) GENERATE AVG(counth.numFriends) AS avgNumFriends;
joinDatah = JOIN counth BY numFriends, averageh BY avgNumFriends;
aboveAverageh = FILTER joinDatah BY counth::numFriends > averageh::avgNumFriends;
resulth = FOREACH aboveAverageh GENERATE counth.PersonID;
STORE resulth INTO 'taskH.csv' USING PigStorage(',');
