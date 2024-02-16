-- Load and process friends data
friendships = LOAD '/project2/friends.csv' USING PigStorage(',')
    AS (friendIndex:chararray, personID:chararray, myFriend:chararray);

friends = DISTINCT (FOREACH friendships GENERATE personID, myFriend);
friendGroup = GROUP friends BY myFriend;
friendCount = FOREACH friendGroup GENERATE group AS personID, COUNT(friends) AS friendCount;

-- Load pages data
pages = LOAD '/project2/pages.csv' USING PigStorage(',')
    AS (personID:chararray, name:chararray);

-- Left join friendCount with pages and replace null friendCount values with zero
connectedness_factor = JOIN pages BY personID LEFT OUTER, friendCount BY personID;
connectedness_factor_final = FOREACH connectedness_factor GENERATE pages::personID AS personID,
                                                              pages::name AS name,
                                                              (friendCount::friendCount is null ? 0 : friendCount::friendCount) AS friendCount;

-- Print the final output
DUMP connectedness_factor_final;
