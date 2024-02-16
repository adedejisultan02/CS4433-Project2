-- Load and store distinct friendships as personID:myFriend
friendships = LOAD '/project2/friends.csv' USING PigStorage(',')
    AS (friendIndex:chararray, personID:chararray, myFriend:chararray);
friends = DISTINCT (FOREACH friendships GENERATE personID, myFriend);

-- Load and store distinct access records as ByWho:WhatPage
accessed = LOAD '/project2/access_logs.csv' USING PigStorage(',')
    AS (AccessID:chararray, ByWho:chararray, WhatPage:chararray);
accesses = DISTINCT (FOREACH accessed GENERATE ByWho, WhatPage);

-- Perform a cogroup between friends and accesses
cogroupedData = COGROUP friends BY (personID, myFriend), accesses BY (ByWho, WhatPage);

-- Filter out tuples where there are no access records for a friend
neverAccessed = FILTER cogroupedData BY IsEmpty(accesses);

-- Flatten the unfollowed relation to access fields
unfollowed_flattened = FOREACH neverAccessed GENERATE FLATTEN(friends.personID) AS personID;

-- Remove duplicate personIDs
unfollowed = DISTINCT unfollowed_flattened;

-- Load pages data
pages = LOAD '/project2/pages.csv' USING PigStorage(',')
    AS (personID:chararray, name:chararray);

-- Join unfollowed data with pages data
unfollowed_with_names = JOIN unfollowed BY personID LEFT OUTER, pages BY personID;

-- Generate the final output
final_output = FOREACH unfollowed_with_names GENERATE unfollowed::personID AS personID,
                                                  pages::name AS friend_name;

-- Print the final output
DUMP final_output;
