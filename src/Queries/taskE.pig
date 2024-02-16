-- Load and store distinct access records as ByWho:WhatPage
accessed = LOAD '/project2/access_logs.csv' USING PigStorage(',')
    AS (AccessID:chararray, ByWho:chararray, WhatPage:chararray);


pagesAccessed = Group accessed by ByWho;
accessCount = FOREACH pagesAccessed GENERATE group AS ByWho, COUNT(accessed) AS friendCount;

distinctPages = DISTINCT(FOREACH accessed GENERATE ByWho, WhatPage);
distinctAccessed = group distinctPages by ByWho;
distinctCount = FOREACH distinctAccessed GENERATE group AS ByWho, COUNT(distinctPages) AS friendCount;

DUMP distinctCount;
DUMP accessCount;