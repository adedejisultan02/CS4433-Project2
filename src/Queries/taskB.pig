--read pages
access_logs = LOAD 'access_logs.csv' USING PigStorage(',') as (AccessID:chararray, ByWho:chararray, WhatPage:chararray, TypeOfAccess:chararray, AccessTime:chararray);

group_data = GROUP access_logs by WhatPage;

access_count = FOREACH group_data GENERATE group AS WhatPage, COUNT(access_logs) AS AccessCount;

-- Correcting the ORDER BY to use the alias of the aggregated count
order_data = ORDER access_count BY AccessCount DESC;

-- Limit to top 10 records
top_10_records = LIMIT order_data 10;

pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:chararray, Name:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

joined_data = JOIN top_10_records BY WhatPage, pages BY PersonID;

selected_data = FOREACH joined_data GENERATE PersonID, Name, Nationality;

--dump pages
dump selected_data;