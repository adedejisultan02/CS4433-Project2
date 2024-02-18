--read pages
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:chararray, Name:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

group_data = GROUP pages BY Nationality;

country_count = FOREACH group_data GENERATE group AS Nationality, COUNT(pages.PersonID) AS CountryCount;

--store pages
dump country_count;