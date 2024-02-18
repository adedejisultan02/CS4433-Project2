--read pages
pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

filter_data = FILTER pages BY Nationality == 'Spain';

selected_data = FOREACH filter_data GENERATE Name,Nationality,Hobby;

--dump pages
dump selected_data;