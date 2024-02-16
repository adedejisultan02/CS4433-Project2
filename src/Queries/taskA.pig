--read pages
pages = LOAD 'hdfs://localhost:9000/user/cs4433/project2/pages.csv' USING PigStorage(',') as (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:chararray, Hobby:chararray);

filter_data = FILTER pages BY Nationality == 'Spain';

filter1_data = FOREACH filter_data GENERATE Name,Nationality,Hobby;

--dump pages
dump filter1_data;