-- task2a

-- sample execution:
-- pig -f task2a.pig -param MyPage=/pigdata/sampleMyPage.csv -param nationality=fZkjr2t2JJ -param output=sampleOutput


-- load MyPage data
data = LOAD '$MyPage' using PigStorage(',') as (id: int, name:chararray, nationality:chararray, countrycode: int, hobby: chararray);

-- filter by specific country
nat_filt =  FILTER data BY nationality eq '$nationality';

-- grab only name and hobby
-- Note: add nationality of you want to double check the filter works correctly
name_hobby = FOREACH nat_filt GENERATE name, hobby;

-- output processed data
STORE name_hobby INTO '$output';




