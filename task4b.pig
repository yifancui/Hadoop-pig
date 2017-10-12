-- task2b

-- sample execution:
-- pig -f task2b.pig -param MyPage=/pigdata/sampleMyPage.csv -param output=sampleOutput


data = LOAD '$MyPage' using PigStorage(',') as (id: int, name:chararray, nationality:chararray, countrycode: int, hobby: chararray);

-- group data by each contry code
nat_group = GROUP data by countrycode;

-- generate report of citizens per country with a facebook page
nat_cit = FOREACH nat_group GENERATE group AS countrycode, COUNT(data) as count;

-- output processed data
STORE nat_cit INTO '$output';


