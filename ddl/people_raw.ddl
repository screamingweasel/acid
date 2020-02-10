------------------------------------------------------------------------------------------------
-- Exter
------------------------------------------------------------------------------------------------
-- wget https://screamingweasel.s3.us-west-2.amazonaws.com/people.csv
-- hadoop fs -mkdir -p /tmp/people
-- hadoop fs -put ./people.csv /tmp/people

DROP TABLE IF EXISTS default.people_raw;
CREATE EXTERNAL TABLE default.people_raw (
  id bigint,
  first_name string,
  last_name string,
  email string,
  gender string,
  phone_nbr string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/people/';

SELECT * FROM default.people_raw limit 10;