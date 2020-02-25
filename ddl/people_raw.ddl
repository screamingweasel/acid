DROP TABLE IF EXISTS default.people_raw;
CREATE TABLE default.people_raw (
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

SELECT * FROM default.people_raw LIMIT 10;