-- id, first_name, last_name, email, gender, phone_nbr

CREATE EXTERNAL TABLE default.people_raw (
  id bigint,
  first_name string,
  last_name string,
  email string,
  gender string,
  phone_nbr string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION 's3n://screamingweasel/people.csv';