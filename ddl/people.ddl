CREATE DATABASE <mydb>;
USE <mydb>;

CREATE TABLE people (
  id bigint,
  first_name string,
  last_name string,
  email string,
  gender string,
  phone_nbr string)
STORED AS ORC
TBLPROPERTIES (
  'transactional'='true',
  'transactional_properties'='default');