set hivevar:db;
CREATE DATABASE IF NOT EXISTS ${db};
USE ${db}

-- Basic Hive ACID CRUD table
DROP TABLE IF EXISTS people;
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

-- For optional exercise
-- Hive ACID SCD Type 2 table (with history)
DROP TABLE IF EXISTS people_type2;
CREATE TABLE people_type2 (
  id bigint,
  start_dt date,
  end_dt date,
  first_name string,
  last_name string,
  email string,
  gender string,
  phone_nbr string)
STORED AS ORC
TBLPROPERTIES (
  'transactional'='true',
  'transactional_properties'='default');
