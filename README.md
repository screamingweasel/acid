# acid
Hive ACID examples

Compare/Contrast with HBase
Keep short on V1
Hive DWC


-- Get table location
SHOW CREATE TABLE default.people;

-- 1. Insert
INSERT OVERWRITE INTO people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 1 AND 10;

-- 2. Update
UPDATE people SET 
  last_name = CONCAT(last_name,'X')
  first_name = CONCAT(first_name,'X')
WHERE  id BETWEEN 1 AND 5;

-- hdfs dfs -ls -R <table location>

-- 3. New Inserts
INSERT INTO people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 11 AND 20;

-- 4. Merge
MERGE INTO people TGT
USING (
-- Inserts
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 21 AND 30
UNION ALL
-- Updates
SELECT id, CONCAT(first_name,'Y') AS first_name, CONCAT(last_name,'Y') AS last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 11 AND 20
) SRC
ON SRC.id = TGT.id
WHEN NOT MATCHED THEN UPDATE SET
  first_name = SRC.first_name,
  last_name = SRC.last_name
WHEN NOT MATCHED THEN INSERT VALUES 
 (SRC.id, SRC.first_name, SRC.last_name, SRC.email, SRC.gender, SRC.phone_nbr);
 
 

