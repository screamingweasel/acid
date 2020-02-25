------------------------------------------------------------------------------------------------
-- Hive ACID Labs
------------------------------------------------------------------------------------------------
-- https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions
-- http://shzhangji.com/blog/2019/06/10/understanding-hive-acid-transactional-table/
-- https://github.com/cartershanklin/hive-scd-examples

------------------------------------------------------------------------------------------------
-- 0. Preparation
------------------------------------------------------------------------------------------------
-- Edit the script ddl/people.ddl to change database name to your own db (recommend userid)
-- Run ddl/people.ddl in Hive to create your own database and acid table.
-- Then switch to that database 
use <mydb>;

-- Get table location of acid table and examine initial files
SHOW CREATE TABLE people;
!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 1. Insert
------------------------------------------------------------------------------------------------
INSERT OVERWRITE TABLE people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM   default.people_raw
WHERE  id BETWEEN 1 AND 10;

!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 2. Update
------------------------------------------------------------------------------------------------
UPDATE people SET 
  last_name = CONCAT(last_name,'-X'),
  first_name = CONCAT(first_name,'-X')
WHERE  id BETWEEN 1 AND 5;

!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 3. New Inserts
------------------------------------------------------------------------------------------------
INSERT INTO people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM   default.people_raw
WHERE  id BETWEEN 11 AND 20;

INSERT INTO people VALUES (111, 'fname', 'lname', 'myemail111@cloudera.com', 'M', '111-222-4444');
INSERT INTO people VALUES (112, 'fname', 'lname', 'myemail112@cloudera.com', 'F', '112-222-4444');
INSERT INTO people VALUES (113, 'fname', 'lname', 'myemail113@cloudera.com', 'F', '113-222-4444');

!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 4. Delete
------------------------------------------------------------------------------------------------
DELETE FROM people WHERE id IN (1,3,5);

!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 5. Compaction
------------------------------------------------------------------------------------------------
ALTER TABLE people COMPACT 'minor';
SHOW COMPACTIONS;
!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

ALTER TABLE people COMPACT 'major';
SHOW COMPACTIONS;
!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 6. Merge. Note: Union only needed to simulate an input table with mixed inserts and updates
------------------------------------------------------------------------------------------------
MERGE INTO people AS T
USING (
-- Inserts
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 21 AND 30
UNION ALL
-- Updates
SELECT id, CONCAT(first_name,'-Y') AS first_name, CONCAT(last_name,'-Y') AS last_name, email, gender, phone_nbr
FROM default.people_raw
WHERE id BETWEEN 11 AND 20
) AS S
ON S.id = T.id
WHEN MATCHED THEN UPDATE SET
  first_name = S.first_name,
  last_name = S.last_name
WHEN NOT MATCHED THEN INSERT VALUES 
 (S.id, S.first_name, S.last_name, S.email, S.gender, S.phone_nbr);
 
SELECT * FROM people ORDER BY id;

------------------------------------------------------------------------------------------------
-- 6. Type 2 SCD Merge (optional)
--   Uses people_type2 table
--   Natural Key is id + begin_dt
--   end_dt = '9999-12-31' indicates the current record
--   If end_dt < '9999-12-31' then record is (logically) deleted
-- Ref: https://github.com/cartershanklin/hive-scd-examples
------------------------------------------------------------------------------------------------
-- Insert some initial data
INSERT OVERWRITE TABLE people_type2
SELECT id, '2020-01-01', '9999-12-31', first_name, last_name, email, gender, phone_nbr
FROM   default.people_raw
WHERE  id BETWEEN 1 AND 20;

-- Create an update table with updates, inserts, deletes
DROP TABLE IF EXISTS people_updates;
CREATE TABLE people_updates
SELECT id, CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))as date) as begin_dt, CAST('9999-12-31' AS date) as end_dt 
	CASE 
	  WHEN id BETWEEN 11 AND 20 THEN CONCAT(first_name,'-UPD')
	  ELSE CONCAT(first_name,'-INS')
	END AS first_name, 
	last_name, email, gender, phone_nbr,
	CASE WHEN id BETWEEN 12 AND 20 THEN 'Y' ELSE 'N' END AS del_flag
FROM default.people
WHERE ID BETWEEN 11 AND 30;

-- Tip, here is date -1 
CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()-86400))as date)

