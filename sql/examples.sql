------------------------------------------------------------------------------------------------
-- Hive ACID examples
------------------------------------------------------------------------------------------------
use <mydb>;

-- Get table location (to look at files later)
SHOW CREATE TABLE people;
hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people
!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people

------------------------------------------------------------------------------------------------
-- 1. Insert
------------------------------------------------------------------------------------------------
INSERT OVERWRITE TABLE people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM   default.people_raw
WHERE  id BETWEEN 1 AND 10;

!sh hdfs dfs -ls -R hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people
drwxrwx---+  - hive hadoop          0 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1716 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/bucket_00000

------------------------------------------------------------------------------------------------
-- 2. Update
------------------------------------------------------------------------------------------------
UPDATE people SET 
  last_name = CONCAT(last_name,'-X'),
  first_name = CONCAT(first_name,'-X')
WHERE  id BETWEEN 1 AND 5;

drwxrwx---+  - hive hadoop          0 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1716 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop        835 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop       1544 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/bucket_00000

------------------------------------------------------------------------------------------------
-- 3. New Inserts
------------------------------------------------------------------------------------------------
INSERT INTO people
SELECT id, first_name, last_name, email, gender, phone_nbr
FROM   default.people_raw
WHERE  id BETWEEN 11 AND 20;

drwxrwx---+  - hive hadoop          0 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1716 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop        835 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop       1544 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop       1737 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000/bucket_00000

------------------------------------------------------------------------------------------------
-- 4. Delete
------------------------------------------------------------------------------------------------
DELETE FROM people WHERE id = 1;

drwxrwx---+  - hive hadoop          0 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1716 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop        835 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000002_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:04 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000004_0000004_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:04 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000004_0000004_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop        830 2020-02-10 17:04 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000004_0000004_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop       1544 2020-02-10 17:02 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000002_0000/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000/_orc_acid_version
-rw-rw----+  3 hive hadoop       1737 2020-02-10 17:03 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000003_0000003_0000/bucket_00000

------------------------------------------------------------------------------------------------
-- 5. Minor Compaction
------------------------------------------------------------------------------------------------
ALTER TABLE people COMPACT 'minor';
SHOW COMPACTIONS;

drwxrwx---+  - hive hadoop          0 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1716 2020-02-10 17:00 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000001/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000004
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000004/_orc_acid_version
-rw-rw----+  3 hive hadoop        842 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000002_0000004/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000004
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000004/_orc_acid_version
-rw-rw----+  3 hive hadoop       1607 2020-02-10 17:05 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000002_0000004/bucket_00000

------------------------------------------------------------------------------------------------
-- 5. Major Compaction
------------------------------------------------------------------------------------------------
ALTER TABLE people COMPACT 'major';
SHOW COMPACTIONS;

drwxrwx---+  - hive hadoop          0 2020-02-10 17:10 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/.hive-staging_hive_2020-02-10_17-10-27_882_5596841675991100306-1
drwxrwx---+  - hive hadoop          0 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004
-rw-rw----+  3 hive hadoop         48 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/_metadata_acid
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/_orc_acid_version
-rw-rw----+  3 hive hadoop       2096 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/bucket_00000

------------------------------------------------------------------------------------------------
-- 4. Merge. Note: Union only needed to simulate an input table with mixed inserts and updates
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

drwxrwx---+  - hive hadoop          0 2020-02-10 17:10 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/.hive-staging_hive_2020-02-10_17-10-27_882_5596841675991100306-1
drwxrwx---+  - hive hadoop          0 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004
-rw-rw----+  3 hive hadoop         48 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/_metadata_acid
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/_orc_acid_version
-rw-rw----+  3 hive hadoop       2096 2020-02-10 17:07 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000004/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:21 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000007
-rw-rw----+  3 hive hadoop         48 2020-02-10 17:21 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000007/_metadata_acid
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:21 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000007/_orc_acid_version
-rw-rw----+  3 hive hadoop       2457 2020-02-10 17:21 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/base_0000007/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000007_0000007_0003
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000007_0000007_0003/_orc_acid_version
-rw-rw----+  3 hive hadoop        866 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delete_delta_0000007_0000007_0003/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0001
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0001/_orc_acid_version
-rw-rw----+  3 hive hadoop       1738 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0001/bucket_00000
drwxrwx---+  - hive hadoop          0 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0003
-rw-rw----+  3 hive hadoop          1 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0003/_orc_acid_version
-rw-rw----+  3 hive hadoop       1773 2020-02-10 17:20 hdfs://c316-node2.squadron.support.hortonworks.com:8020/warehouse/tablespace/managed/hive/jbarnett.db/people/delta_0000007_0000007_0003/bucket_00000

