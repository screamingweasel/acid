################################################################################################
# Load people_raw table
################################################################################################

wget https://s3.us-west-2.amazonaws.com/screamingweasel/people.csv -o /tmp/people.csv
hadoop fs -rm -r /tmp/people
hadoop fs -mkdir -p /tmp/people
hadoop fs -put /tmp/people.csv /tmp/people/

hive -f ddl/people_raw.ddl
