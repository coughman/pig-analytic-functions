register ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load '../data/staff_data.csv' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);

sg = group s by salary partition by org.apache.pig.test.utils.SimpleCustomPartitioner;
r = foreach sg generate flatten(s.name), group, com.cloudera.sa.pig.FirstValue(s.salary);
dump r;
