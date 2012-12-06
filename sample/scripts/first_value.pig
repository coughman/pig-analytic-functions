register ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load '../data/staff_data.csv' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);

--sg = group s all;

-- will use accumulator
--r2 = foreach sg generate group, com.cloudera.sa.pig.FirstValue(s.name) as name_first;
--dump r2;

sg = group s by salary partition by org.apache.pig.test.utils.SimpleCustomPartitioner;
r = foreach sg generate s.name, group, com.cloudera.sa.pig.FirstValue(s.salary);
dump r;
