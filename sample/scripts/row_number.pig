register ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load '../data/staff_data.csv' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);
ss = order s by name;
r = foreach ss generate name, com.cloudera.sa.pig.RowNumber(name);
dump r;

