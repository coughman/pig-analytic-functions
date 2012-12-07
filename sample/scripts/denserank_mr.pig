register ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load 'data' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);
ss = order s by salary desc;

sg = group ss all;
r = foreach sg generate ss.name, ss.salary, com.cloudera.sa.pig.DenseRank(ss.salary);
dump r;

