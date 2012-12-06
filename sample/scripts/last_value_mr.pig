register ../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load 'input' using PigStorage('\t') as (name:chararray, salary:int);
sg = group s all;

r = foreach sg generate flatten(s.name), com.cloudera.sa.pig.LastValue(s.name);
dump r;
