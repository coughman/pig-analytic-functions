register ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar;
s = load 'input' using PigStorage('\t') as (name:chararray, salary:int);
so = order s by name;
r = foreach so generate name, com.cloudera.sa.pig.RowNumber(name);
dump r;
