copyFromLocal ../data/staff_data.csv data/staff_data.csv;
copyFromLocal ../data/staff_data2.csv data/staff_data2.csv;

s1 = load 'data/staff_data.csv' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);
s2 = load 'data/staff_data2.csv' using PigStorage(',') as (id:int, name:chararray, salary:int, department:chararray);

s1_small = foreach s1 generate name, salary;
s2_small = foreach s2 generate name, salary;

fs -rm -r -f s1;
fs -rm -r -f s2;

store s1_small into 's1';
store s2_small into 's2';

fs -rm -r -f input;
fs -mkdir input;
fs -mv s1/part-m-00000 input/part0;
fs -mv s2/part-m-00000 input/part1;
fs -rm -r -f s1;
fs -rm -r -f s2;

fs -rm -r -f output;


