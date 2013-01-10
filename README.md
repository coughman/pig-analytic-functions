Pig Analytic Functions
======================

Analytic functions in Pig UDF

Illustrative examples on how to implement analytic functions from data warehouses (e.g. Netezza) in Pig.

Functions:
- First Value
- Last Value
- Row Number
- Rank
- Dense Rank

For doing these functions in Hive UDFs, take a look at https://github.com/paulmw/hive-udf.

In addition, Rank and Dense Rank are also implemented with Hadoop MapReduce API (see com.cloudera.sa.mr package).

Building
--------
This project uses Maven. To build the software, simply use "mvn package".

Examples
--------
Examples are provided for running in both Pig's local and mapreduce mode.  Go to the sample/scripts directory.  See the README file there for details.

Future Plans
------------
- add PigUnit unit tests?

