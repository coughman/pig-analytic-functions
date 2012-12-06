hadoop fs -rm -r -f output
hadoop jar ../../target/pig-analytic-functions-1.0-SNAPSHOT.jar com.cloudera.sa.mr.Rank input output
