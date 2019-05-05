#https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/DeprecatedProperties.html
#https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties

SET mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts=-Xmx6144m;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.map.java.opts=-Xmx768m;
--SET mapreduce.map.memory.mb=8192;
--SET mapreduce.map.java.opts=-Xmx6144m;
SET hive.execution.engine=mr; --tez,spark
SET mapreduce.input.fileinputformat.split.minsize=512000000; --for creating less number of mappers
SET mapreduce.input.fileinputformat.split.maxsize=512000000; --for creating more mapper processes
set hive.optimize.ppd=true;
set hive.optimize.ppd.storage=true;
set hive.exec.compress.intermediate=true;
set hive.exec.parallel=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=512000000;
set hive.merge.smallfiles.avgsize=512000000;
--set hive.smalltable.filesize=512000000;
