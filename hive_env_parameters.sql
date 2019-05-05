#https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/DeprecatedProperties.html
#https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties
#http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
#https://stackoverflow.com/questions/42049455/compressed-file-vs-uncompressed-file-in-mapreduce-which-one-gives-better-perfor
#https://data-flair.training/blogs/hadoop-recordreader/
#https://stackoverflow.com/questions/17727468/hadoop-input-split-size-vs-block-size
#https://data-flair.training/forums/topic/what-are-input-format-input-split-record-reader-and-what-they-do/

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


snappy is not splitable
#https://www.tutorialscampus.com/tutorials/map-reduce/algorithm.htm
