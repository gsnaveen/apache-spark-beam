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
--set hive.mapjoin.smalltable.filesize=512000000;
set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;
set hive.exec.temporary.table.storage=memory;
set hive.llap.execution.mode=all;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.aggr=true;
set hive.fetch.task.conversion=more;
Set hive.fetch.task.conversion.threshold=1073741824;

snappy is not splitable

#https://tez.apache.org/releases/0.9.2/tez-runtime-library-javadocs/configs/TezRuntimeConfiguration.html
set tez.grouping.max-size=67108864;
set tez.grouping.min-size=32000000;
set hive.tez.container.size=8192;

#https://hortonworks.com/blog/orcfile-in-hdp-2-better-compression-better-performance/
#https://community.hortonworks.com/articles/68631/optimizing-hive-queries-for-orc-formatted-tables.html
#https://www.cloudera.com/documentation/enterprise/5-3-x/topics/admin_data_compression_performance.html
ANALYZE TABLE page_views_orc COMPUTE STATISTICS FOR COLUMNS;
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.tez.max.partition.factor=20;
SET hive.exec.reducers.bytes.per.reducer=128000000;

--hive commands
show partitions tablesName

--Spark
spark.conf.set("spark.streaming.blockInterval","20")
spark.conf.set("hive.exec.dynamic.partition", "true") 
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

#https://www.tutorialscampus.com/tutorials/map-reduce/algorithm.htm
#http://comphadoop.weebly.com/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
