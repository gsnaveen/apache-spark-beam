https://cwiki.apache.org/confluence/display/Hive/HBaseIntegration#HBaseIntegration-Introduction

CREATE TABLE hbase_table_1(value map<string,int>, row_key int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:,:key"
);
INSERT OVERWRITE TABLE hbase_table_1 SELECT map(bar, foo), foo FROM pokes 
WHERE foo=98 OR foo=100;

CREATE TABLE hbase_table_1(value map<string,int>, row_key int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "cf:col_prefix.*,:key"
);

#https://cwiki.apache.org/confluence/display/Hive/HBaseBulkLoad

CREATE TABLE new_hbase_table(rowkey string, x int, y int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:x,cf:y");
 
SET hive.hbase.bulk=true;
 
INSERT OVERWRITE TABLE new_hbase_table
SELECT rowkey_expression, x, y FROM ...any_hive_query...;
