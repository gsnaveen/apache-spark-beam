#https://stackoverflow.com/questions/21477855/dynamic-partitioning-create-as-on-hive
#https://stackoverflow.com/questions/20756561/how-to-pick-up-all-data-into-hive-from-subdirectories
#https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
#https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions
#https://community.hortonworks.com/articles/49971/hive-streaming-compaction.html
#https://cwiki.apache.org/confluence/display/Hive/ListBucketing

CREATE TABLE page_view(viewTime INT, userid BIGINT,
     page_url STRING, referrer_url STRING,
     ip STRING COMMENT 'IP Address of the User')
 COMMENT 'This is the page view table'
 PARTITIONED BY(dt STRING, country STRING)
 CLUSTERED BY(userid) SORTED BY(viewTime) INTO 32 BUCKETS
 ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\001'
   COLLECTION ITEMS TERMINATED BY '\002'
   MAP KEYS TERMINATED BY '\003'
 STORED AS SEQUENCEFILE;
 
describe tablename;
describe extended tablename;

create table db1.z_part1_like like db1.z_part1; -- Will have the partitions columns as well.

show partitions db1.z_part1;
      viewdate=2018-08-01
      viewdate=2018-08-02


ALTER TABLE db1.z_part1 SET TBLPROPERTIES ("hive.input.dir.recursive" = "TRUE",
    "hive.mapred.supports.subdirectories" = "TRUE",
    "hive.supports.subdirectories" = "TRUE",
    "mapred.input.dir.recursive" = "TRUE");
    
ALTER TABLE db1.z_part1 SET TBLPROPERTIES ("comment" = "mycomment");

# Cluster and Partitioning
ALTER TABLE table_name CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name, ...)]
  INTO num_buckets BUCKETS;

ALTER TABLE mktg_bana.z_part1_like CLUSTERED BY (cookie) SORTED BY (cookie) into 1000 buckets;  
                                                                          
# SKEWING of DATA                                                                       
ALTER TABLE table_name NOT SKEWED;                                                                          

#Primary & foregin Keys
ALTER TABLE table_name ADD CONSTRAINT constraint_name PRIMARY KEY (column, ...) DISABLE NOVALIDATE;
ALTER TABLE table_name ADD CONSTRAINT constraint_name FOREIGN KEY (column, ...) REFERENCES table_name(column, ...) DISABLE NOVALIDATE RELY;
ALTER TABLE table_name DROP CONSTRAINT constraint_name; 
                                                                          
create table pk(id1 integer, id2 integer,
  primary key(id1, id2) disable novalidate);
 
create table fk(id1 integer, id2 integer,
  constraint c1 foreign key(id1, id2) references pk(id2, id1) disable novalidate);
                                                                          
#Add Partition                                                                          
 ALTER TABLE page_view ADD PARTITION (dt='2008-08-08', country='us') location '/path/to/us/part080808'
                          PARTITION (dt='2008-08-09', country='us') location '/path/to/us/part080809';                                                                         

#Temperory table                                                                          
CREATE TEMPORARY TABLE list_bucket_multiple (col1 STRING, col2 int, col3 STRING);
CREATE TEMPORARY TABLE db1.z_part1_temp as Select * from db1.z_part1;

CREATE TRANSACTIONAL TABLE transactional_table_test(key string, value string) PARTITIONED BY(ds string) STORED AS ORC;


spark.conf.set("hive.exec.dynamic.partition", "true") 
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

val dated= "2018-08-05"
var data1 = spark.sql("""Select cookie,url,viewdate
from db1.partners_web_data_uri_sessionized
where viewdate = '""" + dated + """' limit 10""")

data1.createOrReplaceTempView("data1")
spark.sql("""insert overwrite table  db1.z_part1 partition(viewdate) Select cookie,url,viewdate from data1""")

spark.sql("""insert overwrite table  db1.z_part1 partition(viewdate='""" + dated+ """') Select cookie,url from data1""")
