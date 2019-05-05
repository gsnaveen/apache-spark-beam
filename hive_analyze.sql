#https://cwiki.apache.org/confluence/display/Hive/StatsDev

ANALYZE TABLE db1.z_pdata1 COMPUTE STATISTICS; 

ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS;
ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr) COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS FOR COLUMNS;

hive> ANALYZE TABLE db1.z_part1  PARTITION(viewdate) COMPUTE STATISTICS;
Partition db1.z_part1{viewdate=2018-08-12} stats: [numFiles=1, numRows=10, totalSize=890, rawDataSize=2410]
Partition db1.z_part1{viewdate=2018-08-13} stats: [numFiles=1, numRows=10, totalSize=805, rawDataSize=2390]
Partition db1.z_part1{viewdate=2018-08-14} stats: [numFiles=1, numRows=10, totalSize=827, rawDataSize=2350]
Partition db1.z_part1{viewdate=2018-08-01} stats: [numFiles=1, numRows=100, totalSize=1366, rawDataSize=23700]
Partition db1.z_part1{viewdate=2018-08-11} stats: [numFiles=1, numRows=10, totalSize=820, rawDataSize=2580]
Partition db1.z_part1{viewdate=2018-08-10} stats: [numFiles=1, numRows=10, totalSize=722, rawDataSize=2410]
Partition db1.z_part1{viewdate=2018-08-02} stats: [numFiles=1, numRows=10, totalSize=818, rawDataSize=2360]

ANALYZE TABLE db1.z_part1 PARTITION(viewdate) COMPUTE STATISTICS FOR COLUMNS;

describe extended db1.z_pdata1;

DESCRIBE EXTENDED db1.z_part1 PARTITION(viewdate='2018-08-01');
Detailed Partition Information  Partition(values:[2018-08-01], dbName:db1, tableName:z_part1
, createTime:1536847732
, lastAccessTime:0
, sd:StorageDescriptor(cols:[FieldSchema(name:cookie, type:string, comment:null)
, FieldSchema(name:url, type:string, comment:null)
, FieldSchema(name:viewdate, type:string, comment:null)]
, location:maprfs:/app/user/warehouse/db1/z_part1/viewdate=2018-08-01
, inputFormat:org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
, outputFormat:org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat
, compressed:false, numBuckets:-1
, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.orc.OrcSerde, parameters:{serialization.format=1})
, bucketCols:[]
, sortCols:[]
, parameters:{}
, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{})
, storedAsSubDirectories:false)
, parameters:{totalSize=1366, numRows=100, rawDataSize=23700
              , COLUMN_STATS_ACCURATE={"BASIC_STATS":"true","COLUMN_STATS":{"cookie":"true","url":"true"}}
              , numFiles=1, transient_lastDdlTime=1557095539})
