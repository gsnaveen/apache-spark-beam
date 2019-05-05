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

DESCRIBE formatted db1.z_part1 PARTITION(viewdate='2018-08-01');
OK
# col_name              data_type               comment

cookie                  string
url                     string

# Partition Information
# col_name              data_type               comment

viewdate                string

# Detailed Partition Information
Partition Value:        [2018-08-01]
Database:               db1
Table:                  z_part1
CreateTime:             Thu Sep 13 07:08:52 PDT 2018
LastAccessTime:         UNKNOWN
Location:               maprfs:/app/user/warehouse/db1/z_part1/viewdate=2018-08-01
Partition Parameters:
        COLUMN_STATS_ACCURATE   {"BASIC_STATS":"true","COLUMN_STATS":{"cookie":"true","url":"true"}}
        numFiles                1
        numRows                 100
        rawDataSize             23700
        totalSize               1366
        transient_lastDdlTime   1557095539

# Storage Information
SerDe Library:          org.apache.hadoop.hive.ql.io.orc.OrcSerde
InputFormat:            org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat
Compressed:             No
Num Buckets:            -1
Bucket Columns:         []
Sort Columns:           []
Storage Desc Params:
        serialization.format    1
Time taken: 0.182 seconds, Fetched: 35 row(s)
                      
