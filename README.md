# apache-spark

### Spark Pivot (Row data) Python & Scala

```
>>> df.show()
+----------+-------+------+
|  viewdate| userid|visits|
+----------+-------+------+
|2018-01-23|userid1|    10|
|2018-01-09|userid2|    20|
|2018-01-17|userid3|    33|
+----------+-------+------+
```

#### Spark Pivoted data
```
>>> dfPivot.show()
+-------+------+------+------+
| userid|Week_0|Week_1|Week_2|
+-------+------+------+------+
|userid3|     0|    33|     0|
|userid1|     0|     0|    10|
|userid2|    20|     0|     0|
+-------+------+------+------+
```
