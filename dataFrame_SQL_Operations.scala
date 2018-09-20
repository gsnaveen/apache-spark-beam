#Group by Multiple columns, Alias and creating multiple metrics

df.groupBy(col("viewdate").alias("dim1"),col("country_code")).agg(min("viewdate").alias("Min"),max("viewdate").alias("Max")).show()

#Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'leftanti', 'left_anti', 'cross'
val df3 = df1.join(df2, df1.col("viewdate") === df2.col("viewdate") && df1.col("ip") ===df2.col("ip"),"inner").select(df1.col("ip").alias("df1IP"),df2.col("ip").alias("df2IP"),df1.col("viewdate"),df2.col("viewdate"))
val df3 = df1.join(df2, df1.col("viewdate") === df2.col("viewdate") && df1.col("ip") ===df2.col("ip"),"left_outer").select(df1.col("ip").alias("df1IP"),df2.col("ip").alias("df2IP"),df1.col("viewdate"),df2.col("viewdate"))
