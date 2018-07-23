var df = spark.sql("""select 
		column1,
		column2,
		column3
		from db.mytable
		where create_date = '2018-06-24'""")
		
df.coalesce(5).write.csv("/app/db/folder")		# will create only 5 files which may not be equal in size.

hadoop fs -ls /app/db/folder*

hadoop fs -copyToLocal /app/db/folder/part-00000-a00836ba-b258-47d5-94ae-cf1a53a2d109.csv ./part-00000.csv
hadoop fs -copyToLocal /app/db/folder/part-00001-a00836ba-b258-47d5-94ae-cf1a53a2d109.csv ./part-00001.csv
hadoop fs -copyToLocal /app/db/folder/part-00002-a00836ba-b258-47d5-94ae-cf1a53a2d109.csv ./part-00002.csv
hadoop fs -copyToLocal /app/db/folder/part-00003-a00836ba-b258-47d5-94ae-cf1a53a2d109.csv ./part-00003.csv
hadoop fs -copyToLocal /app/db/folder/part-00004-a00836ba-b258-47d5-94ae-cf1a53a2d109.csv ./part-00004.csv
