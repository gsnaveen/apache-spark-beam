 #https://community.hortonworks.com/articles/72414/how-to-create-a-custom-udf-for-hive-using-python.html
 #https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Transform
  
  #!/users/userid/.anaconda2/bin/python
  import sys
  import base64
   for line in sys.stdin:
       line = line.strip()
       key,cookie  = line.split('\t')
       cookie64 = base64.b64encode(cookie)
       print(key+'\t'+cookie+'\t'+cookie64)

add file /hdfs/app/useridxx/workspace/users/userid/mytest3.py;

select TRANSFORM (ax.key,ax.cookie) USING 'python mytest3.py' AS key,cookie, cookie64 from (Select key,cookie from mydatax) ax;
