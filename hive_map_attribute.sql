
create table db.my_testmap (
name      string ,   
ph        string ,   
category  map<string,string>
)

Select * from db.my_testmap


insert into db.my_testmap (name,ph,category) values('n1','p1',map("key1","value1","key2","value2"))

insert into db.my_testmap (name,ph,category) values('n2','p2',map("key1","value2","key2","value22"))

Select * from db.my_testmap

Select name,ph,category['key1'],category['key2'] from db.my_testmap

Presto:

Select name,ph,category['key1'],category['key2'] 
from hive_stg.db.my_testmap

-- https://stackoverflow.com/questions/42846229/select-all-columns-of-a-hive-struct
