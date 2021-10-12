import pandas as pd

data = [
    [1,"after the dog",'2021-11-12','2021-09-04 00:01:21']
,[2,"after the \n dogs 2 rows",'2021-11-12','2021-09-04 20:01:21']
,[3,'after the \n dogs 2 rows and quote " help ','2021-11-12','2021-09-04 20:01:21']
,[4,'after the \n dogs 2 rows and quote `@ help ','2021-11-12','2021-09-04 20:01:21']
,[5,'after the \n dogs 2 rows and \t tab special char ','2021-11-12','2021-09-04 22:01:21']
,[6,'after the \n dogs 2 rows and   tab ','2021-11-12','2021-09-04 23:01:21']
]


df = pd.DataFrame.from_records(data,columns=['id','desc','dated','dtime'])

# df.to_csv('./data.tsv', sep='\t',index=False, header=True)

df.to_csv('./data.tsv', sep='\t',
                 index=False, header=False)