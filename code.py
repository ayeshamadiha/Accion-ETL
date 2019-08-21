import pandas as pd
from sqlalchemy import create_engine 

loc = '/home/ayesha/Desktop/python/cost.csv'
lc=loc.split("/")[-1]
da = pd.read_csv(loc)
df= pd.DataFrame(da)
rc1=df['storeid'].count()
print('row count = ',rc1)
cc1=len(df.columns)
print('column count =',cc1)
df['time_stamp']=(pd.datetime.now())
df['sourcefile']=lc
print(df)
postgres_conn = create_engine('postgresql://postgres:Khushi@localhost:5432/usecase')
#postgres_conn.execute("truncate table accion_sales_stage")

df.to_sql('accion_sales_stage',postgres_conn,schema='sales',if_exists='replace',index=False,chunksize=10000)
df.to_sql('accion_sales_history',postgres_conn,schema='sales',if_exists='append',index=False,chunksize=10000)

#print('stage')
postgres_conn.execute("select * from sales.accion_sales_stage").fetchall()
#print('history')
postgres_conn.execute("select * from sales.accion_sales_history").fetchall()
postgres_conn.execute("select count(*) from sales.accion_sales_stage")
