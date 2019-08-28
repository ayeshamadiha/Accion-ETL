import pandas as pd
from sqlalchemy import create_engine 

from Accion_Console_Log import get_logger

logger = get_logger("Accion_error_load")

file = '/home/ayesha/Desktop/python/cost.csv'

def db_connect(host, user, password):
    try:
        logger.info("Establishing the connection to the postgres")
        return create_engine(f'postgresql://postgres:{password}@{host}:5432/{user}')
    except Exception as e:
        logger.error("Error while establishing the postgres connection " + str(e))
    
def get_dataframe(filepath):
    try:
        logger.info("Creating the dataframe with the given data")
        lc=filepath.split("/")[-1]
        da = pd.read_csv(filepath)
        df = pd.DataFrame(da)
        df['time_stamp'] = (pd.datetime.now())
        df['sourcefile'] = lc
        return df
    except Exception as e:
        logger.error("Error while creating the dataframe " + str(e))

def check_null(table_name):
    try:
        logger.info("checking for any null values in the postgres table")
        df = get_dataframe(file)
        lc = table_name.split(".")[-1]
        db_session = db_connect('localhost', 'postgres','Khushi')
        df.to_sql(lc,db_session,schema='sales',if_exists='replace',index=False,chunksize=10000)
        dt = db_session.execute(f"select * from {table_name} as t where (t.salesdate is null or t.productid is null or t.storeid is null or t.salestypeid is null or t.quantity is null or t.revenue is null)").fetchall()
        return dt
    except Exception as e:
        logger.error("Error while checking the postgers table " + str(e))

#def check_dup(table_name):
    #df = get_dataframe(file)
    #lc = table_name.split(".")[-1]
    #db_session = db_connect('localhost', 'postgres','Khushi')
   # dt = db_session.execute(f"select * from {table_name} ou where (select count(*) from {table_name} inr where inr.storeid = ou.storeid) > 1").fetchall()
    #dt.to_sql(lc,db_session,schema='sales',if_exists='replace',index=False,chunksize=10000)
   #return dt

def insert_into_error_null(val,table_name):
    logger.info("inserting data into the postgres table")
    try:
        df = pd.DataFrame.from_records(val,columns =['salesdate','productid','storeid','salestypeid','quantity','revenue','time_stamp','sourcefile'])  
        df['err'] = 'null_error'
        db_session = db_connect('localhost', 'postgres','Khushi')
        df.to_sql('accion_sales_error',db_session,schema='sales',if_exists='replace',index=False,chunksize=10000)
        dt = db_session.execute("select * from sales.accion_sales_error").fetchall()
        return dt
    except Exception as e:
        logger.error("Error while inserting the data into postgres table " + str(e))



def load_error():
    logger.info("Loading the data to the error table")
    nullval = check_null('sales.accion_sales_stage')
    dump = insert_into_error_null(nullval,'sales.accion_sales_error')
    

