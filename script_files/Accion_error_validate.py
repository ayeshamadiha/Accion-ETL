import pandas as pd
from sqlalchemy import create_engine 

from Accion_Console_Log import get_logger

logger = get_logger("Accion_error_validate")

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

def get_data_table(table_name):
    logger.info("Getting the postgres db data")
    try:
        df = get_dataframe(file)
        lc = table_name.split(".")[-1]
        db_session = db_connect('localhost', 'postgres','Khushi')
        df.to_sql(lc,db_session,schema='sales',if_exists='replace',index=False,chunksize=10000)
        dt = db_session.execute(f"select * from {table_name}").fetchall()
        return dt
    except Exception as e:
        logger.error("Error while getting the postgres db data " + str(e))

def validate_error():
    logger.info("validating the error table")
    data = get_data_table('sales.accion_sales_error')
    #print(data)
    logger.info("error validation - success")

