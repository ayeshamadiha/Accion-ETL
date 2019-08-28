import pandas as pd
from sqlalchemy import create_engine 

from Accion_Console_Log import get_logger

logger = get_logger("Accion_target_load")

file = '/home/ayesha/Desktop/python/cost.csv'

def db_connect(host, user, password):
    try:
        logger.info("Establishing the connection to the postgres")
        return create_engine(f'postgresql://postgres:{password}@{host}:5432/{user}')
    except Exception as e:
        logger.error("Error while establishing the postgres connection " + str(e))

def insert_into_target(table_name):
    logger.info("inserting data into the postgres table")
    try:
        db_session = db_connect('localhost', 'postgres','Khushi')
        da = db_session.execute(f"SELECT * FROM sales.accion_sales_stage where storeid not in (select storeid from sales.accion_sales_error)").fetchall()
        df = pd.DataFrame(da)
        df['project_id'] = 'Accion_0927'
        df['ret_project_id'] = 'Accion_6710'
        df['upc'] = '  '
        df['margin'] = '  '
        df['end_date'] = '2019'
        df.to_sql('accion_sales_target',db_session,schema='sales',if_exists='replace',index=False,chunksize=10000)
        return df
    except Exception as e:
        logger.error("Error while inserting the data into postgres table " + str(e))
    
def load_target():
    logger.info("Loading the data to the target table")
    tar = insert_into_target('sales.accion_sales_stage')

