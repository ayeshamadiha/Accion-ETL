import pandas as pd
from sqlalchemy import create_engine 

from Accion_Console_Log import get_logger

logger = get_logger("Accion_history_validate")

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
        lc = filepath.split("/")[-1]
        da = pd.read_csv(filepath)
        df = pd.DataFrame(da)
        df['time_stamp'] = (pd.datetime.now())
        df['sourcefile'] = lc
        return df
    except Exception as e:
        logger.error("Error while creating the dataframe " + str(e))

def get_dataframe_count():
    try:
        df=get_dataframe(file)
        rc1=len(df)
        return rc1
    except Exception as e:
        logger.error("Error while fetching the dataframe count " + str(e))

def get_row_count(table_name):
    try:
        df = get_dataframe(file)
        db_session = db_connect('localhost', 'postgres','Khushi')
        db_session = create_engine('postgresql://postgres:Khushi@localhost:5432/postgres')
        count = db_session.execute(f"select count(*) from {table_name}").fetchone()
        return count[0]
    except Exception as e:
        logger.error("Error while fetching the count from postgres table" + str(e))

def row_count_valid(df_rc,tb_rc):
    logger.info("validating the data")
    if(df_rc <= tb_rc):
        #print('data has been dumped correctly')
        logger.info('history validation - success')
    else:
        logger.error('history validation - failed')
        exit(0)

def validate_history():
    logger.info("validating the history table")
    rc = get_dataframe_count()
    count_sales = get_row_count('sales.accion_sales_history')
    row_count_valid(rc,count_sales)




