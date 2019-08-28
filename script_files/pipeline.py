import Accion_stage_load
import Accion_stage_validate
import Accion_history_load
import Accion_history_validate
import Accion_error_load
import Accion_error_validate
import Accion_target_load
import Accion_target_validate
import pandas as pd
from sqlalchemy import create_engine 

from Accion_Console_Log import get_logger

logger = get_logger("pipeline")

def main():
    Accion_stage_load.load_stage()
    Accion_stage_validate.validate_stage()
    Accion_history_load.load_history()
    Accion_history_validate.validate_history()
    Accion_error_load.load_error()
    Accion_error_validate.validate_error()
    Accion_target_load.load_target()
    Accion_target_validate.validate_target()
    
main()

def db_connect(host, user, password):
    try:
        logger.info("establishing connection")
        return create_engine(f'postgresql://postgres:{password}@{host}:5432/{user}')
    except Exception as e:
        logger.error("Error while establishing the postgres connection " + str(e))

data = {'feed_name':['sales'],
        'start_time':['12:45:12:1244'],
        'end_time':[pd.datetime.now()],
        'num_of_records':['1'],
        'job_status':['success'],
        'no_of_steps':['9']}
df = pd.DataFrame(data)
db_session = db_connect('localhost','postgres','Khushi')
df.to_sql('accion_audit',db_session,schema='public',if_exists='replace',index=False,chunksize=10000)
dt = db_session.execute("select * from accion_audit").fetchall()
logger.info('new entry into audit table')

