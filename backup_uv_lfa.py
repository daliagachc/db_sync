# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
import db_sync
from db_sync.DBSync import DBSync
parameters = {
    'source': {
        'table_name': 'uv_lfa',
        'user': 'uv_lfa',
        'psw': 'uv_lfa',
        'ip': '10.8.3.1',
        'database_name': 'uv_lfa',
        'time_column_name': 'datetime',
        'time_int_or_datetime': 'datetime',
        'max_rows_to_update': 10000
    },
    'backup': {
        'table_name': 'uv_lfa',
        'user': 'root',
        'psw': '1045',
        'ip': 'localhost',
        'database_name': 'uv_lfa_amazon',
        'time_column_name': 'datetime',
        'time_int_or_datetime': 'datetime',
        'max_rows_to_update': 10000
    }
    ,
    'interval_sync_seconds': 5
    ,
    'logging_level': "ERROR"
}

DBSync(parameters)


