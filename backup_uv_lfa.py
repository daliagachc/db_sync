# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
from db_sync.DBSync import DBSync
parameters = {
    'source': {
        'table_name': 'uv_lfa',
        'user': 'uv_lfa',
        'psw': 'uv_lfa',
        'ip': '10.8.3.1',
        'database_name': 'uv_lfa',
        'time_column_name': 'datetime'
    },
    'backup': {
        'table_name': 'uv_lfa',
        'user': 'root',
        'psw': '1045',
        'ip': 'localhost',
        'database_name': 'uv_lfa_amazon',
        'time_column_name': 'datetime',
        'max_rows_to_update': 10000
    }
    ,
    'interval_sync_seconds': 10
}

DBSync(parameters)


