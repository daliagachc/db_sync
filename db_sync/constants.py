# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo

import sys
import imp



id_instrument = '01'
data_tb_name = 'uv_lfa'
data_db_name = 'uv_lfa'
server_tb_name = 'uv_lfa'
server_db_name = 'uv_lfa'
sync_db_name = 'sync'
sync_column_name = 'sync'
sync_tb_name = 'uv_lfa_sync'
conn_local = 'mysql://root:raspberry@localhost/'
# conn_local = 'mysql://root:raspberry@10.8.3.106/'
conn_server = 'mysql://uv_lfa:uv_lfa@10.8.3.1/uv_lfa'
interval = 60
time_column_name = 'datetime'


logging_level = 'DEBUG'

max_rows_per_update = 500

# print str(sys.argv)

# upload user constants if any. to pass them use -config
if '-config' in sys.argv:
    i = sys.argv.index('-config')
    path = sys.argv[i+1]
    # directory = os.path.dirname(path)
    # file = os.path.basename(path)
    # module = os.path.splitext(file)[0]
    imp.load_source('user_constants', path)
    from user_constants import *




