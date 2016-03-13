# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
import logging

import time

import datetime
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select
import db_sync.constants as cs
import sys

logger = logging.getLogger(__name__)
logger.debug('start dbsync %s', )


class DBSync(object):
    def __init__(self):
        self.eng_local = create_engine(cs.conn_local)
        self.meta_local = None
        self.local_data_table = None
        self.local_sync_table = None

        self.eng_server = create_engine(cs.conn_server)
        self.meta_server = None
        self.server_data_table = None
        self.create_sync_tb()
        self.create_data_tb_in_server()

    def create_sync_tb(self):
        # connect to local server
        conn = self.eng_local.connect()
        if not (cs.sync_db_name,) in self.eng_local.execute("SHOW DATABASES").fetchall():
            self.eng_local.execute("CREATE DATABASE IF NOT EXISTS " + cs.sync_db_name)

        self.meta_local = MetaData(bind=self.eng_local)

        self.meta_local.reflect(bind=self.eng_local,
                                schema=cs.data_db_name,
                                only=[cs.data_tb_name])

        self.local_data_table = self.meta_local.tables[cs.data_db_name + '.' + cs.data_tb_name]

        column_time = self.local_data_table.c[cs.time_column_name].copy()
        # column_sync = Column(cs.sync_column_name, Boolean, nullable=False)

        table = Table(cs.sync_tb_name,
                      self.meta_local,
                      column_time,
                      # column_sync,
                      schema=cs.sync_db_name
                      )

        self.local_sync_table = table

        if not (cs.sync_tb_name,) in self.eng_local.execute("SHOW TABLES IN " + cs.sync_db_name).fetchall():
            table.create()

        conn.close()

        pass

    def create_data_tb_in_server(self):
        # database should already be created in server and user must have permissions there
        conn_server = self.eng_server.connect()
        tables = self.eng_server.execute("SHOW TABLES IN " + cs.server_db_name).fetchall()
        logger.debug('tbls in server are %s', tables)

        table_local = self.local_data_table

        logger.debug('table is %s', table_local)
        meta_server = MetaData(bind=self.eng_server)
        table_server = Table(cs.server_tb_name,
                             meta_server
                             )

        for column in table_local.columns:
            table_server.append_column(column.copy())

        self.server_data_table = table_server

        if not (cs.server_tb_name,) in tables:
            table_server.create()

        conn_server.close()

        pass

    def retrieve_new_values_from_data_tb(self):

        conn_local = self.eng_local.connect()
        local_time_column = self.local_sync_table.c[cs.time_column_name]
        query = select([local_time_column])
        query = query.order_by(local_time_column.desc()).limit(1)

        logger.debug('query is %s', query)
        min_time = conn_local.execute(query).fetchall()
        logger.debug('mintime is  %s', min_time)
        if min_time == []:
            min_time = [(0,)]

        min_time = min_time[0][0]

        local_time_column = self.local_data_table.c[cs.time_column_name]

        response = dict()
        labels = ['values', 'times']
        sels = [self.local_data_table.select(), select([local_time_column])]

        for label, sel in zip(labels, sels):
            query = sel.order_by(local_time_column.asc())
            query = query.limit(cs.max_rows_per_update)
            query = query.where(local_time_column > min_time)
            vals = conn_local.execute(query).fetchall()
            response[label] = vals

        conn_local.close()
        return response

    def sync_values_to_server_data_tb(self, values, conn_server):

        # conn_server = self.eng_server.connect()
        ins = self.server_data_table.insert()
        conn_server.execute(ins, values)

        pass

    def update_sync_values_to_sync_tb(self, times, conn_local):

        # conn_local = self.eng_local.connect()
        ins = self.local_sync_table.insert()
        conn_local.execute(ins, times)

        pass

    def delete_old_values_from_data_tb(self):
        pass

    def get_local_last_time(self):
        conn_local = self.eng_local.connect()
        local_time_column = self.local_data_table.c[cs.time_column_name]
        query = select([local_time_column])
        query = query.order_by(local_time_column.desc()).limit(1)

        # logger.debug('query is %s',query)

        max_time = conn_local.execute(query).fetchall()
        logger.debug('lasttime local is  %s', max_time)
        if max_time == []:
            max_time = [(0,)]

        max_time = max_time[0][0]

        return max_time

    def get_server_last_time(self):
        conn_server = self.eng_server.connect()
        server_time_column = self.server_data_table.c[cs.time_column_name]
        server_time_column_type = server_time_column.type.python_type
        query = select([server_time_column])
        query = query.order_by(server_time_column.desc()).limit(1)

        # logger.debug('query is %s',query)

        max_time = conn_server.execute(query).fetchall()
        logger.debug('lasttime server is  %s', max_time)
        if max_time == []:
            if server_time_column_type is datetime.datetime:
                max_time = [(datetime.datetime(1970, 1, 1, 0, 0),)]
            else:
                max_time = [(0,)]

        max_time = max_time[0][0]

        return max_time

    def erase_new_local_values_in_server(self, times):
        flatten_times = [time[0] for time in times]
        conn_server = self.eng_server.connect()
        time_column = self.server_data_table.c[cs.time_column_name]
        delete = self.server_data_table.delete()
        delete = delete.where(time_column.in_(flatten_times))
        # logger.debug('delete is  %s',delete)
        rows_deleted = conn_server.execute(delete).rowcount
        conn_server.close()
        logger.error('rows deleted are %s. we are hoping for more than 0 to fix the issue', rows_deleted)


def start_retrieving_loop():
    while True:

        try:
            db = DBSync()
            while True:
                max_time_server = db.get_server_last_time()

                max_time_local = db.get_local_last_time()

                while max_time_local > max_time_server:
                    res = db.retrieve_new_values_from_data_tb()
                    try:
                        max_time_server = _update_local_server(db, max_time_server, res)
                    except Exception:
                        # logger.exception('there was an error updating. Lets try to erase some values from server data')
                        logger.error('there was an error updating. lets try deleting some rows')
                        db.erase_new_local_values_in_server(res['times'])
                        max_time_server = _update_local_server(db, max_time_server, res)

                logger.debug('going to sleep for %s seconds', cs.interval)
                time.sleep(cs.interval)

        except Exception:
            logger.exception('there was an error again. something is wrong')
            try:
                db.eng_server.dispose()
                db.eng_local.dispose()
            except:
                pass
            logger.error('lets sleep for %s seconds. hopefully the error will be gone', cs.interval)
            time.sleep(cs.interval)


def _update_local_server(db, max_time_server, res):
    conn_local = db.eng_local.connect()
    trans_local = conn_local.begin()
    conn_server = db.eng_server.connect()
    trans_server = conn_server.begin()
    try:
        db.sync_values_to_server_data_tb(res["values"], conn_server)
        db.update_sync_values_to_sync_tb(res["times"], conn_local)
        logger.debug('start commitin %s', '')
        trans_server.commit()
        logger.debug('server commited %s', '')
        trans_local.commit()
        logger.debug('end commiting %s', '')
        max_time_server = res['times'][-1][0]
        logger.debug('last time server is  %s', max_time_server)
    except:
        logger.error('Could not update values')
        raise
    finally:
        conn_server.close()
        conn_local.close()
        logger.debug('we are closing the connection. this is not necesarily bad')
    return max_time_server
