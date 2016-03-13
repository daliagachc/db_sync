# project name: pyranometer
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
import datetime
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
import pandas as pd
import my_logger
import db_sync.functions as functions

logger = my_logger.get_logger(name=__name__, level='DEBUG')


class Database(object):
    """
    a class

    Attributes:
        dic: None
        table: None
        user: None
        psw: None
        ip: None
        database_name: None
        time_column_name: None
        url: None
        eng: None
        meta: None
        base: None
        time_column: None
        session: None

    """

    # class in charge to handle connection to a particular
    # table in a database
    def __init__(self, dic, src_table_name=None, src_eng=None):
        self.dic = dic
        self.table_name = None
        self.user = None
        self.psw = None
        self.ip = None
        self.database_name = None
        self.time_column_name = None
        self.max_rows_to_update = None

        for key in dic.keys():
            setattr(self, key, dic[key])

        self.url = 'mysql://{user}:{psw}@{ip}/{db}'
        self.url = self.url.format(
                user=self.user,
                psw=self.psw,
                ip=self.ip,
                db=self.database_name
        )

        self.eng = sa.create_engine(self.url)


        if src_table_name is not None:
            if not functions.check_table_exists(self.table_name, self.eng):
                functions.clone_table(src_eng, src_table_name, self.eng, self.table_name)

        self.meta = sa.MetaData(bind=self.eng)

        self.session = Session(self.eng)
        self.meta.reflect(bind=self.eng,
                          # schema=self.data_base_name,
                          only=[self.table_name])

        self.base = automap_base(metadata=self.meta)
        self.base.prepare()

        self.table = self.base.classes[self.table_name]
        self.time_column = getattr(self.table, self.time_column_name)

        self.last_datetime = None
        self.set_last_datetime()

    def set_last_datetime(self):
        query = self.session.query(self.time_column)
        query = query.order_by(self.time_column.desc())
        query = query.first()
        # if there is no result then last datetime is None
        if query is None:
            query = (datetime.datetime(1970, 1, 1, 0, 0, 0),)
        self.last_datetime = query[0]

    def update_from_source(self, source_db):
        self.set_last_datetime()
        source_db.set_last_datetime()

        if self.last_datetime < source_db.last_datetime:
            vals = source_db.get_values_greater_than(self.last_datetime)
            self.insert_values(vals)

        self.set_last_datetime()
        source_db.set_last_datetime()


    def get_values_greater_than(self, time):
        query = self.session.query(self.table)
        query = query.order_by(self.time_column.asc())
        query = query.filter(self.time_column > time)
        query = query.limit(self.max_rows_to_update)
        # res = query.all()
        res_dataframe = pd.read_sql(
                query.statement,
                self.session.bind
        )
        return res_dataframe

    def insert_values(self, dataframe):
        logger.debug('startin insert', )
        dataframe.to_sql(
            name=self.table_name,
            con=self.eng,
            if_exists='append',
            index=False
        )

