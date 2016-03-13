# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo


import time
from db_sync.Database import Database
import my_logger

logger = my_logger.get_logger(name=__name__, level='DEBUG')

logger.debug('start dbsync', )


class DBSync(object):
    def __init__(self, parameters):
        self.source = {}
        self.backup = {}
        self.interval_sync_seconds = 300 #default but can be overriden
        for key in parameters.keys():
            setattr(self, key, parameters[key])

        db_source = Database(self.source)

        db_backup = Database(
                self.backup,
                src_table_name=db_source.table_name,
                src_eng=db_source.eng
        )

        while True:
            db_backup.session.commit()
            db_source.session.commit()
            db_backup.set_last_datetime()
            db_source.set_last_datetime()
            logger.debug('last time source is  %s', db_source.last_datetime)
            logger.debug('last time backup is %s', db_backup.last_datetime)
            while db_source.last_datetime > db_backup.last_datetime:
                db_backup.update_from_source(db_source)
                logger.debug('last time source is  %s', db_source.last_datetime)
                logger.debug('last time backup is %s', db_backup.last_datetime)
            logger.debug('sleeping for %s seconds', self.interval_sync_seconds)
            time.sleep(self.interval_sync_seconds)
