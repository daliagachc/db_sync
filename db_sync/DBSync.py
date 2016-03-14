# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo


import time
from db_sync.Database import Database
import my_logger
import logging
logger = my_logger.get_logger(name=__name__, level='DEBUG')



class DBSync(object):
    def __init__(self, parameters):
        self.source = {}
        self.backup = {}
        self.interval_sync_seconds = 300 #default but can be overriden
        self.logging_level = "DEBUG"

        for key in parameters.keys():
            setattr(self, key, parameters[key])



        logging.disable(logging.getLevelName(self.logging_level)-1)


        while True:
            try:
                self.set_databases()
                try:
                    self.main_loop()
                except Exception as e:
                    logger.error('something happenden in the main loop %s', e)


            except Exception as e:

                logger.error('could not set databases. trying again in %s',
                            self.interval_sync_seconds
                            )
                logger.error('the error was: %s', e)


            logger.error('going to sleep for %s', self.interval_sync_seconds)
            time.sleep(self.interval_sync_seconds)



    def set_databases(self):
        logger.debug('setting database %s',)
        self.db_source = Database(self.source)

        self.db_backup = Database(
                self.backup,
                src_table_name=self.db_source.table_name,
                src_eng=self.db_source.eng
        )

    def loop_update(self):
        logger.debug('starting loop update %s',)
        try:
            while (self.db_source.last_datetime >
                       self.db_backup.last_datetime):

                self.db_backup.update_from_source(
                        self.db_source
                )

                logger.debug('last time source is  %s',
                             self.db_source.last_datetime)
                logger.debug('last time backup is %s',
                             self.db_backup.last_datetime)
        except Exception as e:
            logger.error('something happened while updating the values. The error is %s', e)


    def main_loop(self):
        logger.debug('starting main loop %s',)
        while True:
            self.db_backup.session.commit()
            self.db_source.session.commit()
            self.db_backup.set_last_datetime()
            self.db_source.set_last_datetime()
            logger.debug('last time source is  %s', self.db_source.last_datetime)
            logger.debug('last time backup is %s', self.db_backup.last_datetime)

            self.loop_update()

            logger.debug('sleeping for %s seconds',
                         self.interval_sync_seconds)
            time.sleep(self.interval_sync_seconds)