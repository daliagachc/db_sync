# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
import logging
from db_sync import DBSync
logger = logging.getLogger(__name__)

logger.debug('starting main %s','')


DBSync.start_retrieving_loop()