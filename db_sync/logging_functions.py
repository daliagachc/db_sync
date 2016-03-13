# project name: ceil_bokeh
# created by diego aliaga daliaga_at_chacaltaya.edu.bo
import logging

logger = logging.getLogger(__name__)

from functools import wraps

def log_start_end(f):
	@wraps(f)
	def log_fun(*args, **kwargs):
		logger = logging.getLogger(f.__module__)
		logger.debug('starting function %s',f.__name__)
		res =  f(*args, **kwargs)
		logger.debug('finishing function %s',f.__name__)
		return  res

	return log_fun