import logging.config
import logging
import pkg_resources
import db_sync.constants as cs
import yaml


def setup_logging():
    """Setup logging configuration

    """
    path = pkg_resources.resource_filename(__package__, 'logging.yaml')

    with open(path, 'rt') as f:
        config = yaml.load(f.read())



        if cs.logging_level:
            config['root']['level'] = cs.logging_level
    logging.config.dictConfig(config)
