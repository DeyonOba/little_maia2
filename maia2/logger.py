import logging.config
import logging
import pathlib
from maia2.utils import setup_project_directories


log_path = setup_project_directories()["logs"]

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True, # Keep existing loggers
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s' 
        },
        "console": {
            "format": "$(message)s"
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler', #
            'formatter': 'console',
        },
        "data_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": log_path / "data.log",
            "formatter": "standard",
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 3
        },
        "file": { # default file handler
            "class": "logging.handlers.RotatingFileHandler",
            "filename": log_path / "app.log",
            "formatter": "standard",
            "maxBytes": 1 * 1024 * 1024,
            "backupCount": 1
        }
    },
    'loggers': {
        'data': {
            'handlers': ['data_file'],
            'level': 'DEBUG',
        },
        'processing': {
            'handlers': ['file'],
            'level': 'DEBUG',
        },
        "training": {
            "handlers": ["file", "console"],
            "level": "DEBUG"
        },
        "chess.pgn": {
            "handlers": ["data_file"],
            "level": "DEBUG"
        }
    }
}


def get_logger(name: str) -> logging.Logger:
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger(name)