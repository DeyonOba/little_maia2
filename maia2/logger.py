import logging.config
import logging

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False, # Keep existing loggers
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s' #
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler', #
            'formatter': 'standard',
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "app.log",
            "formatter": "standard",
            "maxBytes": 1024,
            "backupCount": 3
        }
    },
    'loggers': {
        'data': {
            'handlers': ['file'],
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
    }
}

def get_logger(name: str) -> logging.Logger:
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger(name)