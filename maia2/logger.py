import logging.config
import logging
import pathlib

def setup_log_directory():
    log_dir_path = pathlib.Path(__file__).parent / "logs"
    print("Creating logs directory ...")
    log_dir_path.mkdir(parents=True, exist_ok=True)
    print(f"Created logs directory: {log_dir_path}")
    return log_dir_path

log_path = setup_log_directory()

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True, # Keep existing loggers
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s' 
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler', #
            'formatter': 'standard',
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