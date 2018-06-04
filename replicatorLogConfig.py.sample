# https://docs.python.org/3/library/logging.html
# https://docs.python.org/3/library/logging.config.html
# https://github.com/216software/clepy-logging
# https://docs.python.org/3/library/logging.handlers.html

LOGGING = {

    'version': 1,
    'disable_existing_loggers': True,
    'root': {
        'level': "ERROR",
        'handlers': ['console', "logfile"],
    },

    'formatters': {
        'basic': {
            'format': "%(asctime)-22s [%(process)d] %(name)-30s %(lineno)-5d %(levelname)-8s %(message)s",
        },
    },

    'handlers': {

        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
        },

        "logfile": {
            "class": "logging.FileHandler",
            "formatter": "basic",
            "level": "DEBUG",
            "filename": "replicator.log",
            "mode": "a",
        },

        # Come back here and add an smtp handler
    },

    'loggers': {
        "replicator": {
            "handlers": ["console", "logfile"],
            "level": "ERROR",
            "propagate": False
        },

    },

}
