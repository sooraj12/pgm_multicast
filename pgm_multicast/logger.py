from logging.config import dictConfig
import logging

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s -- %(module)s: %(message)s",
            }
        },
        "handlers": {
            "wsgi": {"class": "logging.StreamHandler", "formatter": "default"}
        },
        "root": {"level": "DEBUG", "handlers": ["wsgi"]},
    }
)
