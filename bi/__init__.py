from __future__ import absolute_import
import logging
import os
import sys

import redis
from flask_mail import Mail
from flask_limiter import Limiter
from flask_limiter.util import get_ipaddr
from flask_migrate import Migrate
from statsd import StatsClient
from flask import Flask, safe_join
from werkzeug.routing import BaseConverter, ValidationError
from . import settings
from .app import create_app  # noqa
from .query_runner import import_query_runners
from .destinations import import_destinations

__version__ = "1.2.2"
__DeepBI_version__ = "1.2.2"


def setup_logging():
    handler = logging.StreamHandler(sys.stdout if settings.LOG_STDOUT else sys.stderr)
    formatter = logging.Formatter(settings.LOG_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(settings.LOG_LEVEL)

    # Make noisy libraries less noisy
    if settings.LOG_LEVEL != "DEBUG":
        for name in [
            "passlib",
            "requests.packages.urllib3",
            "snowflake.connector",
            "apiclient",
        ]:
            logging.getLogger(name).setLevel("ERROR")


setup_logging()

redis_connection = redis.from_url(settings.REDIS_URL)
rq_redis_connection = redis.from_url(settings.RQ_REDIS_URL)
mail = Mail()
migrate = Migrate(compare_type=True)
statsd_client = StatsClient(
    host=settings.STATSD_HOST, port=settings.STATSD_PORT, prefix=settings.STATSD_PREFIX
)
limiter = Limiter(key_func=get_ipaddr, storage_uri=settings.LIMITER_STORAGE)

import_query_runners(settings.QUERY_RUNNERS)
import_destinations(settings.DESTINATIONS)

class SlugConverter(BaseConverter):
    def to_python(self, value):
        # This is ay workaround for when we enable multi-org and some files are being called by the index rule:
        for path in settings.STATIC_ASSETS_PATH:
            full_path = safe_join(path, value)
            if os.path.isfile(full_path):
                raise ValidationError()

        return value

    def to_url(self, value):
        return value