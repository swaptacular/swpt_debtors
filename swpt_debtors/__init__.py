__version__ = '0.1.0'

import logging
import sys
import os
import os.path
from typing import List


def _excepthook(exc_type, exc_value, traceback):  # pragma: nocover
    logging.error("Uncaught exception occured", exc_info=(exc_type, exc_value, traceback))


def _remove_handlers(logger):
    for h in logger.handlers:
        logger.removeHandler(h)  # pragma: nocover


def _add_console_hander(logger, format: str):
    handler = logging.StreamHandler(sys.stdout)
    fmt = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

    if format == 'text':
        handler.setFormatter(logging.Formatter(fmt))
    elif format == 'json':  # pragma: nocover
        from pythonjsonlogger import jsonlogger
        handler.setFormatter(jsonlogger.JsonFormatter(fmt))
    else:  # pragma: nocover
        raise RuntimeError(f'invalid log format: {format}')

    logger.addHandler(handler)


def _configure_root_logger(format: str) -> logging.Logger:
    root_logger = logging.getLogger()
    _remove_handlers(root_logger)
    _add_console_hander(root_logger, format)

    return root_logger


def configure_logging(level: str, format: str, associated_loggers: List[str]) -> None:
    root_logger = _configure_root_logger(format)

    # Set the log level for this app's logger.
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level.upper())
    app_logger_level = app_logger.getEffectiveLevel()

    # Make sure that all loggers that are associated to this app have
    # their log levels set to the specified level as well.
    for qualname in associated_loggers:
        logging.getLogger(qualname).setLevel(app_logger_level)

    # Make sure that the root logger's log level (that is: the log
    # level for all third party libraires) is not lower than the
    # specified level.
    if app_logger_level > root_logger.getEffectiveLevel():
        root_logger.setLevel(app_logger_level)  # pragma: no cover

    # Delete all gunicorn's log handlers (they are not needed in a
    # docker container because everything goes to the stdout anyway),
    # and make sure that the gunicorn logger's log level is not lower
    # than the specified level.
    gunicorn_logger = logging.getLogger('gunicorn.error')
    gunicorn_logger.propagate = True
    _remove_handlers(gunicorn_logger)
    if app_logger_level > gunicorn_logger.getEffectiveLevel():
        gunicorn_logger.setLevel(app_logger_level)  # pragma: no cover


class MetaEnvReader(type):
    def __init__(cls, name, bases, dct):
        """MetaEnvReader class initializer.

        This function will get called when a new class which utilizes
        this metaclass is defined, as opposed to when an instance is
        initialized. This function overrides the default configuration
        from environment variables.

        """

        super().__init__(name, bases, dct)
        NoneType = type(None)
        annotations = dct.get('__annotations__', {})
        falsy_values = {'false', 'off', 'no', ''}
        for key, value in os.environ.items():
            if hasattr(cls, key):
                target_type = annotations.get(key) or type(getattr(cls, key))
                if target_type is NoneType:  # pragma: no cover
                    target_type = str

                if target_type is bool:
                    value = value.lower() not in falsy_values
                else:
                    value = target_type(value)

                setattr(cls, key, value)


class Configuration(metaclass=MetaEnvReader):
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE: int = None
    SQLALCHEMY_POOL_TIMEOUT: int = None
    SQLALCHEMY_POOL_RECYCLE: int = None
    SQLALCHEMY_MAX_OVERFLOW: int = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    PROTOCOL_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    API_TITLE = 'Debtors API'
    API_VERSION = 'v1'
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/debtors/.docs'
    OPENAPI_REDOC_PATH = ''
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    APP_AUTHORITY_URI = 'urn:example:authority'
    APP_TRANSFERS_FINALIZATION_AVG_SECONDS = 5.0
    APP_MAX_TRANSFERS_PER_MONTH = 300
    APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT = 10000
    APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT = 10000
    APP_DEBTORS_SCAN_DAYS = 7
    APP_DEBTORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_DEBTORS_SCAN_BEAT_MILLISECS = 25
    APP_DEACTIVATED_DEBTOR_RETENTION_DAYS = 7305.0
    APP_INACTIVE_DEBTOR_RETENTION_DAYS = 14.0
    APP_MAX_HEARTBEAT_DELAY_DAYS = 365
    APP_MAX_CONFIG_DELAY_HOURS = 24
    APP_DEBTORS_PER_PAGE = 2000
    APP_SUPERUSER_SUBJECT_REGEX = '^debtors-superuser$'
    APP_SUPERVISOR_SUBJECT_REGEX = '^debtors-supervisor$'
    APP_DEBTOR_SUBJECT_REGEX = '^debtors:([0-9]+)$'


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_lib.utils import Int64Converter
    from .extensions import db, migrate, protocol_broker, api
    from .routes import admin_api, debtors_api, transfers_api, specs
    from .cli import swpt_debtors
    from . import models  # noqa

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_port=1)
    app.url_map.converters['i64'] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config['API_SPEC_OPTIONS'] = specs.API_SPEC_OPTIONS
    db.init_app(app)
    migrate.init_app(app, db)
    protocol_broker.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    api.register_blueprint(debtors_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_debtors)
    return app


configure_logging(
    level=os.environ.get('APP_LOG_LEVEL', 'warning'),
    format=os.environ.get('APP_LOG_FORMAT', 'text'),
    associated_loggers=os.environ.get('APP_ASSOCIATED_LOGGERS', '').split(),
)
sys.excepthook = _excepthook
