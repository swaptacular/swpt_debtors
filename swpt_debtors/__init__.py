__version__ = '0.1.0'

import os
import os.path
import logging
import logging.config

# Configure app logging. If the value of "$APP_LOGGING_CONFIG_FILE" is
# a relative path, the directory of this (__init__.py) file will be
# used as a current directory.
config_filename = os.environ.get('APP_LOGGING_CONFIG_FILE')
if config_filename:  # pragma: no cover
    if not os.path.isabs(config_filename):
        current_dir = os.path.dirname(__file__)
        config_filename = os.path.join(current_dir, config_filename)
    logging.config.fileConfig(config_filename, disable_existing_loggers=False)
else:
    logging.basicConfig(level=logging.WARNING)


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
    DRAMATIQ_BROKER_CLASS = 'RabbitmqBroker'
    DRAMATIQ_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    API_TITLE = 'Debtors API'
    API_VERSION = 'v1'
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/debtors/.docs'
    OPENAPI_REDOC_PATH = ''
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    APP_AUTHORITY_URI = 'urn:example:authority'
    APP_MAX_LIMITS_COUNT = 10
    APP_TRANSFERS_FINALIZATION_AVG_SECONDS = 5.0
    APP_MAX_TRANSFERS_PER_MONTH = 300
    APP_SIGNALBUS_MAX_DELAY_DAYS = 7
    APP_ACCOUNTS_SCAN_HOURS = 24
    APP_RUNNING_TRANSFERS_SCAN_DAYS = 7
    APP_INTEREST_RATE_CHANGE_MIN_DAYS = 8
    APP_DELETION_ATTEMPTS_MIN_DAYS = 14
    APP_MAX_INTEREST_TO_PRINCIPAL_RATIO = 0.01
    APP_RUNNING_TRANSFERS_ABANDON_DAYS = 365
    APP_DEAD_ACCOUNTS_ABANDON_DAYS = 365
    APP_MIN_INTEREST_CAPITALIZATION_DAYS = 14
    APP_DEBTORS_SCAN_DAYS = 7
    APP_DEBTORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_DEBTORS_SCAN_BEAT_MILLISECS = 25
    APP_INACTIVE_DEBTOR_RETENTION_DAYS = 14
    APP_DEBTORS_PER_PAGE = 2000
    APP_SUPERUSER_SUBJECT_REGEX = '^debtors-superuser$'
    APP_SUPERVISOR_SUBJECT_REGEX = '^debtors-supervisor$'
    APP_DEBTOR_SUBJECT_REGEX = '^debtors:([0-9]+)$'


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_lib.utils import Int64Converter
    from .extensions import db, migrate, broker, api
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
    broker.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    api.register_blueprint(debtors_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_debtors)
    return app
