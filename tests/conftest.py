import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from swpt_debtors import create_app
from swpt_debtors.extensions import db

DB_SESSION = 'swpt_debtors.extensions.db.session'

server_name = 'example.com'
config_dict = {
    'TESTING': True,
    'SERVER_NAME': server_name,
    'MIN_DEBTOR_ID': 4294967296,
    'MAX_DEBTOR_ID': 8589934591,
    'APP_ENABLE_CORS': True,
    'APP_DEBTORS_PER_PAGE': 2,
    'APP_DEACTIVATED_DEBTOR_RETENTION_DAYS': 365.0,
    'APP_TRANSFERS_FINALIZATION_APPROX_SECONDS': 10.0,
    'APP_MAX_TRANSFERS_PER_MONTH': 10,
    'APP_DOCUMENT_MAX_CONTENT_LENGTH': 100,
    'APP_DOCUMENT_MAX_SAVES_PER_YEAR': 2,
    'APP_SUPERUSER_SUBJECT_REGEX': '^debtors-superuser$',
    'APP_SUPERVISOR_SUBJECT_REGEX': '^debtors-supervisor$',
    'APP_DEBTOR_SUBJECT_REGEX': '^debtors:([0-9]+)$',
}


@pytest.fixture(scope='module')
def app():
    """Get a Flask application object."""

    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope='function')
def db_session(app):
    """Get a Flask-SQLAlchmey session, with an automatic cleanup."""

    yield db.session

    # Cleanup:
    db.session.remove()
    for cmd in [
            'TRUNCATE TABLE debtor CASCADE',
            'TRUNCATE TABLE configure_account_signal',
            'TRUNCATE TABLE prepare_transfer_signal',
            'TRUNCATE TABLE finalize_transfer_signal',
    ]:
        db.session.execute(sqlalchemy.text(cmd))
    db.session.commit()


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)
