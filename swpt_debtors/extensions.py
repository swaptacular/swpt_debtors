import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from swpt_pythonlib.flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from swpt_pythonlib import rabbitmq
from flask_smorest import Api

TO_COORDINATORS_EXCHANGE = 'to_coordinators'
TO_DEBTORS_EXCHANGE = 'to_debtors'
TO_CREDITORS_EXCHANGE = 'to_creditors'
ACCOUNTS_IN_EXCHANGE = 'accounts_in'
CREDITORS_OUT_EXCHANGE = 'creditors_out'
CREDITORS_IN_EXCHANGE = 'creditors_in'
DEBTORS_OUT_EXCHANGE = 'debtors_out'
DEBTORS_IN_EXCHANGE = 'debtors_in'

warnings.filterwarnings(
    'ignore',
    r"this is a regular expression for the text of the warning",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


db = CustomAlchemy()
db.signalbus.autoflush = False
migrate = Migrate()
publisher = rabbitmq.Publisher(url_config_key='PROTOCOL_BROKER_URL')
api = Api()
