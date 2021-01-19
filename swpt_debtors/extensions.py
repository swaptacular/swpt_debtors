import os
import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from flask_melodramatiq import RabbitmqBroker
from dramatiq import Middleware
from flask_smorest import Api

MAIN_EXCHANGE_NAME = 'dramatiq'
APP_QUEUE_NAME = os.environ.get('APP_QUEUE_NAME', 'swpt_debtors')

warnings.filterwarnings(
    'ignore',
    r"this is a regular expression for the text of the warning",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


class EventSubscriptionMiddleware(Middleware):
    @property
    def actor_options(self):
        return {'event_subscription'}


db = CustomAlchemy()
db.signalbus.autoflush = False
migrate = Migrate()
protocol_broker = RabbitmqBroker(config_prefix='PROTOCOL_BROKER', confirm_delivery=True)
protocol_broker.add_middleware(EventSubscriptionMiddleware())
api = Api()
